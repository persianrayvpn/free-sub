"""
IR track: test global working.txt nodes through a live IR egress relay pool.

TCP CONNECT and the Xray handshake both egress via the current relay.
A heartbeat 204 on the relay itself runs beside the workers. If the
relay drops, recent failures are re-queued and the pool failovers.
Results collected while the relay was failing are not trusted as "dead".
"""

from __future__ import annotations

import asyncio
import time
from collections import deque

from relay_ir import IrRelay, LivePool, probe_relay
from tester import full_xray_check

TCP_TIMEOUT = 4.0
XRAY_CONCURRENCY = 2
SUSPECT_WINDOW = 15
HEARTBEAT_SECS = 30
HEARTBEAT_STRIKES = 2


async def tcp_via_relay(relay: IrRelay, host: str, port: int, timeout: float = TCP_TIMEOUT) -> bool:
    try:
        from python_socks.async_.asyncio import Proxy as SocksClient
    except ImportError:
        print("python-socks is required for the IR track (pip install -r requirements.txt)")
        return False
    try:
        client = SocksClient.from_url(relay.url)
        sock = await asyncio.wait_for(
            client.connect(dest_host=host, dest_port=port),
            timeout=timeout,
        )
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
        return True
    except Exception:
        return False


async def test_node_via_relay(node: dict, local_port: int, relay: IrRelay) -> dict:
    result = {**node, "ok": False, "latency": None}
    try:
        if not await tcp_via_relay(relay, node["address"], node["port"]):
            result["fail_stage"] = "tcp"
            return result
        ok, latency = await full_xray_check(
            node, local_port, dialer=relay.as_dialer()
        )
        result["ok"] = ok
        result["latency"] = latency
        if not ok:
            result["fail_stage"] = "xray"
        return result
    except Exception as exc:
        result["fail_stage"] = "error"
        result["error"] = str(exc)
        return result


class IrSession:
    def __init__(self, pool: LivePool, nodes: list[dict]):
        self.pool = pool
        self.pending: asyncio.Queue[tuple[dict, int]] = asyncio.Queue()
        for node in nodes:
            self.pending.put_nowait((node, 0))
        self.total = len(nodes)
        self.working: list[dict] = []
        self.seen_ok: set[str] = set()
        self.real_fail = 0
        self.suspect: deque[tuple[dict, int]] = deque(maxlen=SUSPECT_WINDOW)
        self.lock = asyncio.Lock()
        self.abort = False
        self.since_hb = 0
        self.finished = 0
        self.inflight = 0
        self._failover_lock = asyncio.Lock()
        self._hb_strikes = 0

    def remaining(self) -> int:
        return self.pending.qsize()

    async def failover(self, reason: str) -> bool:
        async with self._failover_lock:
            if self.abort:
                return False
            current = self.pool.current()
            print(f"IR relay heartbeat failed ({reason})")
            async with self.lock:
                retry = list(self.suspect)
                self.suspect.clear()
            for node, attempts in retry:
                raw = node.get("raw", "")
                if raw in self.seen_ok:
                    continue
                await self.pending.put((node, attempts))
            nxt = await self.pool.mark_dead(current)
            if nxt is None:
                if not await self.pool.restock():
                    print("IR relay pool empty; aborting remaining tests.")
                    self.abort = True
                    return False
            self._hb_strikes = 0
            return True

    async def heartbeat_if_needed(self, force: bool = False) -> bool:
        if self.abort:
            return False
        if not force:
            return True
        if self._failover_lock.locked():
            return True
        relay = self.pool.current()
        ok = await probe_relay(relay, require_ir=False) if relay else False
        if ok:
            self._hb_strikes = 0
            return True
        self._hb_strikes += 1
        print(
            f"IR relay heartbeat miss {self._hb_strikes}/{HEARTBEAT_STRIKES} "
            f"({relay.url if relay else 'none'})",
            flush=True,
        )
        if self._hb_strikes < HEARTBEAT_STRIKES:
            return True
        return await self.failover("scheduled heartbeat")


async def _heartbeat_loop(session: IrSession, stop: asyncio.Event) -> None:
    while not stop.is_set() and not session.abort:
        try:
            await asyncio.wait_for(stop.wait(), timeout=HEARTBEAT_SECS)
            return
        except asyncio.TimeoutError:
            pass
        if session.abort or stop.is_set():
            return
        await session.heartbeat_if_needed(force=True)


async def run_ir_tests(nodes: list[dict], pool: LivePool, concurrency: int = XRAY_CONCURRENCY):
    concurrency = max(1, min(int(concurrency), XRAY_CONCURRENCY))
    session = IrSession(pool, nodes)
    ports = asyncio.Queue()
    for offset in range(concurrency):
        ports.put_nowait(21000 + offset)

    stop = asyncio.Event()
    hb_task = asyncio.create_task(_heartbeat_loop(session, stop))
    started = time.time()

    async def worker() -> None:
        while not session.abort:
            async with session.lock:
                try:
                    node, attempts = session.pending.get_nowait()
                    session.inflight += 1
                except asyncio.QueueEmpty:
                    node = None
                    attempts = 0
                    idle = session.inflight == 0
            if node is None:
                if idle:
                    await asyncio.sleep(0.2)
                    async with session.lock:
                        if session.inflight == 0 and session.pending.empty():
                            return
                    continue
                await asyncio.sleep(0.05)
                continue

            relay = session.pool.current()
            if relay is None:
                waited = 0.0
                while session.pool.current() is None and not session.abort and waited < 20:
                    await asyncio.sleep(0.5)
                    waited += 0.5
                relay = session.pool.current()
            if relay is None:
                async with session.lock:
                    session.inflight -= 1
                await session.pending.put((node, attempts))
                if not session.abort:
                    session.abort = True
                return

            port = await ports.get()
            try:
                result = await test_node_via_relay(node, port, relay)
            finally:
                ports.put_nowait(port)
                async with session.lock:
                    session.inflight -= 1

            if session.abort:
                if not result.get("ok"):
                    await session.pending.put((node, attempts))
                continue

            raw = node.get("raw", "")
            relay_now = session.pool.current()
            switched = relay_now is None or relay_now.identity != relay.identity

            if result.get("ok"):
                async with session.lock:
                    if raw not in session.seen_ok:
                        session.seen_ok.add(raw)
                        session.working.append(result)
                    session.finished += 1
                    session.since_hb += 1
                continue

            if switched:
                await session.pending.put((node, attempts))
                continue

            async with session.lock:
                session.real_fail += 1
                session.finished += 1
                session.since_hb += 1
                session.suspect.append((node, attempts))
            # Heartbeat is the dedicated loop only. Workers must not
            # all failover in parallel through one flaky public relay.

    while not session.abort:
        await asyncio.gather(*(worker() for _ in range(concurrency)))
        if session.pending.empty():
            break
    stop.set()
    hb_task.cancel()
    try:
        await hb_task
    except asyncio.CancelledError:
        pass

    elapsed = time.time() - started
    session.working.sort(key=lambda row: row["latency"] or 0)
    leftover = session.remaining()
    return session, elapsed, leftover
