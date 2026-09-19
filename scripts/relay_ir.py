"""
Fetch public IR-tagged egress endpoints, probe them, and keep a live pool.

A relay is only kept if:
  - protocol is socks5 or http(s) (socks4 is dropped)
  - generate_204 through it returns HTTP 204
  - cloudflare cdn-cgi/trace reports loc=IR
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from dataclasses import dataclass

import requests

USER_AGENT = "Mozilla/5.0 (compatible; PersianRayTester/1.0)"
FETCH_TIMEOUT = 15
PROBE_TIMEOUT = 8.0
TRACE_URL = "https://www.cloudflare.com/cdn-cgi/trace"
PROBE_URL = "https://www.gstatic.com/generate_204"
PROBE_CONCURRENCY = 20
POOL_WANT = 5
POOL_MIN = 2

_HOSTPORT_RE = re.compile(
    r"^(?:(?P<scheme>socks5|socks4|https?|socks)://)?"
    r"(?:(?P<user>[^:@/\s]+):(?P<password>[^@/\s]*)@)?"
    r"(?P<host>\[[^\]]+\]|[^:/\s]+):(?P<port>\d{2,5})\s*$",
    re.IGNORECASE,
)


@dataclass
class IrRelay:
    protocol: str  # socks5 | http
    address: str
    port: int
    username: str = ""
    password: str = ""

    @property
    def url(self) -> str:
        auth = ""
        if self.username:
            auth = f"{self.username}:{self.password}@"
        return f"{self.protocol}://{auth}{self.address}:{self.port}"

    @property
    def identity(self) -> tuple:
        return (self.protocol, self.address, self.port)

    def as_dialer(self) -> dict:
        return {
            "protocol": "socks" if self.protocol == "socks5" else "http",
            "address": self.address,
            "port": self.port,
            "username": self.username,
            "password": self.password,
        }


def read_source_urls(path: str) -> list[str]:
    if not os.path.exists(path):
        return []
    urls = []
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    return urls


def parse_relay_line(line: str) -> IrRelay | None:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    line = line.split()[0].rstrip(",;")
    match = _HOSTPORT_RE.match(line)
    if not match:
        return None
    scheme = (match.group("scheme") or "http").lower()
    if scheme == "socks":
        scheme = "socks5"
    if scheme == "socks4":
        return None
    if scheme == "https":
        scheme = "http"
    if scheme not in ("socks5", "http"):
        return None
    host = match.group("host")
    if host.startswith("[") and host.endswith("]"):
        host = host[1:-1]
    return IrRelay(
        protocol=scheme,
        address=host,
        port=int(match.group("port")),
        username=match.group("user") or "",
        password=match.group("password") or "",
    )


def fetch_list(url: str) -> list[IrRelay]:
    try:
        resp = requests.get(
            url,
            timeout=FETCH_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"  [!] IR relay source {url}: {exc}")
        return []
    found: list[IrRelay] = []
    seen: set[tuple] = set()
    for raw in resp.text.splitlines():
        relay = parse_relay_line(raw)
        if relay is None or relay.identity in seen:
            continue
        seen.add(relay.identity)
        found.append(relay)
    print(f"  IR source {url} -> {len(found)} candidates")
    return found


def collect_candidates(source_urls: list[str]) -> list[IrRelay]:
    all_relays: list[IrRelay] = []
    seen: set[tuple] = set()
    for url in source_urls:
        for relay in fetch_list(url):
            if relay.identity in seen:
                continue
            seen.add(relay.identity)
            all_relays.append(relay)
    return all_relays


def _curl_relay_args(relay: IrRelay) -> list[str]:
    if relay.protocol == "socks5":
        return ["--socks5-hostname", f"{relay.address}:{relay.port}"]
    return ["--proxy", f"http://{relay.address}:{relay.port}"]


async def _curl_capture(relay: IrRelay, url: str, write_code: bool) -> tuple[int, str]:
    args = [
        "curl", "-s", "--max-time", str(int(PROBE_TIMEOUT)),
        *_curl_relay_args(relay),
    ]
    if write_code:
        args += ["-o", "/dev/null", "-w", "%{http_code}"]
    args.append(url)
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=PROBE_TIMEOUT + 2)
        return proc.returncode or 0, stdout.decode("utf-8", errors="replace")
    except (asyncio.TimeoutError, OSError):
        return 1, ""


async def probe_relay(relay: IrRelay, require_ir: bool = True) -> bool:
    code_rc, code_out = await _curl_capture(relay, PROBE_URL, write_code=True)
    if code_rc != 0 or code_out.strip() != "204":
        return False
    if not require_ir:
        return True
    trace_rc, trace_out = await _curl_capture(relay, TRACE_URL, write_code=False)
    if trace_rc != 0:
        return False
    for line in trace_out.splitlines():
        if line.startswith("loc=") and line.split("=", 1)[1].strip().upper() == "IR":
            return True
    return False


async def build_pool(
    candidates: list[IrRelay],
    want: int = POOL_WANT,
    minimum: int = POOL_MIN,
    require_ir: bool = True,
) -> list[IrRelay]:
    if not candidates:
        return []
    print(f"Probing {len(candidates)} IR relay candidates (want {want}, min {minimum})")
    sem = asyncio.Semaphore(PROBE_CONCURRENCY)
    alive: list[IrRelay] = []
    lock = asyncio.Lock()

    async def worker(relay: IrRelay) -> None:
        async with sem:
            if len(alive) >= want:
                return
            ok = await probe_relay(relay, require_ir=require_ir)
            if not ok:
                return
            async with lock:
                if len(alive) < want:
                    alive.append(relay)
                    print(f"  [ok] {relay.url}")

    await asyncio.gather(*(worker(r) for r in candidates))
    if len(alive) < minimum:
        print(f"Only {len(alive)} IR relays passed; need {minimum}.")
        return []
    print(f"IR relay pool: {len(alive)}")
    return alive


class LivePool:
    """Current relay + standbys. mark_dead() rotates; restock() re-probes leftovers."""

    def __init__(self, relays: list[IrRelay], leftovers: list[IrRelay]):
        self._lock = asyncio.Lock()
        self.alive = list(relays)
        self.leftovers = list(leftovers)
        self.failovers = 0
        self.used: list[str] = [r.url for r in relays]

    def current(self) -> IrRelay | None:
        return self.alive[0] if self.alive else None

    def current_dialer(self) -> dict | None:
        relay = self.current()
        return relay.as_dialer() if relay else None

    async def heartbeat(self) -> bool:
        relay = self.current()
        if relay is None:
            return False
        return await probe_relay(relay, require_ir=False)

    async def mark_dead(self, relay: IrRelay | None = None) -> IrRelay | None:
        async with self._lock:
            target = relay or self.current()
            if target is not None:
                self.alive = [r for r in self.alive if r.identity != target.identity]
                print(f"  [!] IR relay lost: {target.url}")
        while True:
            async with self._lock:
                if not self.alive:
                    return None
                nxt = self.alive[0]
            ok = await probe_relay(nxt, require_ir=False)
            if ok:
                async with self._lock:
                    self.failovers += 1
                    if nxt.url not in self.used:
                        self.used.append(nxt.url)
                print(f"  [->] switched to {nxt.url}")
                return nxt
            print(f"  [!] standby also lost: {nxt.url}")
            async with self._lock:
                self.alive = [r for r in self.alive if r.identity != nxt.identity]

    async def restock(self, want: int = POOL_WANT, minimum: int = 1) -> bool:
        if self.alive:
            return True
        if not self.leftovers:
            return False
        print(f"Restocking IR pool from {len(self.leftovers)} leftovers...")
        fresh = await build_pool(self.leftovers, want=want, minimum=minimum, require_ir=True)
        used_ids = {r.identity for r in fresh}
        self.leftovers = [r for r in self.leftovers if r.identity not in used_ids]
        if not fresh:
            return False
        self.alive = fresh
        for relay in fresh:
            if relay.url not in self.used:
                self.used.append(relay.url)
        self.failovers += 1
        print(f"  [->] restocked, now {self.alive[0].url}")
        return True


async def make_pool(source_file: str) -> LivePool | None:
    urls = read_source_urls(source_file)
    if not urls:
        print(f"No IR relay sources in {source_file}")
        return None
    candidates = collect_candidates(urls)
    print(f"{len(candidates)} unique IR relay candidates")
    if not candidates:
        return None
    alive = await build_pool(candidates)
    if not alive:
        return None
    alive_ids = {r.identity for r in alive}
    leftovers = [r for r in candidates if r.identity not in alive_ids]
    return LivePool(alive, leftovers)


if __name__ == "__main__":
    # Manual pool check: python relay_ir.py ../../relay_sources_ir.txt
    src = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "relay_sources_ir.txt",
    )

    async def _main() -> int:
        pool = await make_pool(src)
        if pool is None or pool.current() is None:
            print("No healthy IR relays.")
            return 1
        print("Pool ready:")
        for relay in pool.alive:
            print(f"  {relay.url}")
        return 0

    raise SystemExit(asyncio.run(_main()))
