"""

IR pipeline (runs after the global job in each 5-hour cycle):



 1. Read working.txt from the global track

 2. Fetch + probe public IR egress relays (204 + loc=IR)

 3. Re-test those configs through the live IR relay pool

 4. Fail over if a relay drops mid-run

 5. Write working_ir.txt / working_ir_base64.txt only on a complete run

"""



import asyncio

import base64

import os

import sys

from datetime import datetime, timezone



sys.path.insert(0, os.path.dirname(__file__))

from relay_ir import make_pool  # noqa: E402

from parser import dedupe_nodes, extract_uris, parse_node  # noqa: E402

from tester_ir import run_ir_tests  # noqa: E402



ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

WORKING_FILE = os.path.join(ROOT, "working.txt")

SOURCES_IR = os.path.join(ROOT, "relay_sources_ir.txt")

OUT_PLAIN = os.path.join(ROOT, "working_ir.txt")

OUT_B64 = os.path.join(ROOT, "working_ir_base64.txt")





def atomic_write(path: str, data: str) -> None:

    import tempfile



    directory = os.path.dirname(path) or "."

    fd, tmp_path = tempfile.mkstemp(prefix=".tmp_out_", dir=directory)

    try:

        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:

            handle.write(data)

        os.replace(tmp_path, path)

    except Exception:

        try:

            os.remove(tmp_path)

        except OSError:

            pass

        raise





def load_global_working() -> list[dict]:

    if not os.path.exists(WORKING_FILE):

        print(f"No {WORKING_FILE}; run the global track first.")

        return []

    with open(WORKING_FILE, encoding="utf-8") as handle:

        text = handle.read()

    uris = extract_uris(text)

    nodes = []

    for uri in uris:

        node = parse_node(uri)

        if node is not None:

            nodes.append(node)

    return dedupe_nodes(nodes)





def write_outputs(working: list[dict], pool_used: list[str], failovers: int) -> None:

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    lines = [

        f"# Updated: {timestamp}\n",

        f"# track: ir\n",

        f"# {len(working)} working nodes (IR egress tested, sorted fastest first)\n",

        f"# relays_used: {len(pool_used)}\n",

        f"# ir_failovers: {failovers}\n\n",

    ]

    for row in working:

        latency_ms = int((row["latency"] or 0) * 1000)

        lines.append(f"{row['raw']} # {latency_ms}ms\n")

    atomic_write(OUT_PLAIN, "".join(lines))

    joined = "\n".join(row["raw"] for row in working)

    blob = base64.b64encode(joined.encode("utf-8")).decode("ascii")

    atomic_write(OUT_B64, blob)

    print(f"Wrote {OUT_PLAIN} and {OUT_B64}")





def write_step_summary(working: int, tested: int, leftover: int, failovers: int, elapsed: float) -> None:

    path = os.environ.get("GITHUB_STEP_SUMMARY")

    if not path:

        return

    with open(path, "a", encoding="utf-8") as handle:

        handle.write("## IR config check\n\n")

        handle.write(f"- global working.txt nodes: {tested}\n")

        handle.write(f"- working via IR egress: {working}\n")

        handle.write(f"- leftover untested: {leftover}\n")

        handle.write(f"- failovers: {failovers}\n")

        handle.write(f"- elapsed: {elapsed:.1f}s\n")





async def main() -> int:

    print("fetch-configs IR tester starting", flush=True)

    nodes = load_global_working()

    if not nodes:

        print("No global working configs; refusing to overwrite working_ir.txt.")

        return 1

    print(f"Loaded {len(nodes)} nodes from working.txt")



    pool = await make_pool(SOURCES_IR)

    if pool is None or pool.current() is None:

        print("No healthy IR relays; keeping previous working_ir.txt.")

        return 1



    session, elapsed, leftover = await run_ir_tests(nodes, pool)

    print(f"IR run finished in {elapsed:.1f}s")

    print(f"  working via IR: {len(session.working)}")

    print(f"  real misses: {session.real_fail}")

    print(f"  leftover untested: {leftover}")

    print(f"  failovers: {pool.failovers}")



    write_step_summary(len(session.working), len(nodes), leftover, pool.failovers, elapsed)



    if leftover > 0 or session.abort:

        print("Incomplete IR run (relay pool lost); keeping previous working_ir.txt.")

        return 1



    write_outputs(session.working, pool.used, pool.failovers)

    return 0





if __name__ == "__main__":

    try:

        raise SystemExit(asyncio.run(main()))

    except SystemExit:

        raise

    except Exception as exc:

        print(f"IR pipeline failed: {exc}", file=sys.stderr)

        raise SystemExit(1)

