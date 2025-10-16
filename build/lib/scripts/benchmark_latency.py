import argparse
import asyncio
import base64
import time

from solhunter_zero.depth_client import stream_depth, submit_signed_tx
from solhunter_zero.dex_scanner import scan_new_pools_sync, DEX_PROGRAM_ID
from solhunter_zero.scanner_common import token_matches
from solana.rpc.api import Client


async def _run(token: str, tx_b64: str, updates: int) -> None:
    count = 0
    async for _ in stream_depth(token, max_updates=updates):
        start = time.perf_counter()
        await submit_signed_tx(tx_b64)
        end = time.perf_counter()
        print(f"latency: {end - start:.3f}s")
        count += 1
        if count >= updates:
            break


def _scan_new_pools_old(rpc_url: str) -> None:
    """Old synchronous pool scan used for comparison."""
    client = Client(rpc_url)
    resp = client.get_program_accounts(DEX_PROGRAM_ID, encoding="jsonParsed")
    for acc in resp.get("result", []):
        info = (
            acc.get("account", {})
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
        )
        for key in ("tokenA", "tokenB"):
            mint = info.get(key, {}).get("mint")
            name = info.get(key, {}).get("name")
            token_matches(mint or "", name)


def benchmark_pools(rpc_url: str, runs: int) -> None:
    """Benchmark pool scanning before and after async change."""
    start = time.perf_counter()
    for _ in range(runs):
        _scan_new_pools_old(rpc_url)
    old = (time.perf_counter() - start) / runs

    start = time.perf_counter()
    for _ in range(runs):
        scan_new_pools_sync(rpc_url)
    new = (time.perf_counter() - start) / runs

    print(f"old: {old:.3f}s new: {new:.3f}s per run")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark latency")
    sub = parser.add_subparsers(dest="cmd", required=True)

    tx_p = sub.add_parser("tx", help="Benchmark transaction latency")
    tx_p.add_argument("token", help="Token symbol")
    tx_p.add_argument("tx", help="Base64 transaction")
    tx_p.add_argument("--updates", type=int, default=1)

    pool_p = sub.add_parser("pools", help="Benchmark pool scanning")
    pool_p.add_argument("rpc", help="RPC URL")
    pool_p.add_argument("--runs", type=int, default=1)

    args = parser.parse_args()

    if args.cmd == "tx":
        asyncio.run(_run(args.token, args.tx, args.updates))
    else:
        benchmark_pools(args.rpc, args.runs)


if __name__ == "__main__":  # pragma: no cover
    main()
