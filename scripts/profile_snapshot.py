#!/usr/bin/env python
"""Benchmark depth snapshot retrieval under different cache TTL and HTTP limits."""
import argparse
import asyncio
import importlib
import json
import os
import time
from pathlib import Path
import logging

try:  # pragma: no cover - optional dependency
    import psutil
except Exception:  # pragma: no cover - psutil optional
    psutil = None  # type: ignore
    logging.getLogger(__name__).warning(
        "psutil not installed; CPU profiling disabled"
    )
from aiohttp import web

from solhunter_zero import http, depth_client


def _prepare_mmap(path: Path) -> None:
    data = {"tok": {"tx_rate": 1.0, "dex": {"dex1": {"bids": 10, "asks": 5}}}}
    path.write_text(json.dumps(data))


async def _start_server(payload: dict, host: str, port: int) -> web.AppRunner:
    async def handler(_request: web.Request) -> web.Response:
        return web.json_response(payload)

    app = web.Application()
    app.router.add_get("/snapshot", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    return runner


async def _stop_server(runner: web.AppRunner) -> None:
    await runner.cleanup()


async def _fetch(url: str, iterations: int) -> None:
    session = await http.get_session()
    for _ in range(iterations):
        async with session.get(url) as resp:
            await resp.text()
        depth_client.snapshot("tok")


async def _run_case(ttl: float, limit: int, per_host: int, iterations: int, url: str) -> tuple[float, float]:
    os.environ["DEPTH_CACHE_TTL"] = str(ttl)
    os.environ["HTTP_CONNECTOR_LIMIT"] = str(limit)
    os.environ["HTTP_CONNECTOR_LIMIT_PER_HOST"] = str(per_host)
    importlib.reload(http)
    importlib.reload(depth_client)

    proc = psutil.Process(os.getpid()) if psutil is not None else None
    if proc is not None:
        proc.cpu_percent(interval=None)
    start = time.perf_counter()
    await _fetch(url, iterations)
    elapsed = time.perf_counter() - start
    cpu = proc.cpu_percent(interval=None) if proc is not None else 0.0
    return elapsed / iterations, cpu


async def main() -> None:
    parser = argparse.ArgumentParser(description="Profile snapshot retrieval")
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--ttl", type=float, nargs="+", default=[0.0, 0.5, 1.0])
    parser.add_argument("--limit", type=int, nargs="+", default=[10, 20])
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    mmap_path = Path("sample_depth.mmap")
    _prepare_mmap(mmap_path)
    os.environ["DEPTH_MMAP_PATH"] = str(mmap_path)

    runner = await _start_server({"ok": True}, args.host, args.port)
    url = f"http://{args.host}:{args.port}/snapshot"
    try:
        for ttl in args.ttl:
            for lim in args.limit:
                latency, cpu = await _run_case(ttl, lim, lim, args.iterations, url)
                print(
                    f"TTL {ttl}s limit {lim} -> latency {latency*1000:.2f}ms CPU {cpu:.1f}%"
                )
    finally:
        await _stop_server(runner)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
