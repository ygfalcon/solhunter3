import argparse
import asyncio
import importlib
import os
import time
import psutil
import websockets


async def _run(batch_ms: int, n: int, host: str, port: int) -> tuple[float, float]:
    os.environ["EVENT_BATCH_MS"] = str(batch_ms)
    import solhunter_zero.event_bus as ev

    ev = importlib.reload(ev)
    await ev.start_ws_server(host, port)
    async with websockets.connect(
        f"ws://{host}:{port}",
        ping_interval=float(os.getenv("WS_PING_INTERVAL", "20") or 20),
        ping_timeout=float(os.getenv("WS_PING_TIMEOUT", "20") or 20),
    ) as ws:
        proc = psutil.Process(os.getpid())
        proc.cpu_percent(interval=None)
        start = time.perf_counter()
        for _ in range(n):
            ev.publish("heartbeat", {"service": "bench"})
        recv = 0
        while recv < n:
            raw = await ws.recv()
            msgs = ev._unpack_batch(raw) or [raw]
            recv += len(msgs)
        duration = time.perf_counter() - start
        cpu = proc.cpu_percent(interval=None)
    await ev.stop_ws_server()
    return n / duration, cpu


async def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark event bus batching")
    parser.add_argument("--messages", type=int, default=1000)
    parser.add_argument("--batch-ms", type=int, default=10)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8779)
    args = parser.parse_args()

    rate1, cpu1 = await _run(0, args.messages, args.host, args.port)
    rate2, cpu2 = await _run(args.batch_ms, args.messages, args.host, args.port)

    print(f"no batching: {rate1:.0f} msg/s CPU {cpu1:.1f}%")
    print(f"batching ({args.batch_ms}ms): {rate2:.0f} msg/s CPU {cpu2:.1f}%")


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
