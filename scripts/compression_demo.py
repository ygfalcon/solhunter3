import os
import asyncio
import importlib
import time

import websockets

import solhunter_zero.event_bus as ev
from solhunter_zero import event_pb2

async def measure(compress: bool) -> tuple[int, float]:
    if compress:
        os.environ["COMPRESS_EVENTS"] = "1"
        os.environ["EVENT_COMPRESSION"] = "zstd"
    else:
        os.environ["COMPRESS_EVENTS"] = "0"
        os.environ["EVENT_COMPRESSION"] = "none"
    os.environ["EVENT_COMPRESSION_THRESHOLD"] = "0"
    importlib.reload(ev)
    port = 8799 if compress else 8798
    await ev.start_ws_server("localhost", port)
    async with websockets.connect(
        f"ws://localhost:{port}",
        ping_interval=float(os.getenv("WS_PING_INTERVAL", "20") or 20),
        ping_timeout=float(os.getenv("WS_PING_TIMEOUT", "20") or 20),
    ) as ws:
        start = time.perf_counter()
        ev.publish("depth_service_status", {"status": "ok"})
        raw = await ws.recv()
        latency = time.perf_counter() - start
    await ev.stop_ws_server()
    size = len(raw) if isinstance(raw, (bytes, bytearray)) else len(str(raw))
    return size, latency

async def main() -> None:
    size_plain, lat_plain = await measure(False)
    size_zstd, lat_zstd = await measure(True)
    print(f"plain: {size_plain} bytes, latency {lat_plain*1000:.3f} ms")
    print(f"zstd:  {size_zstd} bytes, latency {lat_zstd*1000:.3f} ms")

if __name__ == "__main__":
    asyncio.run(main())
