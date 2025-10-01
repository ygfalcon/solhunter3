import argparse
import importlib
import os
import time
import asyncio

async def ws_latency(n: int, host: str, port: int) -> float:
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)
    await ev.start_ws_server(host, port)
    async with ev.websockets.connect(f"ws://{host}:{port}") as ws:
        latencies = []
        for _ in range(n):
            ts = time.perf_counter()
            ev.publish("heartbeat", {"service": str(ts)})
            raw = await ws.recv()
            ev_msg = ev.pb.Event()
            ev_msg.ParseFromString(ev._maybe_decompress(raw))
            latencies.append(time.perf_counter() - float(ev_msg.heartbeat.service))
    await ev.stop_ws_server()
    return sum(latencies) / len(latencies)


def _read_mmap(mm, pos):
    size = mm.size()
    if pos < 4 or pos >= size:
        pos = 4
    ln = int.from_bytes(mm[pos:pos+4], "little")
    pos += 4
    if pos + ln > size:
        part1 = bytes(mm[pos:size])
        remain = ln - (size - pos)
        part2 = bytes(mm[4:4+remain])
        pos = 4 + remain
        return part1 + part2, pos
    data = bytes(mm[pos:pos+ln])
    pos += ln
    if pos >= size:
        pos = 4
    return data, pos


def mmap_latency(n: int, path: str, size: int) -> float:
    os.environ["EVENT_BUS_MMAP"] = path
    os.environ["EVENT_BUS_MMAP_SIZE"] = str(size)
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)
    mm = ev.open_mmap()
    if mm is None:
        raise RuntimeError("failed to open mmap")
    pos = int.from_bytes(mm[:4], "little") or 4
    latencies = []
    for _ in range(n):
        ts = time.perf_counter()
        ev.publish("heartbeat", {"service": str(ts)})
        while True:
            new_pos = int.from_bytes(mm[:4], "little") or 4
            if new_pos != pos:
                break
            time.sleep(0.0001)
        data, pos = _read_mmap(mm, pos)
        ev_msg = ev.pb.Event()
        ev_msg.ParseFromString(ev._maybe_decompress(data))
        latencies.append(time.perf_counter() - float(ev_msg.heartbeat.service))
    return sum(latencies) / len(latencies)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark event bus latency")
    parser.add_argument("--messages", type=int, default=100)
    parser.add_argument("--mmap", action="store_true", help="measure mmap latency")
    parser.add_argument("--path", default="/tmp/events.mmap")
    parser.add_argument("--size", type=int, default=1 << 20)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8769)
    args = parser.parse_args()

    if args.mmap:
        avg = mmap_latency(args.messages, args.path, args.size)
        print(f"mmap average latency: {avg*1000:.3f} ms")
    else:
        avg = await ws_latency(args.messages, args.host, args.port)
        print(f"websocket average latency: {avg*1000:.3f} ms")


if __name__ == "__main__":
    asyncio.run(main())
