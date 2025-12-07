import asyncio
import socket
from collections import defaultdict
from contextlib import suppress


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_gateway_soak_restarts_with_active_discovery_streams():
    port = _find_free_port()
    drain_logs: list[int] = []
    ports_seen: set[int] = set()
    frames: list[tuple[str | None, str]] = []
    active_consumers: defaultdict[str | None, int] = defaultdict(int)
    duplicate_consumers: list[str] = []
    restart_cycles = 3

    async def runner() -> None:
        stop_event = asyncio.Event()
        gateway_active = asyncio.Event()

        async def run_gateway_cycles():
            for _ in range(restart_cycles):
                ports_seen.add(port)
                gateway_active.set()
                await asyncio.sleep(0.05)
                gateway_active.clear()
                drain_logs.append(len(frames))
                await asyncio.sleep(0.02)
            stop_event.set()

        async def discovery_stream(name: str):
            while not stop_event.is_set():
                try:
                    await gateway_active.wait()
                    if stop_event.is_set():
                        break
                    active_consumers[name] += 1
                    if active_consumers[name] > 1:
                        duplicate_consumers.append(name)
                    for idx in range(3):
                        frames.append((name, f"{name}-frame-{idx}"))
                        await asyncio.sleep(0.005)
                    active_consumers[name] -= 1
                    if active_consumers[name] <= 0:
                        active_consumers.pop(name, None)
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    break

        discovery_tasks = [
            asyncio.create_task(discovery_stream("alpha")),
            asyncio.create_task(discovery_stream("beta")),
        ]
        gateway_task = asyncio.create_task(run_gateway_cycles())

        await gateway_task
        for task in discovery_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    asyncio.run(runner())

    assert ports_seen == {port}, "gateway should reuse the same port across restarts"
    assert len(drain_logs) == restart_cycles, "gateway should record a drain log for every restart"
    assert not duplicate_consumers, "discovery consumers should not duplicate across restarts"
    assert len(frames) > 0, "discovery streams should stay active and send frames during the soak"
