import subprocess
import sys
from unittest.mock import Mock

import pytest

from solhunter_zero.memory import Memory
from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_stop_waits_for_depth_process():
    runtime = trading_runtime.TradingRuntime()

    proc = Mock()
    proc.poll.return_value = None
    runtime.depth_proc = proc

    await runtime.stop()

    proc.terminate.assert_called_once_with()
    proc.wait.assert_called_once_with(
        timeout=trading_runtime.DEPTH_PROCESS_SHUTDOWN_TIMEOUT
    )


@pytest.mark.anyio("asyncio")
async def test_stop_reports_depth_process_timeout(caplog):
    runtime = trading_runtime.TradingRuntime()

    proc = Mock()
    proc.poll.return_value = None
    proc.wait.side_effect = subprocess.TimeoutExpired(cmd="depth_service", timeout=1)
    runtime.depth_proc = proc

    await runtime.stop()

    message = (
        "depth_service process did not exit within "
        f"{trading_runtime.DEPTH_PROCESS_SHUTDOWN_TIMEOUT} seconds"
    )
    entries = runtime.activity.snapshot()
    assert any(
        entry["stage"] == "depth_service" and entry["detail"] == message
        and entry["ok"] is False
        for entry in entries
    )

    assert any(message in record.getMessage() for record in caplog.records)


@pytest.mark.anyio("asyncio")
async def test_stop_drains_memory_writer(monkeypatch):
    runtime = trading_runtime.TradingRuntime()
    memory = Memory("sqlite:///:memory:")
    runtime.memory = memory

    memory.start_writer(batch_size=10, interval=60.0)
    await memory.wait_ready()

    flush_calls = 0
    original_flush = memory._flush

    async def tracking_flush(items):
        nonlocal flush_calls
        flush_calls += 1
        await original_flush(items)

    monkeypatch.setattr(memory, "_flush", tracking_flush)

    await memory.log_trade(
        token="SOL",
        direction="buy",
        amount=1.0,
        price=10.0,
    )

    # Trade should not be persisted until the writer is stopped.
    assert await memory.list_trades() == []

    await runtime.stop()

    assert flush_calls >= 1
    assert memory._writer_task is None
    assert memory._queue is None

    trades = await memory.list_trades()
    assert len(trades) == 1

    await memory.close()


@pytest.mark.anyio("asyncio")
async def test_stop_terminates_local_redis_process(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {"broker": {"urls": ["redis://127.0.0.1:6379"]}}

    monkeypatch.setattr(
        trading_runtime,
        "get_broker_urls",
        lambda cfg: ["redis://127.0.0.1:6379"],
    )

    spawned: list[subprocess.Popen[bytes]] = []

    def fake_ensure(urls):
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import time\nimport signal\nimport sys\n"
                "signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))\n"
                "time.sleep(60)",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        spawned.append(proc)
        return proc

    async def fake_start_ws_server(host, port):
        class DummyServer:
            sockets: list = []

        return DummyServer()

    async def fake_verify_broker_connection(*, timeout):
        return True

    async def fake_stop_ws_server():
        return None

    async def fake_sleep(delay):
        return None

    monkeypatch.setattr(trading_runtime, "ensure_local_redis_if_needed", fake_ensure)
    monkeypatch.setattr(trading_runtime, "start_ws_server", fake_start_ws_server)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )
    monkeypatch.setattr(trading_runtime, "stop_ws_server", fake_stop_ws_server)
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(runtime, "_subscribe_to_events", lambda: None)

    proc: subprocess.Popen[bytes] | None = None

    try:
        await runtime._start_event_bus()
        assert runtime._redis_process is not None
        proc = runtime._redis_process
        assert proc in spawned

        await runtime.stop()
        assert runtime._redis_process is None
        assert proc is not None
        proc.wait(timeout=5.0)
    finally:
        if spawned:
            for p in spawned:
                if p.poll() is None:
                    p.kill()
                    p.wait(timeout=5.0)
