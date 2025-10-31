import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_run_cleans_up_on_start_failure(monkeypatch, caplog):
    runtime = trading_runtime.TradingRuntime()
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_, **__: None)

    async def fake_prepare_configuration() -> None:
        runtime.cfg = {}

    async def fake_start_event_bus() -> None:
        runtime.bus_started = True

    ui_server = Mock()
    ui_server.stop = Mock()

    async def fake_start_ui() -> None:
        runtime.ui_server = ui_server

    async def failing_start_agents() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime, "_prepare_configuration", fake_prepare_configuration)
    monkeypatch.setattr(runtime, "_start_event_bus", fake_start_event_bus)
    monkeypatch.setattr(runtime, "_start_ui", fake_start_ui)
    monkeypatch.setattr(runtime, "_start_agents", failing_start_agents)
    monkeypatch.setattr(runtime, "_start_rl_status_watcher", lambda: None)
    monkeypatch.setattr(runtime, "_start_loop", AsyncMock())

    stop_ws_mock = AsyncMock()
    monkeypatch.setattr(trading_runtime, "stop_ws_server", stop_ws_mock)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="boom"):
            await runtime._run()

    assert runtime.stop_event.is_set()
    ui_server.stop.assert_called_once_with()
    assert stop_ws_mock.await_count == 1

    entries = runtime.activity.snapshot()
    assert any(
        entry["stage"] == "runtime"
        and entry["ok"] is False
        and "failed: boom" in entry["detail"]
        for entry in entries
    )

    assert any(
        "Trading runtime failed during run" in record.getMessage()
        for record in caplog.records
    )
