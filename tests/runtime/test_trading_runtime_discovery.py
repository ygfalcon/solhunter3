import asyncio
from unittest.mock import Mock

import pytest

from solhunter_zero import discovery_state, ui
from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_discovery_post_updates_method_without_restart(monkeypatch):
    monkeypatch.delenv("DISCOVERY_METHOD", raising=False)
    discovery_state.clear_override()

    runtime = trading_runtime.TradingRuntime()
    runtime.agent_manager = Mock()
    runtime.memory = object()
    runtime.portfolio = object()
    runtime.runtime_cfg = {}
    runtime.cfg = {"discovery_method": "helius"}
    runtime.ui_state.discovery_update_callback = runtime._on_discovery_method_update

    app = ui.create_app(runtime.ui_state)
    client = app.test_client()

    captured_methods: list[str | None] = []

    async def fake_run_iteration(*_, **kwargs):
        method = kwargs.get("discovery_method")
        captured_methods.append(method)
        if len(captured_methods) == 1:
            response = client.post(
                "/discovery",
                json={"method": "websocket"},
                environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
            )
            assert response.status_code == 200
        else:
            runtime.stop_event.set()
        return {}

    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(trading_runtime, "run_iteration", fake_run_iteration)
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)

    await runtime._trading_loop()

    assert captured_methods[:2] == ["helius", "websocket"]


def test_discovery_update_refreshes_pipeline(monkeypatch):
    monkeypatch.delenv("DISCOVERY_METHOD", raising=False)
    discovery_state.clear_override()

    runtime = trading_runtime.TradingRuntime()
    refreshed: list[bool] = []

    class _Pipeline:
        def refresh_discovery(self) -> None:
            refreshed.append(True)

    runtime.pipeline = _Pipeline()

    runtime._on_discovery_method_update("mempool")
    assert refreshed == [True]

    runtime._on_discovery_method_update("mempool")
    assert refreshed == [True]
