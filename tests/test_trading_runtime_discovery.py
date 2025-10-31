from __future__ import annotations

import asyncio

import pytest

from solhunter_zero.runtime import trading_runtime
from solhunter_zero import ui


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_refreshes_discovery(monkeypatch):
    monkeypatch.setenv(
        "DISCOVERY_METHOD", trading_runtime.DEFAULT_DISCOVERY_METHOD
    )

    runtime = trading_runtime.TradingRuntime(
        loop_delay=0.05,
        min_delay=0.0,
        max_delay=0.05,
    )
    runtime.cfg = {}
    runtime.runtime_cfg = {}
    runtime.memory = object()
    runtime.portfolio = object()
    runtime.agent_manager = object()
    runtime._use_new_pipeline = False

    methods: list[str | None] = []

    async def fake_run_iteration(*args, **kwargs):
        methods.append(kwargs.get("discovery_method"))
        if len(methods) >= 2:
            runtime.stop_event.set()
        return {}

    monkeypatch.setattr(trading_runtime, "run_iteration", fake_run_iteration)

    published: list[tuple[str, dict[str, str]]] = []

    def fake_publish(topic: str, payload: dict[str, str], *, _broadcast: bool = True):
        published.append((topic, payload))
        if topic == "discovery.updated":
            runtime._discovery_refresh_event.set()

    monkeypatch.setattr(ui, "publish", fake_publish)

    app = ui.create_app(ui.UIState())

    loop_task = asyncio.create_task(runtime._trading_loop())

    async def wait_for_methods(count: int, timeout: float = 2.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if len(methods) >= count:
                return
            await asyncio.sleep(0.01)
        raise AssertionError(f"timed out waiting for {count} iterations: {methods}")

    await wait_for_methods(1)
    assert methods[0] == trading_runtime.DEFAULT_DISCOVERY_METHOD

    def post_update():
        with app.test_client() as client:
            return client.post("/discovery", json={"method": "mempool"})

    response = await asyncio.to_thread(post_update)
    assert response.status_code == 200

    await wait_for_methods(2)
    assert methods[1] == "mempool"

    assert ("discovery.updated", {"method": "mempool"}) in published

    await asyncio.wait_for(loop_task, timeout=2.0)
