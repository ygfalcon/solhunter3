from __future__ import annotations

import pytest

from solhunter_zero.ui import UIState
from solhunter_zero.runtime import runtime_wiring


def test_collectors_status_snapshot_updates(monkeypatch):
    handlers: dict[str, list] = {}

    def fake_subscribe(topic, handler):
        handlers.setdefault(topic, []).append(handler)
        return lambda: None

    monkeypatch.setattr(runtime_wiring, "subscribe", fake_subscribe)

    collectors = runtime_wiring.RuntimeEventCollectors()
    collectors.start()

    async def emit(topic: str, payload):
        for handler in handlers.get(topic, []):
            await handler(payload)

    async def drive():
        await emit("runtime.stage_changed", {"stage": "bus:verify", "ok": True})
        await emit("runtime.stage_changed", {"stage": "agents:loop", "ok": True})
        await emit("heartbeat", {"service": "trading_loop"})
        await emit("runtime.stage_changed", {"stage": "runtime:stopping", "ok": True})

    import asyncio

    asyncio.run(drive())

    status = collectors.status_snapshot()
    assert status["event_bus"] is True
    assert status["trading_loop"] is False
    assert status["last_stage"] == "runtime:stopping"
    assert status["last_stage_ok"] is True
    assert status["bus_latency_ms"] is not None
    assert "environment" in status

    collectors.stop()


def test_wire_ui_state_sets_status_provider(monkeypatch):
    handlers: dict[str, list] = {}

    def fake_subscribe(topic, handler):
        handlers.setdefault(topic, []).append(handler)
        return lambda: None

    monkeypatch.setattr(runtime_wiring, "subscribe", fake_subscribe)

    collectors = runtime_wiring.RuntimeEventCollectors()
    collectors.start()
    ui_state = UIState()
    wiring = runtime_wiring.RuntimeWiring(collectors=collectors)
    wiring.wire_ui_state(ui_state)

    provider = ui_state.status_provider
    assert callable(provider)
    assert getattr(provider, "__self__", None) is collectors
    assert provider() == collectors.status_snapshot()

    collectors.stop()
