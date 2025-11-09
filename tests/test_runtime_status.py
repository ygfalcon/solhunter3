from __future__ import annotations

import asyncio
import time

import pytest

import tests.runtime.test_trading_runtime  # ensure optional deps stubbed

import solhunter_zero.ui as ui
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

    asyncio.run(drive())

    status = collectors.status_snapshot()
    assert status["event_bus"] is True
    assert status["trading_loop"] is False
    assert status["last_stage"] == "runtime:stopping"
    assert status["last_stage_ok"] is True
    assert status["bus_latency_ms"] is not None
    assert "environment" in status
    assert status["workflow"] == runtime_wiring.DEFAULT_RUNTIME_WORKFLOW

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


def test_market_snapshot_falls_back_to_pipeline_fields(monkeypatch):
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

    async def drive() -> None:
        now = time.time()
        await emit(
            "x:market.ohlcv.5m",
            {"mint": "MINT", "c": "1.23", "vol_usd": "4567.89", "buyers": 3, "_received": now},
        )
        await emit(
            "x:market.depth",
            {"mint": "MINT", "buyers": "12", "sellers": 7, "spread_bps": "8.5", "_received": now},
        )

    asyncio.run(drive())

    snapshot = collectors.market_snapshot()
    assert snapshot["markets"], "expected markets snapshot entries"
    market = snapshot["markets"][0]
    assert market["mint"] == "MINT"
    assert market["close"] == pytest.approx(1.23)
    assert market["volume"] == pytest.approx(4567.89)
    assert market["buyers"] == 12
    assert market["sellers"] == 7

    collectors.stop()


def test_collectors_feed_fills_endpoint(monkeypatch):
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

    app = ui.create_app(state=ui_state)
    client = app.test_client()

    async def emit(topic: str, payload):
        for handler in handlers.get(topic, []):
            await handler(payload)

    async def drive() -> None:
        await emit(
            "x:virt.fills",
            {
                "mint": "MINT_V",
                "side": "buy",
                "qty_base": "1.25",
                "price_usd": "2.5",
                "ts": 1700000000.0,
                "order_id": "virt-123",
            },
        )
        await emit(
            "x:live.fills",
            {
                "mint": "MINT_L",
                "side": "sell",
                "qty_base": -0.5,
                "price_usd": 1.75,
                "timestamp": 1700000100.0,
                "venue": "dex",
            },
        )

    asyncio.run(drive())

    try:
        resp = client.get("/api/execution/fills?limit=5")
        assert resp.status_code == 200
        fills = resp.get_json()
        assert isinstance(fills, list)
        assert len(fills) == 2
        assert [entry.get("mint") for entry in fills] == ["MINT_L", "MINT_V"]
        assert {entry.get("source") for entry in fills} == {"virtual", "live"}
        live_fill = fills[0]
        assert live_fill.get("source") == "live"
        assert pytest.approx(live_fill.get("qty_base"), rel=1e-6) == -0.5
        assert live_fill.get("venue") == "dex"

        limited = client.get("/api/execution/fills?limit=1")
        assert limited.status_code == 200
        limited_fills = limited.get_json()
        assert isinstance(limited_fills, list)
        assert len(limited_fills) == 1
        assert limited_fills[0].get("source") == "live"
    finally:
        collectors.stop()
        ui._set_active_ui_state(None)
