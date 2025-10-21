from __future__ import annotations

import asyncio
import time

import pytest

import tests.runtime.test_trading_runtime  # ensure optional deps stubbed

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


def test_golden_snapshot_flat_fields(monkeypatch):
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
            "x:mint.golden@v2",
            {
                "mint": "MINT",
                "hash": "hash",
                "px_mid_usd": "1.23",
                "liq_depth_1pct_usd": "456.0",
                "asof": now,
                "schema_version": "2.0",
                "content_hash": "abc",
            },
        )

    asyncio.run(drive())

    snapshot = collectors.golden_snapshot()
    assert snapshot["snapshots"], "expected golden snapshot entries"
    entry = snapshot["snapshots"][0]
    assert entry["px"] == pytest.approx(1.23)
    assert entry["liq"] == pytest.approx(456.0)

    collectors.stop()
