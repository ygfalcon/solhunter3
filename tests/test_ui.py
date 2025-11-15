import asyncio
import json
import logging
import os
import threading
import time
from typing import Any, Dict, Iterable

import pytest
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

import solhunter_zero.ui as ui
from solhunter_zero.runtime import runtime_wiring
from solhunter_zero.runtime_defaults import DEFAULT_UI_PORT


@pytest.fixture(scope="module")
def flask_app() -> Flask:
    app = ui.create_app()
    return app


@pytest.fixture()
def client(flask_app: Flask):
    return flask_app.test_client()


def _active_state() -> ui.UIState:
    state = ui._get_active_ui_state()
    assert state is not None
    return state


def test_split_netloc_invalid_port():
    host, port, scheme = ui._split_netloc("wss://example.com:notaport")

    assert host == "example.com"
    assert port is None
    assert scheme == "wss"


def test_bootstrap_ui_environment_respects_existing(monkeypatch, caplog):
    monkeypatch.setattr(ui, "_ENV_BOOTSTRAPPED", False)
    monkeypatch.setenv("SOLHUNTER_MODE", "paper")
    monkeypatch.setenv("BROKER_CHANNEL", "custom-channel")
    monkeypatch.setenv("REDIS_URL", "redis://example")

    caplog.set_level(logging.INFO, logger="solhunter_zero.ui")
    ui._bootstrap_ui_environment()

    try:
        assert os.environ["SOLHUNTER_MODE"] == "paper"
        assert os.environ["BROKER_CHANNEL"] == "custom-channel"
        assert os.environ["REDIS_URL"] == "redis://example"

        skip_messages = [
            record
            for record in caplog.records
            if record.name == "solhunter_zero.ui"
            and "skipping default" in record.getMessage()
        ]
        assert len(skip_messages) == 3
    finally:
        ui._teardown_ui_environment()


def test_bootstrap_ui_environment_defaults_live(monkeypatch):
    monkeypatch.setattr(ui, "_ENV_BOOTSTRAPPED", False)
    monkeypatch.setattr(ui, "load_production_env", lambda: {})
    for name in ("SOLHUNTER_MODE", "BROKER_CHANNEL", "REDIS_URL"):
        monkeypatch.delenv(name, raising=False)

    ui._bootstrap_ui_environment()
    try:
        assert os.environ["SOLHUNTER_MODE"] == "live"
    finally:
        ui._teardown_ui_environment()


def test_bootstrap_ui_environment_preserves_redis_url(monkeypatch):
    monkeypatch.setattr(ui, "_ENV_BOOTSTRAPPED", False)
    monkeypatch.setattr(ui, "load_production_env", lambda: {})
    monkeypatch.delenv("SOLHUNTER_MODE", raising=False)
    monkeypatch.delenv("BROKER_CHANNEL", raising=False)
    monkeypatch.setenv("REDIS_URL", "redis://cache.example:6380/2")

    ui._bootstrap_ui_environment()
    try:
        assert os.environ["REDIS_URL"] == "redis://cache.example:6380/2"
        assert os.environ["SOLHUNTER_MODE"] == "live"
        assert os.environ["BROKER_CHANNEL"] == "solhunter-events-v3"
    finally:
        ui._teardown_ui_environment()


def test_create_app_proxy_fix_opt_in(monkeypatch):
    monkeypatch.setenv("UI_PROXY_FIX", "1")
    previous_state = ui._get_active_ui_state()
    app = ui.create_app(previous_state)
    try:
        assert isinstance(app.wsgi_app, ProxyFix)
    finally:
        ui._set_active_ui_state(previous_state)


def test_active_state_not_overwritten_mid_request(monkeypatch):
    monkeypatch.setattr(ui, "_ui_meta_cache", None)

    live_state = ui.UIState()
    live_state.run_state_provider = lambda: {"mode": "live"}
    ui._set_active_ui_state(live_state)

    request_entered = threading.Event()
    release_request = threading.Event()
    setter_started = threading.Event()
    setter_finished = threading.Event()

    def blocking_get_active_state():
        with ui._ACTIVE_UI_STATE_LOCK:
            request_entered.set()
            release_request.wait(timeout=1)
            return ui._active_ui_state

    monkeypatch.setattr(ui, "_get_active_ui_state", blocking_get_active_state)

    meta_holder: Dict[str, Any] = {}

    def request_meta_snapshot():
        meta_holder["meta"] = ui.get_ui_meta_snapshot(force=True)

    request_thread = threading.Thread(target=request_meta_snapshot)
    request_thread.start()
    assert request_entered.wait(timeout=1)

    def set_paper_state():
        setter_started.set()
        paper_state = ui.UIState()
        paper_state.run_state_provider = lambda: {"mode": "paper"}
        ui._set_active_ui_state(paper_state)
        setter_finished.set()

    setter_thread = threading.Thread(target=set_paper_state)
    setter_thread.start()
    assert setter_started.wait(timeout=1)
    time.sleep(0.05)
    assert not setter_finished.is_set()

    release_request.set()
    request_thread.join(timeout=1)
    setter_thread.join(timeout=1)
    assert not request_thread.is_alive()
    assert not setter_thread.is_alive()
    assert setter_finished.is_set()

    assert meta_holder["meta"]["run_state"]["mode"] == "live"


def test_run_state_endpoint(client, monkeypatch):
    monkeypatch.setenv("SOLHUNTER_MODE", "live")
    monkeypatch.setenv("KEYPAIR_PATH", "/tmp/example-keypair.json")
    resp = client.get("/api/run/state")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["mode"] == "live"
    assert "workflow" in payload
    assert "keypair_pubkey" in payload


def test_run_env_endpoint_masks(monkeypatch, client):
    monkeypatch.setenv("SOLHUNTER_API_KEY", "abcdef123456")
    resp = client.get("/api/run/env")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "SOLHUNTER_API_KEY" in data
    assert data["SOLHUNTER_API_KEY"].startswith("ab")
    assert "…" in data["SOLHUNTER_API_KEY"]


def test_discovery_recent_endpoint_uses_provider(monkeypatch, client):
    state = _active_state()

    def provider(limit: int) -> Iterable[Dict[str, Any]]:
        assert limit == 25
        return [{"mint": "mint1", "score": 1.0}]

    monkeypatch.setattr(state, "discovery_recent_provider", provider)
    resp = client.get("/api/discovery/recent?limit=25")
    assert resp.status_code == 200
    items = resp.get_json()
    assert items == [{"mint": "mint1", "score": 1.0}]


def test_token_meta_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "token_meta_provider", lambda mint: {"mint": mint, "name": "Token"})
    resp = client.get("/api/token/meta/abc")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["mint"] == "abc"
    assert payload["name"] == "Token"


def test_token_snapshot_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "token_snapshot_provider", lambda mint: {"mint": mint, "supply": 42})
    resp = client.get("/api/token/xyz")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload == {"mint": "xyz", "supply": 42}


def test_token_price_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "token_price_provider", lambda mint: {"mint": mint, "price": 1.23})
    resp = client.get("/api/price/mno")
    assert resp.status_code == 200
    assert resp.get_json()["price"] == 1.23


def test_agent_events_endpoint(monkeypatch, client):
    state = _active_state()

    def provider(mint: str, since: float | None) -> Iterable[Dict[str, Any]]:
        assert mint == "mintA"
        assert since == 100.0
        return [{"intent": "buy"}]

    monkeypatch.setattr(state, "agent_events_provider", provider)
    resp = client.get("/api/agents/events?mint=mintA&since=100")
    assert resp.status_code == 200
    assert resp.get_json() == [{"intent": "buy"}]


def test_execution_plan_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "execution_plan_provider", lambda mint: {"mint": mint, "route": "jupiter"})
    resp = client.get("/api/execution/plan?mint=test")
    assert resp.status_code == 200
    assert resp.get_json()["route"] == "jupiter"


def test_execution_fills_endpoint(monkeypatch, client):
    state = _active_state()

    def provider(limit: int) -> Iterable[Dict[str, Any]]:
        assert limit == 10
        return [{"mint": "abc", "qty": 1}]

    monkeypatch.setattr(state, "fills_provider", provider)
    resp = client.get("/api/execution/fills?limit=10")
    assert resp.status_code == 200
    assert resp.get_json() == [{"mint": "abc", "qty": 1}]


def test_execution_close_requires_params(client):
    resp = client.post("/api/execution/close", data={})
    assert resp.status_code == 400
    assert resp.get_json()["ok"] is False


def test_execution_close_delegates_handler(monkeypatch, client):
    state = _active_state()

    def handler(mint: str, qty: float) -> Dict[str, Any]:
        return {"ok": True, "mint": mint, "qty": qty}

    monkeypatch.setattr(state, "close_position_handler", handler)
    resp = client.post("/api/execution/close?mint=abc&qty=2")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True, "mint": "abc", "qty": 2.0}


def test_positions_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "positions_provider", lambda: [{"mint": "abc", "qty": 5}])
    resp = client.get("/api/portfolio/positions")
    assert resp.status_code == 200
    assert resp.get_json() == [{"mint": "abc", "qty": 5}]


def test_pnl_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "pnl_provider", lambda window: {"window": window, "pnl": 10})
    resp = client.get("/api/portfolio/pnl?window=1h")
    assert resp.status_code == 200
    assert resp.get_json() == {"window": "1h", "pnl": 10}


def test_risk_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "risk_provider", lambda: {"var": 5})
    resp = client.get("/api/risk/state")
    assert resp.status_code == 200
    assert resp.get_json() == {"var": 5}


def test_provider_status_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "provider_status_provider", lambda: [{"name": "helius"}])
    resp = client.get("/api/providers/status")
    assert resp.status_code == 200
    assert resp.get_json() == [{"name": "helius"}]


def test_health_endpoint(monkeypatch, client):
    state = _active_state()

    handlers: dict[str, list] = {}

    def fake_subscribe(topic, handler):
        handlers.setdefault(topic, []).append(handler)
        return lambda: None

    monkeypatch.setattr(runtime_wiring, "subscribe", fake_subscribe)

    collectors = runtime_wiring.RuntimeEventCollectors()
    collectors.start()

    monkeypatch.setattr(state, "health_provider", collectors.health_snapshot)

    async def emit(topic: str, payload):
        for handler in handlers.get(topic, []):
            await handler(payload)

    async def drive():
        base = time.time()
        await emit(
            "runtime.stage_changed",
            {
                "stage": "bus:verify",
                "ok": True,
                "timestamp": base,
                "elapsed": 0.0,
            },
        )
        await emit(
            "runtime.stage_changed",
            {
                "stage": "agents:loop",
                "ok": True,
                "timestamp": base + 0.1,
                "elapsed": 0.1,
            },
        )
        await emit("heartbeat", {"service": "trading_loop"})
        await emit(
            "runtime.stage_changed",
            {
                "stage": "runtime:stopping",
                "ok": True,
                "timestamp": base + 0.2,
                "elapsed": 0.2,
            },
        )

    try:
        asyncio.run(drive())

        resp = client.get("/api/health")
        assert resp.status_code == 200
        payload = resp.get_json()
        assert payload["event_bus"] is True
        assert payload["trading_loop"] is False
        assert payload["last_stage"] == "runtime:stopping"
        assert payload["last_stage_ok"] is True
        assert payload["heartbeat_ts"]
        assert payload["last_stage_ts"]
    finally:
        collectors.stop()


def test_root_health_defaults_to_false(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "health_provider", lambda: {})

    resp = client.get("/health")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["ok"] is False
    assert payload["health"]["ok"] is False


def test_logs_tail_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "logs_provider", lambda: [{"msg": "a"}, {"msg": "b"}])
    resp = client.get("/api/logs/tail?lines=1")
    assert resp.status_code == 200
    assert resp.get_json() == [{"msg": "b"}]


def test_logs_tail_endpoint_zero_lines(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "logs_provider", lambda: [{"msg": "a"}, {"msg": "b"}])
    resp = client.get("/api/logs/tail?lines=0")
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_token_depth_endpoint(monkeypatch, client):
    state = _active_state()
    monkeypatch.setattr(state, "token_depth_provider", lambda mint: {"mint": mint, "bids": []})
    resp = client.get("/api/depth/abc")
    assert resp.status_code == 200
    assert resp.get_json() == {"mint": "abc", "bids": []}


def test_manifest_ui_port_defaults(monkeypatch):
    monkeypatch.delenv("UI_PORT", raising=False)
    monkeypatch.delenv("PORT", raising=False)

    manifest = ui.build_ui_manifest()

    assert manifest["ui_port"] == DEFAULT_UI_PORT


def test_agent_events_without_mint(client):
    resp = client.get("/api/agents/events")
    assert resp.status_code == 200
    assert resp.get_json() == []


def test_discovery_recent_default(client):
    resp = client.get("/api/discovery/recent")
    assert resp.status_code == 200
    assert isinstance(resp.get_json(), list)
