import sys
import time
import types

import pytest


def _install_sqlalchemy_stub() -> None:
    def _noop(*_args, **_kwargs):
        return None

    class _SQLAlchemyModule(types.ModuleType):
        def __getattr__(self, name: str):  # type: ignore[override]
            if name == "event":
                return types.SimpleNamespace(listens_for=lambda *a, **k: (lambda func: func))
            return _noop

    stub = _SQLAlchemyModule("sqlalchemy")
    event_module = types.ModuleType("sqlalchemy.event")
    event_module.listens_for = lambda *a, **k: (lambda func: func)
    sys.modules["sqlalchemy.event"] = event_module
    sys.modules["sqlalchemy"] = stub

    orm_stub = types.ModuleType("sqlalchemy.orm")
    orm_stub.declarative_base = lambda *a, **k: type("Base", (), {})
    orm_stub.sessionmaker = lambda *a, **k: lambda **_kw: None
    sys.modules["sqlalchemy.orm"] = orm_stub


_install_sqlalchemy_stub()


def _install_base58_stub() -> None:
    if "base58" in sys.modules:
        return
    module = types.ModuleType("base58")
    module.b58decode = lambda *a, **k: b""
    module.b58encode = lambda *a, **k: b""
    sys.modules["base58"] = module


_install_base58_stub()

@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

from solhunter_zero.runtime import trading_runtime
from solhunter_zero.runtime.trading_runtime import TradingRuntime
from solhunter_zero.ui import create_app


@pytest.mark.anyio("asyncio")
async def test_exit_panel_provider_exposes_manager_summary(monkeypatch):
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()
    runtime._exit_manager.register_entry(
        "ABC",
        entry_price=100.0,
        size=5.0,
        breakeven_bps=25.0,
        ts=0.0,
        hot_watch=True,
    )
    runtime._exit_manager.record_missed_exit(
        "ABC", reason="spread_gate", diagnostics={"spread": 150.0}, ts=1.0
    )

    await runtime._start_ui()

    panel_snapshot = runtime.ui_state.exit_provider()
    assert panel_snapshot["hot_watch"], "expected hot_watch entries"
    assert panel_snapshot["diagnostics"], "expected diagnostics entries"

    app = create_app(runtime.ui_state)
    client = app.test_client()
    response = client.get("/swarm/exits")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["hot_watch"], "API should expose hot_watch data"
    assert payload["diagnostics"], "API should expose diagnostics data"


@pytest.mark.anyio("asyncio")
async def test_control_endpoints_publish_pause_and_safety(monkeypatch):
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()
    runtime._rl_window_sec = 2.5

    published: list = []

    def fake_publish(topic: str, payload: dict, *, _broadcast: bool = True) -> None:
        published.append((topic, dict(payload)))

    monkeypatch.setattr(trading_runtime, "publish", fake_publish)

    await runtime._start_ui()
    app = create_app(runtime.ui_state)
    client = app.test_client()

    response = client.post(
        "/api/control/pause",
        json={"active": True, "reason": "maintenance window"},
    )
    assert response.status_code == 200
    body = response.get_json()
    assert body["ok"] is True
    pause_messages = [item for item in published if item[0] == "control:execution:paused"]
    assert pause_messages, "pause should publish control topic"
    pause_payload = pause_messages[-1][1]
    assert pause_payload["value"] is True
    assert pause_payload.get("topic") == "control:execution:paused"
    assert pause_payload.get("ttl") == pytest.approx(runtime._rl_window_sec)
    pause_state = runtime._control_states["control:execution:paused"]
    assert time.time() - pause_state["updated_at"] <= pause_state.get("ttl", 9999)

    response = client.post("/api/control/pause", json={"active": False})
    assert response.status_code == 200
    pause_off = runtime._control_states["control:execution:paused"]
    assert pause_off["value"] is False

    published.clear()
    response = client.post("/api/control/safety-stop", json={"reason": "ops halt"})
    assert response.status_code == 200
    topics = [topic for topic, _ in published]
    assert "control:execution:safety_stop" in topics
    assert "control" in topics, "safety stop should broadcast stop command"
    safety_state = runtime._control_states["control:execution:safety_stop"]
    assert safety_state["value"] is True
    assert time.time() - safety_state["updated_at"] <= safety_state.get("ttl", 9999)
