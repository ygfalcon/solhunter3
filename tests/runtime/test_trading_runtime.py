import asyncio
import os
import sys
import types
from typing import Dict, List

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


def _install_jsonschema_stub() -> None:
    if "jsonschema" in sys.modules:
        return

    module = types.ModuleType("jsonschema")

    class _ValidationError(Exception):
        def __init__(self, message: str = "", *, instance=None, schema=None) -> None:
            super().__init__(message)
            self.message = message
            self.instance = instance
            self.schema = schema

    class _Validator:
        def __init__(self, schema):
            self.schema = schema

        def validate(self, _payload):
            return True

    module.Draft202012Validator = _Validator
    module.ValidationError = _ValidationError

    exceptions = types.ModuleType("jsonschema.exceptions")
    exceptions.ValidationError = _ValidationError

    module.exceptions = exceptions

    sys.modules["jsonschema"] = module
    sys.modules["jsonschema.exceptions"] = exceptions


_install_jsonschema_stub()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

from solhunter_zero import event_bus, resource_monitor
import solhunter_zero.runtime.trading_runtime as runtime_module
from solhunter_zero.runtime.trading_runtime import TradingRuntime
from solhunter_zero.ui import create_app
from solhunter_zero import ui as ui_module


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


def test_collect_health_metrics(monkeypatch):
    runtime = TradingRuntime()
    runtime.status.event_bus = True
    runtime._loop_delay = 12.0
    runtime._loop_min_delay = 2.0
    runtime._loop_max_delay = 30.0
    runtime._resource_snapshot = {"cpu": 50.0}
    runtime._resource_alerts = {"cpu": {"resource": "cpu", "active": True}}
    runtime.pipeline = types.SimpleNamespace(queue_snapshot=lambda: {"execution_queue": 4})

    monkeypatch.setattr(
        resource_monitor,
        "get_budget_status",
        lambda: {"cpu": {"active": True, "action": "exit", "ceiling": 80.0, "value": 85.0}},
    )
    monkeypatch.setattr(
        ui_module,
        "get_ws_client_counts",
        lambda: {"events": 1, "logs": 0, "rl": 0},
    )
    monkeypatch.setattr(runtime, "_internal_heartbeat_age", lambda: 5.0)

    metrics = runtime._collect_health_metrics()
    assert metrics["event_bus"]["connected"] is True
    assert metrics["resource"]["exit_active"] is True
    assert metrics["ui"]["ws_clients"]["events"] == 1


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_sets_stop_on_resource_exit(monkeypatch):
    runtime = TradingRuntime()
    runtime.memory = object()
    runtime.portfolio = object()
    runtime.agent_manager = object()

    async def fake_run_iteration(*_args, **_kwargs):
        return {}

    async def fake_apply(self, summary, stage="loop"):
        return None

    monkeypatch.setattr(runtime_module, "run_iteration", fake_run_iteration)
    monkeypatch.setattr(TradingRuntime, "_apply_iteration_summary", fake_apply)

    def fake_active(action: str):
        if action == "exit":
            return [{"resource": "cpu", "action": "exit"}]
        return []

    monkeypatch.setattr(runtime_module.resource_monitor, "active_budget", fake_active)

    task = asyncio.create_task(runtime._trading_loop())
    await asyncio.wait_for(task, timeout=0.5)
    assert runtime.stop_event.is_set()


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_updates_event_bus_url_for_auto_port(monkeypatch):
    monkeypatch.setenv("EVENT_BUS_WS_PORT", "0")
    monkeypatch.setenv("UI_ENABLED", "0")
    monkeypatch.setenv("EVENT_BUS_HEALTH_DISABLE", "1")

    runtime = TradingRuntime()

    monkeypatch.setattr(runtime_module, "ensure_local_redis_if_needed", lambda *_a, **_k: None)
    monkeypatch.setattr(resource_monitor, "start_monitor", lambda: None)

    assigned_port: Dict[str, int] = {"value": 0}
    connection_urls: List[str] = []

    class _DummySocket:
        def __init__(self, host: str, port: int) -> None:
            self._host = host
            self._port = port

        def getsockname(self) -> tuple[str, int]:
            return self._host, self._port

    class _DummyServer:
        def __init__(self, host: str, port: int) -> None:
            self.sockets = [_DummySocket(host, port)]

        def close(self) -> None:
            return None

        async def wait_closed(self) -> None:
            return None

    async def _fake_serve(handler, host: str, port: int, **_kwargs):
        actual_port = 43210 if not port else port
        assigned_port["value"] = actual_port
        return _DummyServer(host, actual_port)

    class _DummyConnection:
        def __init__(self, url: str) -> None:
            self.url = url

        async def close(self) -> None:
            return None

    async def _fake_connect(url: str, **_kwargs):
        connection_urls.append(url)
        return _DummyConnection(url)

    monkeypatch.setattr(
        event_bus,
        "websockets",
        types.SimpleNamespace(serve=_fake_serve, connect=_fake_connect),
    )

    async def _fake_verify(*_a, **_k) -> bool:
        return True

    monkeypatch.setattr(runtime_module, "verify_broker_connection", _fake_verify)

    prev_event_bus_url = os.environ.get("EVENT_BUS_URL")
    prev_broker_ws_urls = os.environ.get("BROKER_WS_URLS")
    original_url = event_bus.DEFAULT_WS_URL
    original_host, original_port = event_bus.get_ws_address()

    try:
        await asyncio.shield(runtime._start_event_bus())

        listen_host, listen_port = event_bus.get_ws_address()
        assert listen_port != 0, "expected auto-assigned port"

        expected_url = f"ws://{listen_host}:{listen_port}"
        assert os.environ["EVENT_BUS_URL"] == expected_url
        assert os.environ["BROKER_WS_URLS"] == expected_url

        entries = [e for e in runtime.activity.snapshot() if e["stage"] == "event_bus"]
        assert entries, "expected event bus activity entry"
        assert expected_url in entries[-1]["detail"]
        assert runtime.status.event_bus is True
        conn = await event_bus.websockets.connect(expected_url)
        await conn.close()
        assert connection_urls[-1] == expected_url
        assert assigned_port["value"] == listen_port
    finally:
        try:
            await event_bus.disconnect_ws()
        except Exception:
            pass
        try:
            await event_bus.stop_ws_server()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

        event_bus.DEFAULT_WS_URL = original_url
        event_bus._WS_LISTEN_HOST = original_host
        event_bus._WS_LISTEN_PORT = original_port

        if prev_event_bus_url is None:
            os.environ.pop("EVENT_BUS_URL", None)
        else:
            os.environ["EVENT_BUS_URL"] = prev_event_bus_url

        if prev_broker_ws_urls is None:
            os.environ.pop("BROKER_WS_URLS", None)
        else:
            os.environ["BROKER_WS_URLS"] = prev_broker_ws_urls
