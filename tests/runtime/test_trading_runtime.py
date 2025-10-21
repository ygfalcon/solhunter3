import asyncio
import sys
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

from solhunter_zero import resource_monitor
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
