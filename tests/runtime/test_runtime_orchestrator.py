import asyncio
import asyncio
import importlib
import sys
import types

import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_start_agents_aborts_when_no_agents(monkeypatch, request):
    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.reload(importlib.import_module("solhunter_zero.runtime.orchestrator"))
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)
    stages: list[tuple[str, bool, str]] = []

    async def capture_stage(stage: str, ok: bool, detail: str = "") -> None:
        stages.append((stage, ok, detail))

    monkeypatch.setattr(orchestrator, "_publish_stage", capture_stage)

    async def fake_startup(config_path: str | None, offline: bool, dry_run: bool):
        return {}, {}, None

    monkeypatch.setattr(runtime_orchestrator, "perform_startup_async", fake_startup)

    class DummyMemory:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start_writer(self) -> None:
            pass

    class DummyPortfolio:
        def __init__(self, *_, **__):
            pass

    class DummyAgentManager:
        agents: list = []

        @classmethod
        def from_config(cls, *_: object, **__: object):
            return None

        @classmethod
        def from_default(cls):
            return None

    class DummyStrategyManager:
        def __init__(self, *_, **__):
            pass

    monkeypatch.setattr(runtime_orchestrator, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_orchestrator, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_orchestrator, "TradingState", lambda: object())
    monkeypatch.setattr(runtime_orchestrator, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(runtime_orchestrator, "StrategyManager", DummyStrategyManager)

    result = await orchestrator.start_agents()

    assert result is False
    assert orchestrator.handles.tasks == []

    failure_messages = [detail for stage, ok, detail in stages if stage == "agents:loaded" and not ok]
    assert failure_messages, "Expected a failed agents:loaded stage"
    assert "no agents available" in failure_messages[0]


@pytest.mark.anyio("asyncio")
async def test_discovery_loop_respects_override(monkeypatch, request):
    discovery_state = importlib.import_module("solhunter_zero.discovery_state")
    discovery_state.clear_override()
    request.addfinalizer(discovery_state.clear_override)

    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.import_module("solhunter_zero.runtime.orchestrator")
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    async def fake_startup(*_: object, **__: object):
        return (
            {
                "agents": True,
                "discovery_method": runtime_orchestrator.DEFAULT_DISCOVERY_METHOD,
                "loop_delay": 10,
            },
            {},
            None,
        )

    monkeypatch.setattr(runtime_orchestrator, "perform_startup_async", fake_startup)

    class DummyMemory:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start_writer(self) -> None:
            pass

    class DummyPortfolio:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class DummyAgentManager:
        evolve_interval = 30
        mutation_threshold = 0.0

        def __init__(self) -> None:
            self.agents = [types.SimpleNamespace(name="dummy")]

        @classmethod
        def from_config(cls, *_: object, **__: object) -> "DummyAgentManager":
            return cls()

        @classmethod
        def from_default(cls) -> "DummyAgentManager":
            return cls()

        async def evolve(self, *_: object, **__: object) -> None:
            pass

        async def update_weights(self) -> None:
            pass

        def save_weights(self) -> None:
            pass

    class DummyAgentRuntime:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        async def start(self) -> None:
            pass

    class DummyTradeExecutor:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start(self) -> None:
            pass

    class DummyDiscoveryAgent:
        instances: list["DummyDiscoveryAgent"] = []

        def __init__(self) -> None:
            self.calls: list[str] = []
            DummyDiscoveryAgent.instances.append(self)

        async def discover_tokens(self, *, method: str, offline: bool) -> list[str]:
            self.calls.append(method)
            return []

    async def fake_sleep(_: float) -> None:
        fake_sleep.calls += 1
        if fake_sleep.calls == 1:
            discovery_state.set_override("mempool")
            return
        raise asyncio.CancelledError

    fake_sleep.calls = 0  # type: ignore[attr-defined]

    class DummyTask:
        def __init__(self, coro):
            self.coro = coro

        def cancel(self) -> None:
            pass

        def __await__(self):  # pragma: no cover - exercised in test
            return self.coro.__await__()

    created_tasks: dict[str, DummyTask] = {}

    def fake_create_task(coro, *, name: str | None = None):
        task = DummyTask(coro)
        if name:
            created_tasks[name] = task
        return task

    monkeypatch.setattr(runtime_orchestrator, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_orchestrator, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_orchestrator, "TradingState", lambda: object())
    monkeypatch.setattr(runtime_orchestrator, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(runtime_orchestrator, "StrategyManager", lambda *_: None)
    runtime_agents_runtime = importlib.import_module("solhunter_zero.agents.runtime")
    monkeypatch.setattr(runtime_agents_runtime, "AgentRuntime", DummyAgentRuntime, raising=False)

    exec_service_module = importlib.import_module("solhunter_zero.exec_service")
    monkeypatch.setattr(exec_service_module, "TradeExecutor", DummyTradeExecutor)

    discovery_agent_module = importlib.import_module("solhunter_zero.agents.discovery")
    monkeypatch.setattr(discovery_agent_module, "DiscoveryAgent", DummyDiscoveryAgent)

    async def fake_init_rl_training(*_: object, **__: object) -> None:
        return None

    loop_module = importlib.import_module("solhunter_zero.loop")
    monkeypatch.setattr(loop_module, "_init_rl_training", fake_init_rl_training)
    monkeypatch.setattr(runtime_orchestrator, "_trading_loop", lambda *_, **__: None)
    async def publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        return None

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator,
        "_publish_stage",
        publish_stage,
    )
    monkeypatch.setattr(runtime_orchestrator.event_bus, "publish", lambda *_, **__: None)
    monkeypatch.setattr(runtime_orchestrator.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(runtime_orchestrator.asyncio, "create_task", fake_create_task)

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)

    result = await orchestrator.start_agents()
    assert result is True
    assert "discovery_loop" in created_tasks

    discovery_task = created_tasks["discovery_loop"]
    with pytest.raises(asyncio.CancelledError):
        await discovery_task

    instance = DummyDiscoveryAgent.instances[-1]
    assert instance.calls[:2] == [
        runtime_orchestrator.DEFAULT_DISCOVERY_METHOD,
        "mempool",
    ]


@pytest.mark.anyio("asyncio")
async def test_start_invokes_stop_all_on_stage_failure(monkeypatch):
    dummy_ui = types.ModuleType("solhunter_zero.ui")
    dummy_ui.create_app = lambda *_, **__: types.SimpleNamespace()
    dummy_ui.start_websockets = lambda: {}
    dummy_ui.UIState = type("UIState", (), {})
    dummy_ui.UIServer = type("UIServer", (), {})
    dummy_ui.UIStartupError = type("UIStartupError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", dummy_ui)
    monkeypatch.setattr(importlib.import_module("solhunter_zero"), "ui", dummy_ui, raising=False)

    sys.modules.pop("solhunter_zero.runtime.orchestrator", None)
    sys.modules.pop("solhunter_zero.runtime", None)
    runtime_orchestrator = importlib.import_module("solhunter_zero.runtime.orchestrator")

    class DummyBus:
        stop_calls = 0

        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        async def stop_ws_server(self):
            DummyBus.stop_calls += 1

    dummy_bus = DummyBus()
    monkeypatch.setattr(runtime_orchestrator, "event_bus", dummy_bus)

    async def fake_publish(self, *_args, **_kwargs):
        return None

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator,
        "_publish_stage",
        fake_publish,
        raising=False,
    )

    async def fake_start_ui(self):
        self.handles.ui_app = object()

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator, "start_ui", fake_start_ui
    )

    async def fail_bus(self):
        raise RuntimeError("boom")

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator, "start_bus", fail_bus
    )

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)

    with pytest.raises(RuntimeError):
        await orchestrator.start()

    assert orchestrator._closed is True
    assert orchestrator.handles.tasks == []

    # stop_all should remain safe to call again after a partial startup
    await orchestrator.stop_all()
    assert orchestrator._closed is True


@pytest.mark.anyio("asyncio")
async def test_stop_all_idempotent_after_partial_start(monkeypatch):
    dummy_ui = types.ModuleType("solhunter_zero.ui")
    dummy_ui.create_app = lambda *_, **__: types.SimpleNamespace()
    dummy_ui.start_websockets = lambda: {}
    dummy_ui.UIState = type("UIState", (), {})
    dummy_ui.UIServer = type("UIServer", (), {})
    dummy_ui.UIStartupError = type("UIStartupError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", dummy_ui)
    monkeypatch.setattr(importlib.import_module("solhunter_zero"), "ui", dummy_ui, raising=False)

    sys.modules.pop("solhunter_zero.runtime.orchestrator", None)
    sys.modules.pop("solhunter_zero.runtime", None)
    runtime_orchestrator = importlib.import_module("solhunter_zero.runtime.orchestrator")

    class DummyBus:
        def __init__(self) -> None:
            self.stop_calls = 0

        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

        async def stop_ws_server(self):
            self.stop_calls += 1

    dummy_bus = DummyBus()
    monkeypatch.setattr(runtime_orchestrator, "event_bus", dummy_bus)

    async def fake_publish(self, *_args, **_kwargs):
        return None

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator,
        "_publish_stage",
        fake_publish,
        raising=False,
    )

    async def fake_start_ui(self):
        self.handles.ui_app = object()

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator, "start_ui", fake_start_ui
    )

    async def fake_start_bus(self):
        self.handles.bus_started = True

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator, "start_bus", fake_start_bus
    )

    class DummyMetrics:
        def __init__(self) -> None:
            self.stopped = False

        def start(self) -> None:
            return None

        def stop(self) -> None:
            self.stopped = True

    class DummyRisk:
        def __init__(self) -> None:
            self.stopped = False

        def start(self) -> None:
            return None

        def stop(self) -> None:
            self.stopped = True

    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.metrics.decision_metrics",
        types.SimpleNamespace(DecisionMetrics=DummyMetrics),
    )
    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.risk_controller",
        types.SimpleNamespace(RiskController=DummyRisk),
    )

    async def fail_agents(self):
        raise RuntimeError("agents failed")

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator,
        "start_agents",
        fail_agents,
    )

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)

    with pytest.raises(RuntimeError):
        await orchestrator.start()

    assert dummy_bus.stop_calls == 1
    assert getattr(orchestrator, "_dec_metrics").stopped is True
    assert getattr(orchestrator, "_risk_ctl").stopped is True

    await orchestrator.stop_all()
    assert dummy_bus.stop_calls == 1


@pytest.mark.anyio("asyncio")
async def test_stop_all_clears_event_runtime_subscriptions(monkeypatch):
    event_bus_module = importlib.import_module("solhunter_zero.event_bus")
    event_bus_module.reset()

    dummy_ui = types.ModuleType("solhunter_zero.ui")
    dummy_ui.create_app = lambda *_, **__: types.SimpleNamespace()
    dummy_ui.start_websockets = lambda: {}
    dummy_ui.UIState = type("UIState", (), {})
    dummy_ui.UIServer = type("UIServer", (), {})
    dummy_ui.UIStartupError = type("UIStartupError", (Exception,), {})
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", dummy_ui)
    monkeypatch.setattr(importlib.import_module("solhunter_zero"), "ui", dummy_ui, raising=False)

    runtime_orchestrator = importlib.import_module("solhunter_zero.runtime.orchestrator")

    async def noop_publish(*_args, **_kwargs):
        return None

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)
    monkeypatch.setattr(orchestrator, "_publish_stage", noop_publish)

    async def fake_close_session() -> None:
        return None

    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.http",
        types.SimpleNamespace(close_session=fake_close_session),
    )

    service = importlib.import_module("solhunter_zero.exec_service.service")
    agents_pkg = importlib.import_module("solhunter_zero.agents")
    from importlib import util as importlib_util
    from pathlib import Path

    runtime_spec = importlib_util.spec_from_file_location(
        "solhunter_zero.agents.runtime_actual",
        Path(agents_pkg.__file__).with_name("runtime.py"),
    )
    assert runtime_spec and runtime_spec.loader
    runtime_module = importlib_util.module_from_spec(runtime_spec)
    sys.modules[runtime_spec.name] = runtime_module
    runtime_spec.loader.exec_module(runtime_module)

    class DummyMemory:
        def start_writer(self) -> None:
            pass

        async def log_trade(self, **_kwargs):
            return None

    class DummyPortfolio:
        def __init__(self) -> None:
            self.price_history: dict[str, list[float]] = {}
            self.balances: dict[str, object] = {}

        async def update_async(self, *_args, **_kwargs):
            return None

        def total_value(self, _prices):
            return 0.0

        def record_prices(self, *_args, **_kwargs):
            return None

    class DummyManager:
        def __init__(self) -> None:
            self.agents = [types.SimpleNamespace(name="dummy")]

        async def evaluate(self, *_args, **_kwargs):
            return []

    portfolio = DummyPortfolio()
    runtime = runtime_module.AgentRuntime(DummyManager(), portfolio)
    await runtime.start()

    executor = service.TradeExecutor(DummyMemory(), portfolio)
    executor.start()

    orchestrator._agent_runtime = runtime
    orchestrator._trade_executor = executor

    await orchestrator.stop_all()

    assert event_bus_module._subscribers.get("action_decision") is None
    assert event_bus_module._subscribers.get("token_discovered") is None
    assert event_bus_module._subscribers.get("price_update") is None
