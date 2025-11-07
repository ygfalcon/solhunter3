import asyncio
import importlib
import os
import sys
import types
import warnings

import pytest

from solhunter_zero.services import DepthServiceStartupError


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
async def test_start_bus_reinitializes_after_local_bind(monkeypatch, request):
    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    event_bus_module = importlib.import_module("solhunter_zero.event_bus")
    config_module = importlib.import_module("solhunter_zero.config")
    redis_util = importlib.import_module("solhunter_zero.redis_util")

    monkeypatch.setenv("BROKER_WS_URLS", "ws://remote:9999")
    monkeypatch.setenv("EVENT_BUS_URL", "ws://remote:9999")
    monkeypatch.setenv("EVENT_BUS_WS_PORT", "9101")

    event_bus_module.BUS.reset()
    event_bus_module.configure(broker_urls=["ws://remote:9999"])

    init_calls = {"count": 0}

    def fake_initialize_event_bus() -> None:
        init_calls["count"] += 1
        urls_env = os.getenv("BROKER_WS_URLS", "")
        urls = [u for u in urls_env.split(",") if u]
        event_bus_module.configure(broker_urls=urls)

    monkeypatch.setattr(runtime_orchestrator, "initialize_event_bus", fake_initialize_event_bus)
    monkeypatch.setattr(runtime_orchestrator, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(runtime_orchestrator, "load_selected_config", lambda: {})
    monkeypatch.setattr(runtime_orchestrator, "load_config", lambda path=None: {})
    monkeypatch.setattr(config_module, "set_env_from_config", lambda cfg: None)
    monkeypatch.setattr(config_module, "get_broker_urls", lambda cfg: [])
    monkeypatch.setattr(redis_util, "ensure_local_redis_if_needed", lambda urls: None)

    async def fake_start_ws_server(host, port):
        fake_start_ws_server.calls.append((host, port))

    fake_start_ws_server.calls = []  # type: ignore[attr-defined]
    monkeypatch.setattr(event_bus_module, "start_ws_server", fake_start_ws_server)

    async def fake_verify_broker_connection(*_: object, **__: object) -> bool:
        return True

    monkeypatch.setattr(event_bus_module, "verify_broker_connection", fake_verify_broker_connection)

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)

    await orchestrator.start_bus()

    assert init_calls["count"] == 2
    assert fake_start_ws_server.calls == [("localhost", 9101)]
    assert orchestrator.handles.local_ws_bound is True
    assert event_bus_module.BUS.broker_urls == ["ws://127.0.0.1:9101"]


@pytest.mark.anyio("asyncio")
async def test_discovery_loop_respects_override(monkeypatch, request):
    discovery_state = importlib.import_module("solhunter_zero.discovery_state")
    discovery_state.clear_override()
    request.addfinalizer(discovery_state.clear_override)

    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )
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
        token = "TEST_TOKEN"

        def __init__(self) -> None:
            self.calls: list[str] = []
            DummyDiscoveryAgent.instances.append(self)

        async def discover_tokens(self, *, method: str, offline: bool) -> list[str]:
            self.calls.append(method)
            return [self.token]

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
    published_events: list[tuple[str, object]] = []

    def capture_publish(topic: str, payload: object, *_, **__) -> None:
        published_events.append((topic, payload))

    monkeypatch.setattr(runtime_orchestrator.event_bus, "publish", capture_publish)
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

    token_events = [payload for topic, payload in published_events if topic == "token_discovered"]
    assert len(token_events) >= 2
    first_event, second_event = token_events[:2]
    assert first_event == {
        "tokens": [DummyDiscoveryAgent.token],
        "metadata_refresh": False,
        "changed_tokens": [],
    }
    assert second_event == {
        "tokens": [DummyDiscoveryAgent.token],
        "metadata_refresh": True,
        "changed_tokens": [DummyDiscoveryAgent.token],
    }


@pytest.mark.anyio("asyncio")
async def test_start_reaches_ready_when_depth_service_fails(monkeypatch, request):
    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    monkeypatch.setenv("EVENT_DRIVEN", "0")
    warnings.filterwarnings(
        "ignore",
        message="coroutine 'measure_dex_latency_async",
        category=RuntimeWarning,
    )

    async def failing_startup(*_: object, **__: object) -> None:
        raise DepthServiceStartupError("boom")

    monkeypatch.setattr(runtime_orchestrator, "perform_startup_async", failing_startup)

    class DummyMemory:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start_writer(self) -> None:
            pass

    class DummyPortfolio:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class DummyAgentManager:
        def __init__(self) -> None:
            self.agents = [types.SimpleNamespace(name="dummy")]

        @classmethod
        def from_config(cls, *_: object, **__: object):
            return cls()

        @classmethod
        def from_default(cls):
            return cls()

    class DummyStrategyManager:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    async def fake_trading_loop(*_: object, **__: object) -> None:
        return None

    class DummyDecisionMetrics:
        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

    class DummyRiskController:
        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

    decision_metrics_module = types.ModuleType("solhunter_zero.metrics.decision_metrics")
    decision_metrics_module.DecisionMetrics = DummyDecisionMetrics
    monkeypatch.setitem(sys.modules, "solhunter_zero.metrics.decision_metrics", decision_metrics_module)

    risk_module = types.ModuleType("solhunter_zero.risk_controller")
    risk_module.RiskController = DummyRiskController
    monkeypatch.setitem(sys.modules, "solhunter_zero.risk_controller", risk_module)

    monkeypatch.setattr(runtime_orchestrator, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_orchestrator, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_orchestrator, "TradingState", lambda: object())
    monkeypatch.setattr(runtime_orchestrator, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(runtime_orchestrator, "StrategyManager", DummyStrategyManager)
    monkeypatch.setattr(runtime_orchestrator, "_trading_loop", fake_trading_loop)

    class DummyTask:
        def __init__(self, coro):
            self.coro = coro

        def cancel(self) -> None:
            pass

        def __await__(self):
            return self.coro.__await__()

    def fake_create_task(coro, *, name: str | None = None):
        return DummyTask(coro)

    monkeypatch.setattr(runtime_orchestrator.asyncio, "create_task", fake_create_task)

    async def fake_start_ui() -> None:
        return None

    async def fake_start_bus() -> None:
        return None

    async def fake_stop_ws_server() -> None:
        return None

    monkeypatch.setattr(runtime_orchestrator.event_bus, "publish", lambda *_, **__: None)
    monkeypatch.setattr(runtime_orchestrator.event_bus, "subscribe", lambda *_, **__: None)
    monkeypatch.setattr(runtime_orchestrator.event_bus, "stop_ws_server", fake_stop_ws_server)

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)
    monkeypatch.setattr(orchestrator, "start_ui", fake_start_ui)
    monkeypatch.setattr(orchestrator, "start_bus", fake_start_bus)

    stages: list[tuple[str, bool, str]] = []

    async def capture_stage(stage: str, ok: bool, detail: str = "") -> None:
        stages.append((stage, ok, detail))

    monkeypatch.setattr(orchestrator, "_publish_stage", capture_stage)

    await orchestrator.start()
    await orchestrator.stop_all()

    ready_stages = [stage for stage in stages if stage[0] == "runtime:ready" and stage[1]]
    assert ready_stages, "Runtime should reach ready stage despite depth service failure"
    assert orchestrator.handles.depth_proc is None
    assert os.getenv("DEPTH_SERVICE") == "false"
    os.environ.pop("DEPTH_SERVICE", None)


@pytest.mark.anyio("asyncio")
async def test_start_invokes_stop_all_on_stage_failure(monkeypatch):
    dummy_ui = types.ModuleType("solhunter_zero.ui")
    dummy_ui.create_app = lambda *_, **__: types.SimpleNamespace()
    dummy_ui.start_websockets = lambda: {}
    dummy_ui.UIState = type("UIState", (), {})
    dummy_ui.UIServer = type("UIServer", (), {})

    class DummyUIError(Exception):
        pass

    dummy_ui.UIStartupError = DummyUIError
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", dummy_ui)

    dummy_trading_runtime = types.ModuleType("solhunter_zero.runtime.trading_runtime")
    dummy_trading_runtime.TradingRuntime = type("TradingRuntime", (), {})
    monkeypatch.setitem(
        sys.modules, "solhunter_zero.runtime.trading_runtime", dummy_trading_runtime
    )

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )

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
async def test_start_invokes_metrics_aggregator(monkeypatch):
    dummy_ui = types.ModuleType("solhunter_zero.ui")
    dummy_ui.create_app = lambda *_, **__: types.SimpleNamespace()
    dummy_ui.start_websockets = lambda: {}
    dummy_ui.UIState = type("UIState", (), {})
    dummy_ui.UIServer = type("UIServer", (), {})

    class DummyUIError(Exception):
        pass

    dummy_ui.UIStartupError = DummyUIError
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", dummy_ui)

    dummy_trading_runtime = types.ModuleType("solhunter_zero.runtime.trading_runtime")
    dummy_trading_runtime.TradingRuntime = type("TradingRuntime", (), {})
    monkeypatch.setitem(
        sys.modules, "solhunter_zero.runtime.trading_runtime", dummy_trading_runtime
    )

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )

    class DummyBus:
        def subscribe(self, *_args, **_kwargs):
            return None

        def publish(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(runtime_orchestrator, "event_bus", DummyBus())

    async def fake_publish(self, *_args, **_kwargs):
        return None

    monkeypatch.setattr(
        runtime_orchestrator.RuntimeOrchestrator,
        "_publish_stage",
        fake_publish,
        raising=False,
    )

    async def fake_start_ui(self):
        return None

    async def fake_start_bus(self):
        return None

    async def fake_start_agents(self):
        return True

    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "start_ui", fake_start_ui)
    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "start_bus", fake_start_bus)
    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "start_agents", fake_start_agents)

    start_calls = 0

    def fake_metrics_start():
        nonlocal start_calls
        start_calls += 1

    monkeypatch.setattr(runtime_orchestrator.metrics_aggregator, "start", fake_metrics_start)

    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.metrics.decision_metrics",
        types.SimpleNamespace(DecisionMetrics=lambda: types.SimpleNamespace(start=lambda: None)),
    )
    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.risk_controller",
        types.SimpleNamespace(RiskController=lambda: types.SimpleNamespace(start=lambda: None)),
    )

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)

    await orchestrator.start()

    assert start_calls == 1


@pytest.mark.anyio("asyncio")
async def test_stop_all_idempotent_after_partial_start(monkeypatch):
    dummy_ui = types.ModuleType("solhunter_zero.ui")
    dummy_ui.create_app = lambda *_, **__: types.SimpleNamespace()
    dummy_ui.start_websockets = lambda: {}
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", dummy_ui)

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )

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


def test_main_returns_after_stop(monkeypatch):
    monkeypatch.setenv("NEW_RUNTIME", "1")
    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )

    started = False
    stopped = False

    original_stop_all = runtime_orchestrator.RuntimeOrchestrator.stop_all

    async def fake_start(self):
        nonlocal started
        started = True
        asyncio.create_task(self.stop_all())
        await asyncio.sleep(0)

    async def fake_stop_all(self):
        nonlocal stopped
        await original_stop_all(self)
        stopped = True

    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "start", fake_start)
    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "stop_all", fake_stop_all)

    runtime_orchestrator.main([])

    assert started is True
    assert stopped is True
