import importlib
import importlib.util
import subprocess
import sys
import types
from unittest.mock import Mock

import pytest
import solhunter_zero
from pathlib import Path

from solhunter_zero.runtime.orchestrator import RuntimeOrchestrator
from solhunter_zero.runtime.trading_runtime import DEPTH_PROCESS_SHUTDOWN_TIMEOUT


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_stop_all_terminates_depth_process():
    orch = RuntimeOrchestrator(run_http=False)

    proc = Mock()
    proc.poll.return_value = None
    proc.wait.side_effect = [
        subprocess.TimeoutExpired(cmd="depth_service", timeout=1),
        None,
    ]
    orch.handles.depth_proc = proc

    await orch.stop_all()

    proc.terminate.assert_called_once_with()
    proc.kill.assert_called_once_with()
    assert proc.wait.call_count == 2
    first_call = proc.wait.call_args_list[0]
    assert first_call.kwargs == {"timeout": DEPTH_PROCESS_SHUTDOWN_TIMEOUT}
    second_call = proc.wait.call_args_list[1]
    assert second_call.kwargs == {"timeout": DEPTH_PROCESS_SHUTDOWN_TIMEOUT}


@pytest.mark.anyio("asyncio")
async def test_orchestrator_stop_clears_token_discovery_handlers(monkeypatch, request):
    runtime_orchestrator = importlib.reload(importlib.import_module("solhunter_zero.runtime.orchestrator"))
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    event_bus_module = importlib.import_module("solhunter_zero.event_bus")
    event_bus_module.reset()

    async def fake_start_ui(self):
        return None

    async def fake_start_bus(self):
        return None

    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "start_ui", fake_start_ui, raising=False)
    monkeypatch.setattr(runtime_orchestrator.RuntimeOrchestrator, "start_bus", fake_start_bus, raising=False)

    async def fake_startup(
        config_path: str | None,
        *,
        offline: bool,
        dry_run: bool,
        testnet: bool = False,
        preloaded_config: dict | None = None,
    ):
        cfg = {"agents": ["dummy"], "loop_delay": 1}
        return cfg, cfg, None

    monkeypatch.setattr(runtime_orchestrator, "perform_startup_async", fake_startup)

    class DummyMemory:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start_writer(self) -> None:
            pass

    class DummyPortfolio:
        def __init__(self, *_: object, **__: object) -> None:
            self.balances: dict[str, float] = {}
            self.price_history: dict[str, list[float]] = {}

        def record_prices(self, updates: dict[str, float]) -> None:
            for token, price in updates.items():
                self.price_history.setdefault(token, []).append(float(price))

    class DummyAgentManager:
        agents = [types.SimpleNamespace(name="dummy")]
        evolve_interval = 1
        mutation_threshold = 0.0

        @classmethod
        def from_config(cls, *_: object, **__: object) -> "DummyAgentManager":
            return cls()

        @classmethod
        def from_default(cls):
            return None

        async def evaluate(self, token: str, portfolio: DummyPortfolio):
            return []

        async def evolve(self, threshold: float = 0.0):
            return None

        async def update_weights(self):
            return None

        def save_weights(self) -> None:
            pass

    class DummyStrategyManager:
        def __init__(self, *_: object, **__: object) -> None:
            pass

    class DummyTradeExecutor:
        def __init__(self, *_, **__):
            pass

        def start(self) -> None:
            pass

    class DummyDiscoveryAgent:
        async def discover_tokens(self, method: str | None = None, offline: bool = False):
            return []

    async def fake_init_rl_training(*_: object, **__: object):
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

    monkeypatch.setattr(runtime_orchestrator, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_orchestrator, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_orchestrator, "TradingState", lambda: object())
    monkeypatch.setattr(runtime_orchestrator, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(runtime_orchestrator, "StrategyManager", DummyStrategyManager)
    monkeypatch.setattr(runtime_orchestrator.metrics_aggregator, "start", lambda: None)
    monkeypatch.setattr(runtime_orchestrator.discovery_state, "current_method", lambda **_: "dummy")

    discovery_module = importlib.import_module("solhunter_zero.agents.discovery")
    monkeypatch.setattr(discovery_module, "DiscoveryAgent", DummyDiscoveryAgent)

    loop_module = importlib.import_module("solhunter_zero.loop")
    monkeypatch.setattr(loop_module, "_init_rl_training", fake_init_rl_training)

    exec_service_original = sys.modules.get("solhunter_zero.exec_service")
    exec_service_module = types.SimpleNamespace(TradeExecutor=DummyTradeExecutor)
    monkeypatch.setitem(sys.modules, "solhunter_zero.exec_service", exec_service_module)
    request.addfinalizer(lambda: _restore_module("solhunter_zero.exec_service", exec_service_original))

    runtime_module_original = sys.modules.get("solhunter_zero.agents.runtime")
    runtime_impl_path = Path(solhunter_zero.__file__).resolve().parent / "agents" / "runtime.py"
    runtime_module_name = "solhunter_zero.agents._event_runtime_impl_for_test"
    runtime_spec = importlib.util.spec_from_file_location(runtime_module_name, runtime_impl_path)
    assert runtime_spec and runtime_spec.loader
    runtime_module = importlib.util.module_from_spec(runtime_spec)
    sys.modules[runtime_module_name] = runtime_module
    sys.modules["solhunter_zero.agents.runtime"] = runtime_module
    runtime_spec.loader.exec_module(runtime_module)
    request.addfinalizer(lambda: _restore_module("solhunter_zero.agents.runtime", runtime_module_original))
    request.addfinalizer(lambda: sys.modules.pop(runtime_module_name, None))

    decision_metrics_original = sys.modules.get("solhunter_zero.metrics.decision_metrics")
    risk_module_original = sys.modules.get("solhunter_zero.risk_controller")

    decision_metrics_module = types.SimpleNamespace(DecisionMetrics=DummyDecisionMetrics)
    risk_module = types.SimpleNamespace(RiskController=DummyRiskController)
    monkeypatch.setitem(sys.modules, "solhunter_zero.metrics.decision_metrics", decision_metrics_module)
    monkeypatch.setitem(sys.modules, "solhunter_zero.risk_controller", risk_module)

    def _restore_module(name: str, original: object | None) -> None:
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original

    request.addfinalizer(lambda: _restore_module("solhunter_zero.metrics.decision_metrics", decision_metrics_original))
    request.addfinalizer(lambda: _restore_module("solhunter_zero.risk_controller", risk_module_original))

    async def run_cycle() -> None:
        orch = runtime_orchestrator.RuntimeOrchestrator(run_http=False)
        await orch.start()
        await orch.stop_all()

    await run_cycle()
    await run_cycle()

    handlers = list(event_bus_module._subscribers.get("token_discovered", []))
    assert len(handlers) <= 1
