import asyncio
import logging
import sys
import types
from dataclasses import dataclass

import pytest

from solhunter_zero import prices
from solhunter_zero.portfolio import Portfolio


class _DummyExecutor:
    async def execute(self, action):
        return {
            "fills": [
                {"price": 1.5, "filled_amount": 2.0},
                {"price": 1.6, "filled_amount": 1.0},
            ]
        }


class _DummyMemoryAgent:
    def __init__(self):
        self.logged: list[dict] = []

    async def log(self, action, *, skip_db: bool = False):
        self.logged.append(dict(action))


@dataclass
class _DummyContext:
    actions: list[dict]
    swarm: None = None


class _DummyAgentManager:
    skip_simulation = False

    def __init__(self, actions: list[dict]):
        self._actions = actions

    async def evaluate_with_swarm(self, token, portfolio):
        await asyncio.sleep(0)
        return _DummyContext(actions=list(self._actions))


def _install_swarm_stubs(monkeypatch):
    class _StubModuleType(types.ModuleType):
        def __getattr__(self, name):  # pragma: no cover - defensive stub
            def _dummy(*args, **kwargs):
                return None

            return _dummy

    class _StubTorchModule(_StubModuleType):
        def __getattr__(self, name):  # pragma: no cover - defensive stub
            return super().__getattr__(name)

    torch_module = _StubTorchModule("torch")
    torch_module.__path__ = []  # type: ignore[attr-defined]
    nn_module = _StubModuleType("torch.nn")
    nn_module.__path__ = []  # type: ignore[attr-defined]

    class _BaseModule:
        def __init__(self, *args, **kwargs) -> None:
            pass

    nn_module.Module = _BaseModule  # type: ignore[attr-defined]
    nn_module.ModuleList = list  # type: ignore[attr-defined]
    nn_module.Sequential = lambda *args, **kwargs: list(args)  # type: ignore[attr-defined]

    functional_module = _StubModuleType("torch.nn.functional")
    optim_module = _StubModuleType("torch.optim")

    class _DummyOptimizer:
        def step(self) -> None:
            pass

        def zero_grad(self) -> None:
            pass

    optim_module.Adam = lambda *args, **kwargs: _DummyOptimizer()  # type: ignore[attr-defined]

    utils_module = types.ModuleType("torch.utils")
    utils_module.__path__ = []  # type: ignore[attr-defined]
    data_module = types.ModuleType("torch.utils.data")

    class _DummyDataset:
        def __iter__(self):
            return iter(())

    class _DummyDataLoader(list):
        pass

    data_module.Dataset = _DummyDataset  # type: ignore[attr-defined]
    data_module.DataLoader = _DummyDataLoader  # type: ignore[attr-defined]
    utils_module.data = data_module  # type: ignore[attr-defined]

    torch_module.nn = nn_module
    torch_module.optim = optim_module
    torch_module.utils = utils_module

    monkeypatch.setitem(sys.modules, "torch", torch_module)
    monkeypatch.setitem(sys.modules, "torch.nn", nn_module)
    monkeypatch.setitem(sys.modules, "torch.nn.functional", functional_module)
    monkeypatch.setitem(sys.modules, "torch.optim", optim_module)
    monkeypatch.setitem(sys.modules, "torch.utils", utils_module)
    monkeypatch.setitem(sys.modules, "torch.utils.data", data_module)

    datasets_pkg = types.ModuleType("solhunter_zero.datasets")
    datasets_pkg.__path__ = []  # type: ignore[attr-defined]

    sample_ticks_module = types.ModuleType("solhunter_zero.datasets.sample_ticks")

    def _load_sample_ticks(*args, **kwargs):
        return []

    sample_ticks_module.load_sample_ticks = _load_sample_ticks  # type: ignore[attr-defined]
    sample_ticks_module.DEFAULT_PATH = ""  # type: ignore[attr-defined]
    datasets_pkg.sample_ticks = sample_ticks_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "solhunter_zero.datasets", datasets_pkg)
    monkeypatch.setitem(
        sys.modules, "solhunter_zero.datasets.sample_ticks", sample_ticks_module
    )


def test_evaluation_hydrates_action_prices(monkeypatch):
    _install_swarm_stubs(monkeypatch)

    from solhunter_zero.swarm_pipeline import DiscoveryStage, SwarmPipeline

    token = "Tok"
    raw_action = {
        "token": token,
        "side": "buy",
        "amount": 2,
        "price": 0,
        "agent": "tester",
    }

    agent_manager = _DummyAgentManager([raw_action])
    portfolio = Portfolio(path=None)

    # Reset global price cache for isolation
    monkeypatch.setattr(
        prices,
        "PRICE_CACHE",
        prices.TTLCache(maxsize=16, ttl=prices.PRICE_CACHE_TTL),
        raising=False,
    )

    captured = {}

    async def fake_fetch(tokens):
        captured["tokens"] = list(tokens)
        return {token: 1.23}

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.fetch_token_prices_async", fake_fetch)
    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *args, **kwargs: None)

    async def _run() -> None:
        pipeline = SwarmPipeline(agent_manager, portfolio, dry_run=True)

        discovery = DiscoveryStage(tokens=[token], discovered=[token], limit=1)
        discovery.scores = {token: 1.0}

        evaluation = await pipeline._run_evaluation(discovery)
        assert captured["tokens"] == [token]
        assert evaluation.records
        record = evaluation.records[0]
        assert record.actions
        action = record.actions[0]
        assert action.price == pytest.approx(1.23)
        cached = prices.get_cached_price(token)
        assert cached == pytest.approx(1.23)

    asyncio.run(_run())


def test_execution_skips_missing_price(monkeypatch, caplog):
    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        SimulationStage,
        SwarmAction,
        SwarmPipeline,
    )

    class _StubAgentManager:
        skip_simulation = False
        depth_service = False

        def __init__(self) -> None:
            self.executor = types.SimpleNamespace()

        def consume_swarm(self, token, swarm):  # pragma: no cover - interface stub
            return None

    call_flag = {"called": False}

    class _StubDispatcher:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def submit(self, lane, request):  # pragma: no cover - defensive
            call_flag["called"] = True
            raise AssertionError("submit should not be called for missing prices")

        def lane_snapshot(self):
            return {}

        async def close(self) -> None:
            pass

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.ExecutionDispatcher", _StubDispatcher)

    pipeline = SwarmPipeline(_StubAgentManager(), Portfolio(path=None), dry_run=True)
    action = SwarmAction(token="Tok", side="buy", amount=1.0, price=0.0, agent="tester")
    record = EvaluationRecord(
        token="Tok",
        score=1.0,
        latency=0.0,
        actions=[action],
        context=None,
    )
    simulation = SimulationStage(records=[record])

    caplog.set_level(logging.WARNING, logger="solhunter_zero.swarm_pipeline")

    execution = asyncio.run(pipeline._run_execution(simulation))

    assert execution.executed == []
    assert execution.skipped == {"missing_price": 1}
    assert "execution:missing_price" in execution.errors
    assert record.errors == ["missing_price"]
    assert not call_flag["called"]
    assert "missing price" in caplog.text


def test_dispatcher_enriches_realized_metrics(monkeypatch):
    torch_mod = types.ModuleType("torch")
    torch_mod.Tensor = type("_Tensor", (), {})
    torch_mod.device = type("_Device", (), {"__init__": lambda self, *a, **k: None})
    torch_mod.nn = types.SimpleNamespace(
        Module=object,
        ModuleList=list,
        Sequential=lambda *args, **kwargs: list(args),
        functional=types.SimpleNamespace(),
    )
    torch_mod.optim = types.SimpleNamespace(Adam=lambda *args, **kwargs: None)
    torch_mod.utils = types.SimpleNamespace(
        data=types.SimpleNamespace(Dataset=object, DataLoader=list)
    )
    monkeypatch.setitem(sys.modules, "torch", torch_mod)
    monkeypatch.setitem(sys.modules, "torch.nn", torch_mod.nn)
    monkeypatch.setitem(sys.modules, "torch.nn.functional", torch_mod.nn.functional)
    monkeypatch.setitem(sys.modules, "torch.optim", torch_mod.optim)
    monkeypatch.setitem(sys.modules, "torch.utils", torch_mod.utils)
    monkeypatch.setitem(sys.modules, "torch.utils.data", torch_mod.utils.data)

    datasets_pkg = types.ModuleType("solhunter_zero.datasets")
    datasets_pkg.__path__ = []  # type: ignore[attr-defined]
    sample_ticks = types.ModuleType("solhunter_zero.datasets.sample_ticks")
    sample_ticks.load_sample_ticks = lambda *args, **kwargs: []  # type: ignore[attr-defined]
    sample_ticks.DEFAULT_PATH = ""  # type: ignore[attr-defined]
    datasets_pkg.sample_ticks = sample_ticks  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "solhunter_zero.datasets", datasets_pkg)
    monkeypatch.setitem(sys.modules, "solhunter_zero.datasets.sample_ticks", sample_ticks)

    from solhunter_zero.swarm_pipeline import ExecutionDispatcher, ExecutionRequest

    class _AgentManager:
        def __init__(self):
            self.executor = _DummyExecutor()
            self.memory_agent = _DummyMemoryAgent()

    manager = _AgentManager()

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *args, **kwargs: None)

    dispatcher = ExecutionDispatcher(manager)
    record = {"token": "Tok", "side": "buy", "amount": 3.0, "price": 1.4}
    token_payload = {"token": "Tok", "actions": []}
    request = ExecutionRequest(
        "Tok",
        {"token": "Tok", "side": "buy", "amount": 3.0, "price": 1.4},
        record,
        token_payload,
        [],
        [],
    )

    async def _run():
        return await dispatcher._execute_request(request)

    updated = asyncio.run(_run())
    assert updated["realized_amount"] == pytest.approx(3.0)
    assert updated["realized_notional"] == pytest.approx(4.6)
    assert updated["realized_price"] == pytest.approx(4.6 / 3.0)
    assert manager.memory_agent.logged
    logged = manager.memory_agent.logged[0]
    assert logged["realized_price"] == pytest.approx(4.6 / 3.0)
    assert logged["realized_notional"] == pytest.approx(4.6)


def test_simulation_reuses_cached_results(monkeypatch):
    _install_swarm_stubs(monkeypatch)
    from solhunter_zero.simulation import SimulationResult
    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        EvaluationStage,
        SwarmAction,
        SwarmPipeline,
    )

    calls: dict[str, int] = {"count": 0}

    async def fake_simulations(token: str, *, count: int) -> list[SimulationResult]:
        calls["count"] += 1
        return [SimulationResult(success_prob=0.4, expected_roi=0.5)]

    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline.run_simulations_async",
        fake_simulations,
    )
    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *_, **__: None)

    portfolio = Portfolio(path=None)
    agent_manager = types.SimpleNamespace(skip_simulation=False)
    pipeline = SwarmPipeline(agent_manager, portfolio, dry_run=True)

    async def _execute() -> None:
        base_action = SwarmAction(
            token="Tok",
            side="buy",
            amount=1.0,
            price=1.0,
            agent="stub",
            expected_roi=0.0,
            success_prob=0.0,
        )
        stage = await pipeline._run_simulation(
            EvaluationStage(
                records=[
                    EvaluationRecord(
                        token="Tok",
                        score=1.0,
                        latency=0.0,
                        actions=[base_action],
                        context=None,
                    )
                ]
            )
        )

        assert calls["count"] == 1
        first_action = stage.records[0].actions[0]
        assert first_action.metadata["simulation_avg_roi"] == pytest.approx(0.5)
        assert first_action.metadata["simulation_avg_success"] == pytest.approx(0.4)
        assert "cache_derived" not in first_action.metadata

        pipeline._skip_simulation = True
        cached_action = SwarmAction(
            token="Tok",
            side="buy",
            amount=1.0,
            price=1.0,
            agent="stub",
            expected_roi=0.0,
            success_prob=0.0,
        )
        cached_stage = await pipeline._run_simulation(
            EvaluationStage(
                records=[
                    EvaluationRecord(
                        token="Tok",
                        score=1.0,
                        latency=0.0,
                        actions=[cached_action],
                        context=None,
                    )
                ]
            )
        )

        assert calls["count"] == 1
        cached_result = cached_stage.records[0].actions[0]
        assert cached_result.expected_roi == pytest.approx(0.5)
        assert cached_result.success_prob == pytest.approx(0.4)
        assert cached_result.metadata["simulation_avg_roi"] == pytest.approx(0.5)
        assert cached_result.metadata["simulation_avg_success"] == pytest.approx(0.4)
        assert cached_result.metadata["cache_derived"] is True

        pipeline._skip_simulation = False
        warmed_action = SwarmAction(
            token="Tok",
            side="buy",
            amount=1.0,
            price=1.0,
            agent="stub",
            expected_roi=0.5,
            success_prob=0.4,
        )
        warmed_stage = await pipeline._run_simulation(
            EvaluationStage(
                records=[
                    EvaluationRecord(
                        token="Tok",
                        score=1.0,
                        latency=0.0,
                        actions=[warmed_action],
                        context=None,
                    )
                ]
            )
        )

        assert calls["count"] == 1
        warmed_result = warmed_stage.records[0].actions[0]
        assert warmed_result.metadata["simulation_avg_roi"] == pytest.approx(0.5)
        assert warmed_result.metadata["simulation_avg_success"] == pytest.approx(0.4)
        assert warmed_result.metadata["cache_derived"] is True

    asyncio.run(_execute())
