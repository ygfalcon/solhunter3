import asyncio
import logging
import sys
import time
import types
from dataclasses import dataclass

import pytest

from solhunter_zero import prices
from solhunter_zero.portfolio import Portfolio, Position


def _install_torch_stub(monkeypatch):
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
    sample_ticks_module = types.ModuleType("solhunter_zero.datasets.sample_ticks")
    sample_ticks_module.load_sample_ticks = lambda *args, **kwargs: []  # type: ignore[attr-defined]
    sample_ticks_module.DEFAULT_PATH = ""  # type: ignore[attr-defined]
    datasets_pkg.sample_ticks = sample_ticks_module  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "solhunter_zero.datasets", datasets_pkg)
    monkeypatch.setitem(
        sys.modules, "solhunter_zero.datasets.sample_ticks", sample_ticks_module
    )


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


def test_evaluation_hydrates_action_prices(monkeypatch):
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

    from solhunter_zero.swarm_pipeline import DiscoveryStage, SwarmPipeline

    token = "Tok"
    raw_action = {
        "token": token,
        "side": "buy",
        "amount": 2,
        "price": 0,
        "agent": "tester",
        "metadata": {"needs_price": True, "needs_price_agents": ["swarm_zero"]},
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

    async def fake_resolve(tok, pf):
        captured["token"] = tok
        captured["portfolio"] = pf
        return 1.23, {"history_price": 1.23, "source": "history"}

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.resolve_price", fake_resolve)
    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *args, **kwargs: None)

    async def _run() -> None:
        pipeline = SwarmPipeline(agent_manager, portfolio, dry_run=True)

        discovery = DiscoveryStage(tokens=[token], discovered=[token], limit=1)
        discovery.scores = {token: 1.0}

        evaluation = await pipeline._run_evaluation(discovery)
        assert captured["token"] == token
        assert captured["portfolio"] is portfolio
        assert evaluation.records
        record = evaluation.records[0]
        assert record.actions
        action = record.actions[0]
        assert action.price == pytest.approx(1.23)
        assert action.metadata["price_context"]["source"] == "history"
        assert action.metadata["price_context"].get("hydrated_by") == "pipeline"
        assert action.metadata["price_context"].get("hydrated_agents") == [
            "swarm_zero"
        ]
        assert action.metadata.get("price_fallback") == "history"
        nested_meta = action.metadata.get("metadata")
        if isinstance(nested_meta, dict):
            assert "needs_price" not in nested_meta
            assert "needs_price_agents" not in nested_meta
        assert "needs_price" not in action.metadata
        assert "needs_price_agents" not in action.metadata
        cached = prices.get_cached_price(token)
        assert cached == pytest.approx(1.23)

    asyncio.run(_run())


def test_swarm_pipeline_marks_missing_price(monkeypatch):
    from solhunter_zero.swarm_pipeline import DiscoveryStage, SwarmPipeline

    token = "Tok"
    raw_action = {
        "token": token,
        "side": "buy",
        "amount": 1,
        "price": 0,
        "agent": "tester",
    }

    agent_manager = _DummyAgentManager([raw_action])
    portfolio = Portfolio(path=None)

    async def fake_resolve(tok, pf):
        return 0.0, {"fetch_error": "boom"}

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.resolve_price", fake_resolve)
    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *args, **kwargs: None)

    async def _run() -> None:
        pipeline = SwarmPipeline(agent_manager, portfolio, dry_run=True)
        discovery = DiscoveryStage(tokens=[token], discovered=[token], limit=1)
        discovery.scores = {token: 1.0}

        evaluation = await pipeline._run_evaluation(discovery)
        record = evaluation.records[0]
        action = record.actions[0]
        assert action.price == 0.0
        assert action.metadata.get("price_missing") is True
        assert action.metadata["price_context"]["fetch_error"] == "boom"

    asyncio.run(_run())


def test_swarm_pipeline_uses_entry_price_fallback(monkeypatch):
    from solhunter_zero.swarm_pipeline import DiscoveryStage, SwarmPipeline

    token = "Tok"
    raw_action = {
        "token": token,
        "side": "sell",
        "amount": 1,
        "price": 0,
        "agent": "tester",
    }

    agent_manager = _DummyAgentManager([raw_action])
    portfolio = Portfolio(path=None)
    portfolio.balances[token] = Position(token, 1.0, 2.5, 2.5)

    async def fake_resolve(tok, pf):
        return 0.0, {}

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.resolve_price", fake_resolve)
    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *args, **kwargs: None)

    async def _run() -> None:
        pipeline = SwarmPipeline(agent_manager, portfolio, dry_run=True)
        discovery = DiscoveryStage(tokens=[token], discovered=[token], limit=1)
        discovery.scores = {token: 1.0}

        evaluation = await pipeline._run_evaluation(discovery)
        record = evaluation.records[0]
        action = record.actions[0]
        assert action.price == pytest.approx(2.5)
        assert action.metadata.get("price_fallback") == "entry_price"
        assert action.metadata["price_context"]["entry_price"] == pytest.approx(2.5)

    asyncio.run(_run())


def test_discovery_uses_cached_tokens(monkeypatch):
    from solhunter_zero.swarm_pipeline import SwarmPipeline

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *a, **k: None)
    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline._score_token",
        lambda token, pf: float(len(token)),
    )

    pipeline = SwarmPipeline(_DummyAgentManager([]), Portfolio(path=None), dry_run=True)
    pipeline.discovery_cache_limit = 8
    pipeline.discovery_cache_ttl = 30.0
    pipeline._discovery_cache_tokens = ["TokA", "TokB"]
    pipeline._discovery_cache_scores = {"TokA": 4.0, "TokB": 3.0}
    pipeline._discovery_cache_expiry = time.time() + 10.0

    class _SentinelAgent:
        def __init__(self) -> None:
            self.called = False

        async def discover_tokens(self, **kwargs):  # pragma: no cover - should not run
            self.called = True
            return ["TokC"]

    sentinel = _SentinelAgent()
    pipeline._discovery_agent = sentinel

    stage = asyncio.run(pipeline._run_discovery())

    assert not sentinel.called
    assert set(stage.tokens) >= {"TokA", "TokB"}
    assert stage.fallback_used is False


def test_discovery_refreshes_cache(monkeypatch):
    from solhunter_zero.swarm_pipeline import SwarmPipeline

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *a, **k: None)
    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline._score_token",
        lambda token, pf: 10.0 if token == "TokC" else 1.0,
    )

    pipeline = SwarmPipeline(_DummyAgentManager([]), Portfolio(path=None), dry_run=True)
    pipeline.discovery_cache_limit = 6
    pipeline.discovery_cache_ttl = 20.0
    pipeline._discovery_cache_tokens = ["TokA"]
    pipeline._discovery_cache_scores = {"TokA": 1.0}
    pipeline._discovery_cache_expiry = time.time() - 5.0

    call_counter = {"count": 0}

    class _StubAgent:
        async def discover_tokens(self, **kwargs):
            call_counter["count"] += 1
            return ["TokC", "TokB"]

    pipeline._discovery_agent = _StubAgent()

    stage = asyncio.run(pipeline._run_discovery())

    assert call_counter["count"] == 1
    assert "TokC" in stage.tokens
    assert pipeline._discovery_cache_tokens[0] == "TokC"
    assert pipeline._discovery_cache_expiry > time.time()


def test_discovery_emits_token_event(monkeypatch):
    from solhunter_zero.swarm_pipeline import SwarmPipeline

    events: list[list[str]] = []

    def fake_publish(topic, payload, *args, **kwargs):
        if topic == "token_discovered":
            events.append(list(payload))

    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", fake_publish)
    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline._score_token",
        lambda token, pf: {"New": 5.0, "Alt": 4.0, "Old": 3.0}.get(token, 1.0),
    )

    pipeline = SwarmPipeline(_DummyAgentManager([]), Portfolio(path=None), dry_run=True)
    pipeline.discovery_cache_limit = 8
    pipeline.discovery_cache_ttl = 0.0
    pipeline._discovery_cache_tokens = ["Old"]
    pipeline._discovery_cache_scores = {"Old": 3.0}
    pipeline._discovery_cache_expiry = 0.0

    class _StubAgent:
        async def discover_tokens(self, **kwargs):
            return ["New", "Alt"]

    pipeline._discovery_agent = _StubAgent()

    stage = asyncio.run(pipeline._run_discovery())

    assert stage.tokens == ["New", "Alt", "Old"]
    assert events == [stage.tokens]


def test_execution_skips_missing_price(monkeypatch, caplog):
    _install_torch_stub(monkeypatch)
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


def test_simulation_biases_action_limit_by_record_score(monkeypatch):
    monkeypatch.setenv("SWARM_MAX_ACTIONS", "2")

    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        EvaluationStage,
        SimulationStage,
        SwarmAction,
        SwarmPipeline,
    )

    class _StubAgentManager:
        skip_simulation = False

        def __init__(self) -> None:
            self.executor = types.SimpleNamespace(add_executor=lambda *a, **k: None)

        def consume_swarm(self, token, swarm):  # pragma: no cover - interface stub
            return None

    pipeline = SwarmPipeline(_StubAgentManager(), Portfolio(path=None), dry_run=True)

    low_actions = [
        SwarmAction(
            token="LOW",
            side="buy",
            amount=1.0,
            price=1.0,
            agent="stub",
            expected_roi=0.5,
            success_prob=0.9,
        )
        for _ in range(3)
    ]
    high_actions = [
        SwarmAction(
            token="HIGH",
            side="buy",
            amount=1.0,
            price=1.0,
            agent="stub",
            expected_roi=0.5,
            success_prob=0.9,
        )
        for _ in range(3)
    ]

    evaluation = EvaluationStage(
        records=[
            EvaluationRecord(
                token="LOW",
                score=0.0,
                latency=0.0,
                actions=low_actions,
                context=None,
            ),
            EvaluationRecord(
                token="HIGH",
                score=3.0,
                latency=0.0,
                actions=high_actions,
                context=None,
            ),
        ]
    )

    simulation = asyncio.run(pipeline._run_simulation(evaluation))

    results = {rec.token: rec for rec in simulation.records}
    assert len(results["HIGH"].actions) == 2
    assert len(results["LOW"].actions) == 1
    assert simulation.rejected["HIGH"] == 1
    assert simulation.rejected["LOW"] == 2
    for rec in simulation.records:
        for act in rec.actions:
            assert act.metadata["evaluation_score"] == rec.score


def test_execution_carries_evaluation_score(monkeypatch):
    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        SimulationStage,
        SwarmAction,
        SwarmPipeline,
    )

    class _StubAgentManager:
        skip_simulation = False

        def __init__(self) -> None:
            self.executor = types.SimpleNamespace(add_executor=lambda *a, **k: None)

        def consume_swarm(self, token, swarm):  # pragma: no cover - interface stub
            return None

    class _CapturingDispatcher:
        last_instance = None

        def __init__(self, *args, **kwargs):
            self.submitted: list = []
            _CapturingDispatcher.last_instance = self

        async def submit(self, lane, request):
            self.submitted.append(request)
            fut = asyncio.get_running_loop().create_future()
            fut.set_result({"status": "ok"})
            return fut

        def lane_snapshot(self):
            return {}

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline.ExecutionDispatcher", _CapturingDispatcher
    )

    pipeline = SwarmPipeline(_StubAgentManager(), Portfolio(path=None), dry_run=True)

    action = SwarmAction(
        token="Tok",
        side="buy",
        amount=1.0,
        price=1.0,
        agent="stub",
        expected_roi=0.5,
        success_prob=0.9,
    )
    record = EvaluationRecord(
        token="Tok",
        score=2.5,
        latency=0.0,
        actions=[action],
        context=None,
    )

    simulation = SimulationStage(records=[record])
    execution = asyncio.run(pipeline._run_execution(simulation))

    assert execution.executed
    executed = execution.executed[0].result
    assert executed["evaluation_score"] == pytest.approx(2.5)

    dispatcher = _CapturingDispatcher.last_instance
    assert dispatcher is not None
    assert dispatcher.submitted
    token_payload = dispatcher.submitted[0].token_result
    assert token_payload["score"] == pytest.approx(2.5)


def test_execution_skips_when_keypair_missing(monkeypatch, caplog):
    _install_torch_stub(monkeypatch)
    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        SimulationStage,
        SwarmAction,
        SwarmPipeline,
    )

    class _StubExecutor:
        keypair = None
        lane_budgets = {"default": 100.0}

    class _StubAgentManager:
        skip_simulation = False
        depth_service = False

        def __init__(self) -> None:
            self.executor = _StubExecutor()
            self.keypair = None
            self.keypair_path = None

        def consume_swarm(self, token, swarm):  # pragma: no cover - interface stub
            return None

    class _CapturingDispatcher:
        def __init__(self, *args, **kwargs) -> None:
            self.submitted: list = []

        async def submit(self, lane, request):  # pragma: no cover - defensive
            self.submitted.append(request)
            fut = asyncio.get_running_loop().create_future()
            fut.set_result({"status": "ok"})
            return fut

        def lane_snapshot(self):
            return {}

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline.ExecutionDispatcher", _CapturingDispatcher
    )

    pipeline = SwarmPipeline(_StubAgentManager(), Portfolio(path=None), dry_run=True)
    action = SwarmAction(
        token="Tok",
        side="buy",
        amount=1.0,
        price=1.0,
        agent="stub",
        metadata={"requires_keypair": True},
    )
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
    assert execution.skipped == {"missing_keypair": 1}
    assert "execution:missing_keypair" in execution.errors
    assert record.errors == ["missing_keypair"]
    assert "missing keypair" in caplog.text


def test_execution_skips_when_lane_budget_missing(monkeypatch, caplog):
    _install_torch_stub(monkeypatch)
    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        SimulationStage,
        SwarmAction,
        SwarmPipeline,
    )

    class _StubExecutor:
        keypair = object()
        lane_budgets = {}

    class _StubAgentManager:
        skip_simulation = False
        depth_service = False

        def __init__(self) -> None:
            self.executor = _StubExecutor()

        def consume_swarm(self, token, swarm):  # pragma: no cover - interface stub
            return None

    class _CapturingDispatcher:
        def __init__(self, *args, **kwargs) -> None:
            self.submitted: list = []

        async def submit(self, lane, request):  # pragma: no cover - defensive
            self.submitted.append(request)
            fut = asyncio.get_running_loop().create_future()
            fut.set_result({"status": "ok"})
            return fut

        def lane_snapshot(self):
            return {}

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline.ExecutionDispatcher", _CapturingDispatcher
    )

    pipeline = SwarmPipeline(_StubAgentManager(), Portfolio(path=None), dry_run=True)
    action = SwarmAction(
        token="Tok",
        side="buy",
        amount=1.0,
        price=1.0,
        agent="stub",
        metadata={"lane_budget": 10.0},
    )
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
    assert execution.skipped == {"missing_lane_budget": 1}
    assert "execution:missing_lane_budget" in execution.errors
    assert record.errors == ["missing_lane_budget"]
    assert "missing lane budget" in caplog.text


def test_execution_skips_when_executor_missing(monkeypatch, caplog):
    _install_torch_stub(monkeypatch)
    from solhunter_zero.swarm_pipeline import (
        EvaluationRecord,
        SimulationStage,
        SwarmAction,
        SwarmPipeline,
    )

    class _StubExecutor:
        keypair = object()
        lane_budgets = {"default": 50.0}
        available_executors = set()
        _executors = {}

    class _StubAgentManager:
        skip_simulation = False
        depth_service = False
        available_executors: set[str] = set()

        def __init__(self) -> None:
            self.executor = _StubExecutor()

        def consume_swarm(self, token, swarm):  # pragma: no cover - interface stub
            return None

    class _CapturingDispatcher:
        def __init__(self, *args, **kwargs) -> None:
            self.submitted: list = []

        async def submit(self, lane, request):  # pragma: no cover - defensive
            self.submitted.append(request)
            fut = asyncio.get_running_loop().create_future()
            fut.set_result({"status": "ok"})
            return fut

        def lane_snapshot(self):
            return {}

        async def close(self) -> None:
            pass

    monkeypatch.setattr(
        "solhunter_zero.swarm_pipeline.ExecutionDispatcher", _CapturingDispatcher
    )

    pipeline = SwarmPipeline(_StubAgentManager(), Portfolio(path=None), dry_run=True)
    action = SwarmAction(
        token="Tok",
        side="buy",
        amount=1.0,
        price=1.0,
        agent="stub",
        metadata={"requires_executor": "mev"},
    )
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
    assert execution.skipped == {"missing_executor": 1}
    assert "execution:missing_executor" in execution.errors
    assert record.errors == ["missing_executor"]
    assert "missing executors" in caplog.text
