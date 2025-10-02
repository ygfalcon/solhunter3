import asyncio
import sys
import types
from dataclasses import dataclass

import pytest

from solhunter_zero import prices
from solhunter_zero.portfolio import Portfolio


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
