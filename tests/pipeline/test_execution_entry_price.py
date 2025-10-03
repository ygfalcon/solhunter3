import asyncio
import sys
import types

import pytest


def _install_torch_stub() -> None:
    torch_mod = types.ModuleType("torch")
    torch_mod.Tensor = type("_Tensor", (), {})
    torch_mod.device = type("_Device", (), {"__init__": lambda self, *a, **k: None})
    nn_module = types.ModuleType("torch.nn")
    nn_module.Module = object
    nn_module.ModuleList = list
    nn_module.Sequential = lambda *args, **kwargs: list(args)
    functional_module = types.ModuleType("torch.nn.functional")
    optim_module = types.ModuleType("torch.optim")
    optim_module.Adam = lambda *args, **kwargs: None
    utils_module = types.ModuleType("torch.utils")
    data_module = types.ModuleType("torch.utils.data")
    data_module.Dataset = object
    data_module.DataLoader = list
    torch_mod.nn = nn_module
    torch_mod.optim = optim_module
    torch_mod.utils = utils_module
    nn_module.functional = functional_module
    utils_module.data = data_module
    torch_mod.__path__ = []  # type: ignore[attr-defined]
    nn_module.__path__ = []  # type: ignore[attr-defined]
    utils_module.__path__ = []  # type: ignore[attr-defined]
    data_module.__path__ = []  # type: ignore[attr-defined]
    sys.modules.setdefault("torch", torch_mod)
    sys.modules.setdefault("torch.nn", nn_module)
    sys.modules.setdefault("torch.nn.functional", functional_module)
    sys.modules.setdefault("torch.optim", optim_module)
    sys.modules.setdefault("torch.utils", utils_module)
    sys.modules.setdefault("torch.utils.data", data_module)

    datasets_pkg = types.ModuleType("solhunter_zero.datasets")
    datasets_pkg.__path__ = []  # type: ignore[attr-defined]
    sample_ticks_module = types.ModuleType("solhunter_zero.datasets.sample_ticks")
    sample_ticks_module.load_sample_ticks = lambda *args, **kwargs: []  # type: ignore[attr-defined]
    sample_ticks_module.DEFAULT_PATH = ""  # type: ignore[attr-defined]
    datasets_pkg.sample_ticks = sample_ticks_module  # type: ignore[attr-defined]
    sys.modules.setdefault("solhunter_zero.datasets", datasets_pkg)
    sys.modules.setdefault("solhunter_zero.datasets.sample_ticks", sample_ticks_module)


_install_torch_stub()

from solhunter_zero.portfolio import Portfolio, Position
from solhunter_zero.swarm_pipeline import (
    SwarmAction,
    SwarmPipeline,
    EvaluationRecord,
    SimulationStage,
)


class _DummyExecutor:
    async def execute(self, action):
        return {"fills": [{"price": 1.6, "filled_amount": 1.0}]}


class _DummyAgentManager:
    depth_service = False
    skip_simulation = False

    def __init__(self):
        self.executor = _DummyExecutor()


def test_run_execution_attaches_entry_price(monkeypatch):
    monkeypatch.setattr("solhunter_zero.swarm_pipeline.publish", lambda *a, **k: None)

    agent_manager = _DummyAgentManager()
    portfolio = Portfolio(path=None)
    portfolio.balances["TOK"] = Position("TOK", amount=2.0, entry_price=1.5, high_price=1.5)

    action = SwarmAction(token="TOK", side="sell", amount=1.0, price=1.7, agent="tester")
    record = EvaluationRecord(
        token="TOK",
        score=1.0,
        latency=0.0,
        actions=[action],
        context=None,
    )
    simulation = SimulationStage(records=[record])

    pipeline = SwarmPipeline(agent_manager, portfolio, dry_run=False)

    execution = asyncio.run(pipeline._run_execution(simulation))

    assert execution.executed, "execution stage should include at least one record"
    result = execution.executed[0].result
    assert result["entry_price"] == pytest.approx(1.5)
    expected_roi = (1.6 - 1.5) / 1.5
    assert result["realized_roi"] == pytest.approx(expected_roi)
