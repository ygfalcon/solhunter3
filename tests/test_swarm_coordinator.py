import asyncio
import sys
import types

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

sys.modules.setdefault("torch", torch_mod)
sys.modules.setdefault("torch.nn", torch_mod.nn)
sys.modules.setdefault("torch.nn.functional", torch_mod.nn.functional)
sys.modules.setdefault("torch.optim", torch_mod.optim)
sys.modules.setdefault("torch.utils", torch_mod.utils)
sys.modules.setdefault("torch.utils.data", torch_mod.utils.data)

from solhunter_zero.swarm_coordinator import SwarmCoordinator
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory


class DummyAgent:
    def __init__(self, name):
        self.name = name

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return []


def test_swarm_coordinator_adapts_weights():
    mem = Memory("sqlite:///:memory:")
    mem_agent = MemoryAgent(mem)

    async def _run() -> None:
        await mem.log_trade(token="tok", direction="buy", amount=1, price=2, reason="a1")
        await mem.log_trade(token="tok", direction="sell", amount=1, price=1, reason="a1")
        await mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a2")
        await mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a2")

        coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0})
        agents = [DummyAgent("a1"), DummyAgent("a2")]
        w1 = await coord.compute_weights(agents)
        assert w1["a2"] > w1["a1"]

        await mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1")
        await mem.log_trade(token="tok", direction="sell", amount=1, price=3, reason="a1")
        await mem.log_trade(token="tok", direction="buy", amount=1, price=3, reason="a2")
        await mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a2")

        w2 = await coord.compute_weights(agents)
        assert w2["a1"] > w2["a2"]

    asyncio.run(_run())


class _StubMemoryAgent:
    def __init__(self, trades):
        self.memory = types.SimpleNamespace(list_trades=lambda limit=None: list(trades))


def test_swarm_coordinator_uses_realized_roi_weights():
    trades = [
        types.SimpleNamespace(reason="a1", realized_roi=0.3, realized_notional=10.0),
        types.SimpleNamespace(reason="a2", realized_roi=0.2, realized_notional=1.0),
        types.SimpleNamespace(reason="a3", realized_roi=-0.1, realized_notional=20.0),
    ]
    mem_agent = _StubMemoryAgent(trades)
    coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0, "a3": 1.0})
    agents = [DummyAgent("a1"), DummyAgent("a2"), DummyAgent("a3")]

    async def _run() -> None:
        weights = await coord.compute_weights(agents)
        assert weights["a1"] > weights["a2"] > weights["a3"]

    asyncio.run(_run())


def test_swarm_coordinator_scales_by_realized_notional():
    trades = [
        {"reason": "a1", "metrics": {"realized_roi": 0.5, "realized_notional": 1.0}},
        {"reason": "a2", "metrics": {"realized_roi": 0.3, "realized_notional": 100.0}},
        {"reason": "a3", "metrics": {"realized_roi": 0.1, "realized_notional": 10.0}},
    ]
    mem_agent = _StubMemoryAgent(trades)
    coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0, "a3": 1.0})
    agents = [DummyAgent("a1"), DummyAgent("a2"), DummyAgent("a3")]

    async def _run() -> None:
        weights = await coord.compute_weights(agents)
        assert weights["a2"] > weights["a1"] > weights["a3"]
        # The much smaller notional for ``a1`` should keep the weight limited
        assert weights["a1"] < weights["a2"] * 0.1

    asyncio.run(_run())
