import pytest
pytest.importorskip("torch.nn.utils.rnn")
from solhunter_zero.swarm_coordinator import SwarmCoordinator
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.regime import detect_regime

class DummyAgent:
    def __init__(self, name):
        self.name = name


@pytest.mark.asyncio
async def test_swarm_coordinator_regime_weights():
    mem = Memory("sqlite:///:memory:")
    mem_agent = MemoryAgent(mem)

    await mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1")
    await mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a1")
    await mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a2")
    await mem.log_trade(token="tok", direction="sell", amount=1, price=1.5, reason="a2")

    coord = SwarmCoordinator(
        mem_agent,
        {"a1": 1.0, "a2": 1.0},
        regime_weights={"bull": {"a1": 2.0}, "bear": {"a2": 2.0}},
    )
    agents = [DummyAgent("a1"), DummyAgent("a2")]
    w_bull = await coord.compute_weights(agents, regime="bull")
    w_bear = await coord.compute_weights(agents, regime="bear")
    assert w_bull["a1"] > w_bull["a2"]
    assert w_bear["a2"] >= w_bull["a2"]


def test_detect_regime():
    assert detect_regime([1, 2, 3]) == "bull"
    assert detect_regime([3, 2, 1]) == "bear"
    assert detect_regime([1, 1.01, 1.0]) == "sideways"
