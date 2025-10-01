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

    mem.log_trade(token="tok", direction="buy", amount=1, price=2, reason="a1")
    mem.log_trade(token="tok", direction="sell", amount=1, price=1, reason="a1")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a2")
    mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a2")

    coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0})
    agents = [DummyAgent("a1"), DummyAgent("a2")]
    w1 = coord.compute_weights(agents)
    assert w1["a2"] > w1["a1"]

    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1")
    mem.log_trade(token="tok", direction="sell", amount=1, price=3, reason="a1")
    mem.log_trade(token="tok", direction="buy", amount=1, price=3, reason="a2")
    mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a2")

    w2 = coord.compute_weights(agents)
    assert w2["a1"] > w2["a2"]
