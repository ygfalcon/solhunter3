from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.agents.rl_weight_agent import RLWeightAgent
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.swarm_coordinator import SwarmCoordinator

class DummyAgent:
    def __init__(self, name):
        self.name = name

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return []


def test_rl_weight_agent_trains(tmp_path):
    mem = AdvancedMemory(url="sqlite:///:memory:")
    mem_agent = MemoryAgent(mem)
    rl = RLWeightAgent(mem_agent, weights_path=str(tmp_path / "w.json"))
    rl.rl.population = [
        {"weights": {"a1": 0.5, "a2": 1.5}, "risk": {"risk_multiplier": 1.0}},
        {"weights": {"a1": 1.0, "a2": 1.0}, "risk": {"risk_multiplier": 1.0}},
    ]
    w = rl.train(["a1", "a2"])
    assert w["a1"] == 0.5
    assert w["a2"] == 1.5


def test_swarm_coordinator_applies_rl_weights(tmp_path):
    mem = AdvancedMemory(url="sqlite:///:memory:")
    mem_agent = MemoryAgent(mem)
    rl = RLWeightAgent(mem_agent, weights_path=str(tmp_path / "w.json"))
    rl.rl.population = [
        {"weights": {"a1": 0.5, "a2": 1.5}, "risk": {"risk_multiplier": 1.0}},
        {"weights": {"a1": 1.0, "a2": 1.0}, "risk": {"risk_multiplier": 1.0}},
    ]
    agents = [DummyAgent("a1"), DummyAgent("a2"), rl]
    coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0})
    w = coord.compute_weights(agents)
    assert w["a1"] < w["a2"]
