import asyncio
import json

import pytest

from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.agents.rl_weight_agent import RLWeightAgent
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.event_bus import publish, reset

try:
    from solhunter_zero.swarm_coordinator import SwarmCoordinator
except ImportError:  # pragma: no cover - optional torch dependency
    SwarmCoordinator = None  # type: ignore[assignment]

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
    w = asyncio.run(rl.train(["a1", "a2"]))
    assert w["a1"] == 0.5
    assert w["a2"] == 1.5


def test_swarm_coordinator_applies_rl_weights(tmp_path):
    if SwarmCoordinator is None:
        pytest.skip("SwarmCoordinator requires optional torch dependency")

    mem = AdvancedMemory(url="sqlite:///:memory:")
    mem_agent = MemoryAgent(mem)
    rl = RLWeightAgent(mem_agent, weights_path=str(tmp_path / "w.json"))
    rl.rl.population = [
        {"weights": {"a1": 0.5, "a2": 1.5}, "risk": {"risk_multiplier": 1.0}},
        {"weights": {"a1": 1.0, "a2": 1.0}, "risk": {"risk_multiplier": 1.0}},
    ]
    agents = [DummyAgent("a1"), DummyAgent("a2"), rl]
    coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0})
    w = asyncio.run(coord.compute_weights(agents))
    assert w["a1"] < w["a2"]


def test_rl_weight_agent_refreshes_on_weights_event(tmp_path):
    async def _run() -> None:
        reset()
        weights_path = tmp_path / "weights.json"
        initial = [{"weights": {"a1": 1.0}, "risk": {"risk_multiplier": 1.0}}]
        weights_path.write_text(json.dumps(initial))
        agent = RLWeightAgent(MemoryAgent(), weights_path=str(weights_path))
        try:
            updated = [{"weights": {"a1": 2.5}, "risk": {"risk_multiplier": 1.0}}]
            weights_path.write_text(json.dumps(updated))
            publish("weights_updated", {"weights": {"a1": 2.5}})

            for _ in range(5):
                if agent.rl.population[0]["weights"].get("a1") == 2.5:
                    break
                await asyncio.sleep(0.1)

            assert agent.rl.population[0]["weights"]["a1"] == 2.5
        finally:
            agent.close()
            reset()

    asyncio.run(_run())
