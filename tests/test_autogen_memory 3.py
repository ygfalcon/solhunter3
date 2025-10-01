import asyncio
import pytest

from solhunter_zero.agents.swarm import AgentSwarm
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


class MemoryAwareAgent:
    name = "aware"

    def __init__(self):
        self.last_outcome = None
        self.swarm = None
        self.memory = None

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        if self.last_outcome is False:
            return []
        rate = self.swarm.success_rate(token) if self.swarm else 0.0
        if rate < 0.5 and self.last_outcome is not None:
            return []
        return [{"token": token, "side": "buy", "amount": 1.0, "price": 1.0}]


def test_swarm_feedback(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    db = tmp_path / "mem.db"
    idx = tmp_path / "index.faiss"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    agent = MemoryAwareAgent()
    swarm = AgentSwarm([agent], memory=mem)
    pf = DummyPortfolio()

    # Initial successful trade
    actions = asyncio.run(swarm.propose("TOK", pf))
    assert actions
    swarm.record_results([{"ok": True} for _ in actions])

    actions2 = asyncio.run(swarm.propose("TOK", pf))
    assert actions2
    swarm.record_results([{"ok": False} for _ in actions2])

    actions3 = asyncio.run(swarm.propose("TOK", pf))
    assert actions3 == []
    assert mem.simulation_success_rate("TOK") == pytest.approx(0.5)
    assert mem.simulation_success_rate("TOK", agent="aware") == pytest.approx(0.5)
