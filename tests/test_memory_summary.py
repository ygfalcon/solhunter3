import asyncio
import numpy as np
from solhunter_zero.agents.swarm import AgentSwarm
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.portfolio import Portfolio

class DummyModel:
    def __init__(self):
        self.dim = 2
    def get_sentence_embedding_dimension(self):
        return self.dim
    def encode(self, texts):
        return np.array([[len(t), len(t)+1] for t in texts], dtype="float32")

class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}

class CaptureAgent:
    name = "cap"
    def __init__(self):
        self.summary = None
    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None, summary=None):
        self.summary = summary
        return []

def test_latest_summary_empty(tmp_path):
    mem = AdvancedMemory(url=f"sqlite:///{tmp_path/'mem.db'}")
    mem.model = DummyModel()
    summary = mem.latest_summary()
    assert summary.shape == (2,)
    assert np.all(summary == 0)

def test_swarm_passes_summary(tmp_path):
    mem = AdvancedMemory(url=f"sqlite:///{tmp_path/'db.db'}")
    mem.model = DummyModel()
    mem.log_trade(token="TOK", direction="buy", amount=1, price=1, context="hey")
    agent = CaptureAgent()
    swarm = AgentSwarm([agent], memory=mem)
    pf = DummyPortfolio()
    asyncio.run(swarm.propose("TOK", pf))
    assert isinstance(agent.summary, np.ndarray)
    assert agent.summary.shape == (2,)
    assert not np.all(agent.summary == 0)
