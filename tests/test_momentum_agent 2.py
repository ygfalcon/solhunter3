import asyncio
import sys
import types

dummy_trans = types.ModuleType("transformers")
dummy_trans.pipeline = lambda *a, **k: lambda x: [{"label": "POSITIVE", "score": 0.5}]
sys.modules.setdefault("transformers", dummy_trans)
sys.modules["transformers"].pipeline = dummy_trans.pipeline

from solhunter_zero.agents.momentum import MomentumAgent
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_momentum_agent_buy(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.momentum.fetch_sentiment", lambda *a, **k: 0.5
    )
    agent = MomentumAgent(threshold=1.0, feeds=["f"])
    agent.scores["tok"] = 2.0
    actions = asyncio.run(agent.propose_trade("tok", DummyPortfolio()))
    assert actions and actions[0]["side"] == "buy"
    assert actions[0]["amount"] > 1.0


def test_momentum_agent_sell(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.momentum.fetch_sentiment", lambda *a, **k: 0.0
    )
    pf = DummyPortfolio()
    pf.balances["tok"] = Position("tok", 5.0, 1.0)
    agent = MomentumAgent(threshold=1.0)
    agent.scores["tok"] = 0.0
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "sell"
