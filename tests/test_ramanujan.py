import asyncio

from solhunter_zero.agents.ramanujan_agent import RamanujanAgent
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_ramanujan_agent_buy():
    agent = RamanujanAgent(threshold=0.5)
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"


def test_ramanujan_agent_sell():
    agent = RamanujanAgent(threshold=0.5)
    pf = DummyPortfolio()
    pf.balances["token"] = Position("token", 1, 1.0, 1.0)
    actions = asyncio.run(agent.propose_trade("token", pf))
    assert actions and actions[0]["side"] == "sell"
