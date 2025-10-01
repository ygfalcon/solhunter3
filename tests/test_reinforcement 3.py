import asyncio

from solhunter_zero.agents.reinforcement import ReinforcementAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_reinforcement_agent_buy():
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1)
    mem.log_trade(token="tok", direction="sell", amount=1, price=2)
    mem_agent = MemoryAgent(mem)
    agent = ReinforcementAgent(memory_agent=mem_agent, learning_rate=1.0, epsilon=0.0)
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"


def test_reinforcement_agent_sell():
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=2)
    mem.log_trade(token="tok", direction="sell", amount=1, price=1)
    mem_agent = MemoryAgent(mem)
    agent = ReinforcementAgent(memory_agent=mem_agent, learning_rate=1.0, epsilon=0.0)
    pf = DummyPortfolio()
    pf.balances["tok"] = Position("tok", 1, 1.0, 1.0)
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "sell"
    assert agent.q["tok"]["sell"] > agent.q["tok"]["buy"]
