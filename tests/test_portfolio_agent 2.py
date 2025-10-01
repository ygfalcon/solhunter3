import asyncio

from solhunter_zero.agents.portfolio_agent import PortfolioAgent
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self, alloc: float, hold: bool, value: float = 100.0):
        super().__init__(path=None)
        if hold:
            self.balances["tok"] = Position("tok", 1.0, 1.0)
        self._alloc = alloc
        self._value = value

    def percent_allocated(self, token: str, prices=None):
        return self._alloc

    def total_value(self, prices=None):
        return self._value


def test_sell_when_allocation_exceeds(monkeypatch):
    pf = DummyPortfolio(alloc=0.3, hold=True)
    agent = PortfolioAgent(max_allocation=0.2)

    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "sell"


def test_buy_when_allocation_available(monkeypatch):
    pf = DummyPortfolio(alloc=0.1, hold=False, value=50.0)
    agent = PortfolioAgent(max_allocation=0.2)

    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"
