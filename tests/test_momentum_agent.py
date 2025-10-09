import asyncio
import pytest

from solhunter_zero.agents.momentum import MomentumAgent
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self) -> None:
        super().__init__(path=None)
        self.balances = {}


def test_momentum_agent_buy_uses_positive_price():
    agent = MomentumAgent(threshold=1.0, price_helper=lambda _: 2.5)
    agent.scores["tok"] = 2.0

    actions = asyncio.run(agent.propose_trade("tok", DummyPortfolio()))

    assert actions and actions[0]["side"] == "buy"
    assert actions[0]["price"] > 0
    assert actions[0]["price"] == pytest.approx(2.5)


def test_momentum_agent_sell_uses_positive_price():
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 5.0, 1.0)

    agent = MomentumAgent(threshold=1.0, price_helper=lambda _: 3.75)
    agent.scores["tok"] = 0.0

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["price"] > 0
    assert actions[0]["price"] == pytest.approx(3.75)
