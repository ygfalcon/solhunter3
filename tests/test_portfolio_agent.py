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


def _patch_prices(monkeypatch, price: float):
    async def _fake(tokens):
        return {tok: price for tok in tokens}

    monkeypatch.setattr(
        "solhunter_zero.agents.portfolio_agent.fetch_token_prices_async",
        _fake,
    )


def test_sell_when_allocation_exceeds(monkeypatch):
    pf = DummyPortfolio(alloc=0.3, hold=True)
    agent = PortfolioAgent(max_allocation=0.2)

    _patch_prices(monkeypatch, 2.5)

    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["price"] == 2.5


def test_buy_when_allocation_available(monkeypatch):
    pf = DummyPortfolio(alloc=0.1, hold=False, value=50.0)
    agent = PortfolioAgent(max_allocation=0.2)

    _patch_prices(monkeypatch, 1.5)

    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"
    assert actions[0]["price"] == 1.5


def test_skips_adjustment_when_price_unavailable(monkeypatch):
    pf = DummyPortfolio(alloc=0.3, hold=True)
    agent = PortfolioAgent(max_allocation=0.2)

    _patch_prices(monkeypatch, 0.0)

    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions == []
    assert agent.skipped_adjustments == [
        {"token": "tok", "side": "sell", "reason": "missing_price"}
    ]
