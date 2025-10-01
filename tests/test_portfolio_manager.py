import asyncio
import pytest

from solhunter_zero.agents.portfolio_manager import PortfolioManager
from solhunter_zero.portfolio import Portfolio, Position


def test_rebalance(monkeypatch):
    pf = Portfolio(path=None)
    pf.balances = {
        "tok1": Position("tok1", 2, 1.0),
        "tok2": Position("tok2", 2, 1.0),
    }

    async def prices(tokens):
        return {"tok1": 2.0, "tok2": 1.0}

    monkeypatch.setattr(
        "solhunter_zero.agents.portfolio_manager.fetch_token_prices_async",
        prices,
    )

    agent = PortfolioManager(rebalance_threshold=0.1)
    actions1 = asyncio.run(agent.propose_trade("tok1", pf))
    actions2 = asyncio.run(agent.propose_trade("tok2", pf))

    assert any(a["side"] == "sell" for a in actions1)
    assert any(a["side"] == "buy" for a in actions2)


def test_pnl_tracking(monkeypatch):
    pf = Portfolio(path=None)
    pf.add("tok", 1, 1.0)

    async def prices(tokens):
        return {"tok": 2.0}

    monkeypatch.setattr(
        "solhunter_zero.agents.portfolio_manager.fetch_token_prices_async",
        prices,
    )

    agent = PortfolioManager()
    asyncio.run(agent.propose_trade("tok", pf))

    assert agent.pnl_history
    assert agent.pnl_history[-1] == pytest.approx(1.0)
