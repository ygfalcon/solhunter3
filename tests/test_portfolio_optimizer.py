import asyncio
import pytest

from solhunter_zero.agents.portfolio_optimizer import PortfolioOptimizer
from solhunter_zero.portfolio import Portfolio, Position
from solhunter_zero.risk import RiskManager


def fake_prices(tokens):
    return {t: 1.0 for t in tokens}


async def async_prices(tokens):
    return fake_prices(tokens)


def test_optimizer_prefers_higher_returns(monkeypatch):
    pf = Portfolio(path=None)
    pf.balances = {"a": Position("a", 1.0, 1.0), "b": Position("b", 1.0, 1.0)}

    monkeypatch.setattr(
        "solhunter_zero.agents.portfolio_optimizer.fetch_token_prices_async",
        async_prices,
    )
    monkeypatch.setattr(
        PortfolioOptimizer,
        "_expected_return",
        lambda self, t: {"a": 0.2, "b": 0.0}.get(t, 0.0),
    )

    opt = PortfolioOptimizer(risk_manager=RiskManager(max_allocation=1.0), threshold=0.0)
    actions = asyncio.run(opt.propose_trade("a", pf))
    sides = {(a["token"], a["side"]) for a in actions}
    assert ("a", "buy") in sides
    assert ("b", "sell") in sides


def test_optimizer_scales_with_risk(monkeypatch):
    pf = Portfolio(path=None)
    pf.balances = {"a": Position("a", 1.0, 1.0), "b": Position("b", 1.0, 1.0)}
    pf.record_prices({"a": 1.0, "b": 1.0})
    pf.update_risk_metrics()

    monkeypatch.setattr(
        "solhunter_zero.agents.portfolio_optimizer.fetch_token_prices_async",
        async_prices,
    )
    monkeypatch.setattr(
        PortfolioOptimizer,
        "_expected_return",
        lambda self, t: 0.1,
    )

    rm = RiskManager(max_allocation=0.5)

    def low_cov(self, **kwargs):
        return rm

    def high_cov(self, **kwargs):
        return RiskManager(max_allocation=0.1)

    opt = PortfolioOptimizer(risk_manager=rm, threshold=0.0)
    monkeypatch.setattr(RiskManager, "adjusted", low_cov)
    actions = asyncio.run(opt.propose_trade("a", pf))
    assert actions == []

    monkeypatch.setattr(RiskManager, "adjusted", high_cov)
    actions = asyncio.run(opt.propose_trade("a", pf))
    assert any(a["side"] == "sell" for a in actions)
