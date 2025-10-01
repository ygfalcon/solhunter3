import asyncio
import pytest

from solhunter_zero.agents.hedging_agent import HedgingAgent
from solhunter_zero.portfolio import Portfolio
from solhunter_zero.risk import RiskManager


def test_portfolio_correlation_and_hedged_weights():
    pf = Portfolio(path=None)
    pf.add("a", 1, 1.0)
    pf.add("b", 1, 1.0)
    seq = [
        {"a": 1.0, "b": 1.0},
        {"a": 2.0, "b": 2.0},
        {"a": 3.0, "b": 3.0},
        {"a": 2.0, "b": 2.0},
    ]
    for prices in seq:
        pf.record_prices(prices)
    corrs = pf.correlations()
    assert corrs[("a", "b")] > 0.9
    weights = pf.weights({"a": 2.0, "b": 1.0})
    hedged = pf.hedged_weights({"a": 2.0, "b": 1.0})
    assert hedged["a"] < weights["a"]


def test_risk_manager_with_portfolio_metrics():
    prices = {"a": [1, 2, 3], "b": [3, 2, 1]}
    weights = {"a": 0.5, "b": 0.5}
    rm = RiskManager()
    base = rm.adjusted()
    adj = rm.adjusted(price_history=prices, weights=weights, covar_threshold=0.1)
    assert adj.risk_tolerance < base.risk_tolerance


def test_hedging_agent_rebalances(monkeypatch):
    pf = Portfolio(path=None)
    pf.add("a", 1, 1.0)
    pf.add("b", 1, 1.0)

    async def prices(tokens):
        return {"a": 1.0, "b": 1.0, "USDC": 1.0}

    monkeypatch.setattr(
        "solhunter_zero.agents.hedging_agent.fetch_token_prices_async", prices
    )

    # build positive correlation history
    pf.record_prices({"a": 1.0, "b": 1.0, "USDC": 1.0})
    pf.record_prices({"a": 2.0, "b": 2.0, "USDC": 1.0})
    agent = HedgingAgent(threshold=0.0)
    actions = asyncio.run(agent.propose_trade("a", pf))
    assert actions

