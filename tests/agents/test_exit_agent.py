from __future__ import annotations

import asyncio
import logging

import pytest

from solhunter_zero import prices
from solhunter_zero.agents.exit import ExitAgent
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self) -> None:
        super().__init__(path=None)
        self.balances = {}


def test_exit_agent_uses_cached_price_when_fetch_fails(monkeypatch):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 10.0, 2.0, 2.5)

    async def failing_fetch(tokens):  # pragma: no cover - exercised via await
        raise RuntimeError("network down")

    monkeypatch.setattr(prices, "fetch_token_prices_async", failing_fetch)
    monkeypatch.setattr(
        prices,
        "PRICE_CACHE",
        prices.TTLCache(maxsize=16, ttl=prices.PRICE_CACHE_TTL),
        raising=False,
    )
    prices.update_price_cache("tok", 1.6)

    agent = ExitAgent(stop_loss=0.1)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["price"] > 0
    assert actions[0]["price"] == pytest.approx(1.6)


def test_exit_agent_skips_trailing_on_missing_price(monkeypatch, caplog):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 5.0, 2.0, 3.0)

    async def zero_price(tokens):  # pragma: no cover - exercised via await
        return {"tok": 0.0}

    monkeypatch.setattr(prices, "fetch_token_prices_async", zero_price)
    monkeypatch.setattr(prices, "get_cached_price", lambda token: None)

    agent = ExitAgent(trailing=0.2, stop_loss=0.2)

    with caplog.at_level(logging.INFO):
        actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions == []
    assert any("skipping trailing stop" in rec.message for rec in caplog.records)


def test_exit_agent_falls_back_to_cached_on_zero_quote(monkeypatch):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 2.0, 2.0, 2.5)

    async def zero_price(tokens):  # pragma: no cover - exercised via await
        return {"tok": 0.0}

    monkeypatch.setattr(prices, "fetch_token_prices_async", zero_price)
    monkeypatch.setattr(prices, "get_cached_price", lambda token: 1.4)

    agent = ExitAgent(stop_loss=0.1)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["price"] > 0
    assert actions[0]["price"] == pytest.approx(1.4)
