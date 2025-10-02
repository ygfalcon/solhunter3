import asyncio
import logging

import pytest

from solhunter_zero.agents.exit import ExitAgent
from solhunter_zero.agents import price_utils
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self) -> None:
        super().__init__(path=None)
        self.balances = {}


def test_exit_agent_sell_uses_positive_price(monkeypatch):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 5.0, 1.0, 1.0)

    async def fake_fetch(tokens):
        assert tokens == {"tok"}
        return {"tok": 0.8}

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", fake_fetch)

    agent = ExitAgent(stop_loss=0.1)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["price"] > 0
    assert actions[0]["price"] == pytest.approx(0.8)


def test_exit_agent_logs_and_aborts_without_price(monkeypatch, caplog):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 5.0, 1.0, 1.0)

    async def fake_fetch(tokens):
        assert tokens == {"tok"}
        return {}

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", fake_fetch)

    agent = ExitAgent(stop_loss=0.1)

    with caplog.at_level(logging.WARNING):
        actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions == []
    assert any("could not obtain a valid price" in record.getMessage() for record in caplog.records)


def test_exit_agent_uses_cached_price(monkeypatch):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 5.0, 1.0, 1.0)

    monkeypatch.setattr(
        price_utils,
        "get_cached_price",
        lambda token: 1.5 if token == "tok" else None,
    )

    called = False

    async def fail_fetch(tokens):
        nonlocal called
        called = True
        raise AssertionError("fetch should not be called when cached price exists")

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", fail_fetch)

    agent = ExitAgent(take_profit=0.2)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["price"] == pytest.approx(1.5)
    assert called is False


def test_exit_agent_trailing_stop_ignores_zero_price(monkeypatch):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 5.0, 1.0, 1.5)

    async def zero_price(tokens):
        return {"tok": 0.0}

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", zero_price)

    triggered = False

    def fake_trailing(token, price, trailing):
        nonlocal triggered
        triggered = True
        return True

    monkeypatch.setattr(portfolio, "trailing_stop_triggered", fake_trailing)

    agent = ExitAgent(trailing=0.1)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions == []
    assert triggered is False
