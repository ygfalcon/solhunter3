import asyncio
import logging
from datetime import timedelta
from typing import Dict

import pytest

from solhunter_zero.agents.exit import ExitAgent
from solhunter_zero.agents import price_utils
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self) -> None:
        super().__init__(path=None)
        self.balances = {}


class MetricPortfolio(DummyPortfolio):
    def __init__(self, *, hold_seconds: float = 0.0, realized: float = 0.0, dd: float = 0.0) -> None:
        super().__init__()
        self._hold_seconds = hold_seconds
        self._realized = realized
        self._dd = dd

    def holding_duration(self, token: str) -> timedelta:
        return timedelta(seconds=self._hold_seconds)

    def realized_roi(self, token: str, price: float | None = None) -> float:
        return self._realized

    def current_drawdown(self, prices):  # type: ignore[override]
        return self._dd


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


def test_exit_agent_take_profit_includes_metrics(monkeypatch):
    portfolio = DummyPortfolio()
    portfolio.balances["tok"] = Position("tok", 2.0, 1.0, 1.0)

    async def price_high(tokens):
        return {"tok": 1.5}

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", price_high)

    agent = ExitAgent(take_profit=0.4)

    actions = asyncio.run(
        agent.propose_trade(
            "tok",
            portfolio,
            holding_duration=42.0,
            realized_roi=0.5,
            drawdown=0.1,
        )
    )

    assert actions and actions[0]["side"] == "sell"
    action = actions[0]
    assert action["holding_duration"] == pytest.approx(42.0)
    assert action["realized_roi"] == pytest.approx(0.5)
    assert action["drawdown"] == pytest.approx(0.1)


def test_exit_agent_stop_loss_uses_portfolio_metrics(monkeypatch):
    portfolio = MetricPortfolio(hold_seconds=90.0, realized=-0.25, dd=0.35)
    portfolio.balances["tok"] = Position("tok", 4.0, 1.0, 1.0)

    async def price_low(tokens):
        return {"tok": 0.7}

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", price_low)

    agent = ExitAgent(stop_loss=0.2)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    action = actions[0]
    assert action["holding_duration"] == pytest.approx(90.0)
    assert action["realized_roi"] == pytest.approx(-0.25)
    assert action["drawdown"] == pytest.approx(0.35)


class DrawdownPortfolio(DummyPortfolio):
    def __init__(self) -> None:
        super().__init__()
        self.observed_prices: Dict[str, float] | None = None

    def current_drawdown(self, prices):  # type: ignore[override]
        self.observed_prices = {str(k): float(v) for k, v in prices.items()}
        return 0.45


def test_exit_agent_trailing_records_drawdown(monkeypatch):
    portfolio = DrawdownPortfolio()
    portfolio.balances["tok"] = Position("tok", 3.0, 1.0, 1.5)

    async def price_drop(tokens):
        return {"tok": 0.9}

    monkeypatch.setattr(price_utils, "fetch_token_prices_async", price_drop)

    def fake_trailing(token, price, trailing):
        assert token == "tok"
        assert price == pytest.approx(0.9)
        return True

    monkeypatch.setattr(portfolio, "trailing_stop_triggered", fake_trailing)

    agent = ExitAgent(trailing=0.1)

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions and actions[0]["side"] == "sell"
    action = actions[0]
    assert action["drawdown"] == pytest.approx(0.45)
    assert action["realized_roi"] == pytest.approx(-0.1)
    assert "holding_duration" not in action
    assert portfolio.observed_prices is not None
    assert portfolio.observed_prices.get("tok") == pytest.approx(0.9)
