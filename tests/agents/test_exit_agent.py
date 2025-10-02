import asyncio
import logging

import pytest

from solhunter_zero.agents.exit import ExitAgent
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

    monkeypatch.setattr(
        "solhunter_zero.agents.exit.fetch_token_prices_async", fake_fetch
    )

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

    monkeypatch.setattr(
        "solhunter_zero.agents.exit.fetch_token_prices_async", fake_fetch
    )

    agent = ExitAgent(stop_loss=0.1)

    with caplog.at_level(logging.WARNING):
        actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions == []
    assert any("could not obtain a valid price" in record.getMessage() for record in caplog.records)
