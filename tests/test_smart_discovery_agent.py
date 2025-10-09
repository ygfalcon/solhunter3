import asyncio
import pytest

from solhunter_zero.agents.smart_discovery import SmartDiscoveryAgent
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self) -> None:
        super().__init__(path=None)
        self.balances = {}
        self.price_history = {}


def test_high_scoring_token_produces_priced_action(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = SmartDiscoveryAgent(
        trade_volume_threshold=100.0,
        trade_liquidity_threshold=50.0,
    )
    agent.metrics = {
        "high": {
            "predicted_score": 2.5,
            "sentiment": 0.3,
            "rank": 1,
            "volume": 150.0,
            "liquidity": 80.0,
            "mempool_score": 1.4,
        }
    }

    async def fake_resolve_price(token: str, portfolio: Portfolio):
        assert token == "high"
        return 1.23, {"source": "test"}

    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.resolve_price",
        fake_resolve_price,
    )

    actions = asyncio.run(agent.propose_trade("high", DummyPortfolio()))

    assert actions and actions[0]["side"] == "buy"
    assert actions[0]["price"] == pytest.approx(1.23)
    metadata = actions[0]["metadata"]
    assert metadata["predicted_score"] == pytest.approx(2.5)
    assert metadata["sentiment"] == pytest.approx(0.3)
    assert metadata["price_context"] == {"source": "test"}


def test_low_scoring_token_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    agent = SmartDiscoveryAgent(
        trade_volume_threshold=10.0,
        trade_liquidity_threshold=5.0,
    )
    agent.metrics = {
        "low": {
            "predicted_score": -0.1,
            "sentiment": -0.2,
            "rank": 2,
            "volume": 100.0,
            "liquidity": 50.0,
            "mempool_score": 0.8,
        }
    }

    async def fail_resolve_price(token: str, portfolio: Portfolio):
        raise AssertionError("price helper should not be called for low scoring tokens")

    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.resolve_price",
        fail_resolve_price,
    )

    actions = asyncio.run(agent.propose_trade("low", DummyPortfolio()))

    assert actions == []


def test_token_below_volume_or_liquidity_threshold_is_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = SmartDiscoveryAgent(
        trade_volume_threshold=200.0,
        trade_liquidity_threshold=150.0,
    )
    agent.metrics = {
        "thin": {
            "predicted_score": 3.0,
            "sentiment": 0.1,
            "rank": 1,
            "volume": 199.0,
            "liquidity": 150.0,
            "mempool_score": 1.0,
        },
        "illiquid": {
            "predicted_score": 3.0,
            "sentiment": 0.1,
            "rank": 2,
            "volume": 250.0,
            "liquidity": 149.0,
            "mempool_score": 1.0,
        },
    }

    async def fail_resolve_price(token: str, portfolio: Portfolio):
        raise AssertionError("price helper should not be called for filtered tokens")

    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.resolve_price",
        fail_resolve_price,
    )

    thin_actions = asyncio.run(agent.propose_trade("thin", DummyPortfolio()))
    illiquid_actions = asyncio.run(agent.propose_trade("illiquid", DummyPortfolio()))

    assert thin_actions == []
    assert illiquid_actions == []
