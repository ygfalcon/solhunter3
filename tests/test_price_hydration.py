import pytest

from solhunter_zero.agents.ramanujan_agent import RamanujanAgent
from solhunter_zero.agents.trend import TrendAgent
from solhunter_zero.portfolio import Portfolio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_ramanujan_agent_attaches_price(monkeypatch):
    agent = RamanujanAgent(threshold=-1.0, amount=2.0)
    portfolio = Portfolio(path=None)

    async def fake_resolve(token, pf):
        assert pf is portfolio
        return 12.34, {"source": "test"}

    monkeypatch.setattr(
        "solhunter_zero.agents.ramanujan_agent.resolve_price",
        fake_resolve,
    )

    actions = await agent.propose_trade("SOL", portfolio)
    assert actions
    action = actions[0]
    assert action["price"] == pytest.approx(12.34)
    assert action["amount"] == pytest.approx(2.0)


@pytest.mark.anyio
async def test_trend_agent_attaches_price(monkeypatch):
    agent = TrendAgent(volume_threshold=1.0, sentiment_threshold=1.0)
    portfolio = Portfolio(path=None)

    async def fake_trending():
        return ["SOL"]

    async def fake_metrics(token):
        assert token == "SOL"
        return {"volume": 5.0}

    async def fake_sentiment(self):
        return 2.0

    async def fake_resolve(token, pf):
        assert pf is portfolio
        return 3.21, {"source": "mock"}

    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_trending_tokens_async",
        fake_trending,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_token_metrics_async",
        fake_metrics,
    )
    monkeypatch.setattr(TrendAgent, "_current_sentiment", fake_sentiment)
    monkeypatch.setattr("solhunter_zero.agents.trend.resolve_price", fake_resolve)

    actions = await agent.propose_trade("SOL", portfolio)
    assert actions
    action = actions[0]
    assert action["price"] == pytest.approx(3.21)
    assert action["volume"] == pytest.approx(5.0)
    assert action["sentiment"] == pytest.approx(4.0)


@pytest.mark.anyio
async def test_trend_agent_skips_when_price_missing(monkeypatch):
    agent = TrendAgent(volume_threshold=1.0, sentiment_threshold=1.0)
    portfolio = Portfolio(path=None)

    async def fake_trending():
        return ["SOL"]

    async def fake_metrics(token):
        return {"volume": 5.0}

    async def fake_sentiment(self):
        return 2.0

    async def fake_resolve(token, pf):
        return 0.0, {"source": "missing"}

    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_trending_tokens_async",
        fake_trending,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_token_metrics_async",
        fake_metrics,
    )
    monkeypatch.setattr(TrendAgent, "_current_sentiment", fake_sentiment)
    monkeypatch.setattr("solhunter_zero.agents.trend.resolve_price", fake_resolve)

    actions = await agent.propose_trade("SOL", portfolio)
    assert actions == []
