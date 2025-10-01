import asyncio
import types
import sys
import pytest
trans = pytest.importorskip("transformers")
pytest.importorskip("sklearn")
if not hasattr(trans, "pipeline"):
    trans.pipeline = lambda *a, **k: lambda x: []

# provide minimal solana.publickey stub
mod = types.ModuleType("solana.publickey")
class _Pub:
    def __init__(self, *a, **k):
        pass
mod.PublicKey = _Pub
sys.modules.setdefault("solana.publickey", mod)

from solhunter_zero.agents.smart_discovery import SmartDiscoveryAgent


class DummyGBR:
    def fit(self, X, y):
        return self

    def predict(self, X):
        return [sum(x) for x in X]


def fake_mempool_gen(url, **_):
    async def gen():
        yield {"address": "a", "score": 1.0}
        yield {"address": "b", "score": 2.0}
    return gen()


def test_ranking(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.stream_ranked_mempool_tokens",
        lambda url, **_: fake_mempool_gen(url),
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.onchain_metrics.fetch_volume_onchain",
        lambda t, u: 10.0 if t == "a" else 1.0,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.onchain_metrics.fetch_liquidity_onchain",
        lambda t, u: 5.0 if t == "a" else 1.0,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.news.fetch_sentiment", lambda *a, **k: 0.5
    )
    sklearn = types.SimpleNamespace(GradientBoostingRegressor=lambda: DummyGBR())
    monkeypatch.setitem(sys.modules, "sklearn.ensemble", sklearn)

    agent = SmartDiscoveryAgent()

    tokens = asyncio.run(agent.discover_tokens("ws://node", limit=2))
    assert tokens == ["a", "b"]
    assert agent.metrics["a"]["predicted_score"] > agent.metrics["b"]["predicted_score"]


def test_volume_filter(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.stream_ranked_mempool_tokens",
        lambda url, **_: fake_mempool_gen(url),
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.onchain_metrics.fetch_volume_onchain",
        lambda t, u: 1.0,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.onchain_metrics.fetch_liquidity_onchain",
        lambda t, u: 1.0,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.smart_discovery.news.fetch_sentiment", lambda *a, **k: 0.0
    )
    sklearn = types.SimpleNamespace(GradientBoostingRegressor=lambda: DummyGBR())
    monkeypatch.setitem(sys.modules, "sklearn.ensemble", sklearn)

    agent = SmartDiscoveryAgent(trend_volume_threshold=5.0)

    tokens = asyncio.run(agent.discover_tokens("ws://node", limit=2))
    assert tokens == []
