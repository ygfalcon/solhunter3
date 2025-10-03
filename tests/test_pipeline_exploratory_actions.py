import asyncio
from types import SimpleNamespace

import pytest

from solhunter_zero.pipeline.evaluation_service import EvaluationService
from solhunter_zero.pipeline.types import ScoredToken, TokenCandidate
from solhunter_zero.portfolio import Portfolio


class _StubAgentManager:
    def __init__(self, actions=None):
        self._actions = actions or []

    async def evaluate_with_swarm(self, token, portfolio):  # pragma: no cover - interface stub
        _ = portfolio
        return SimpleNamespace(token=token, actions=list(self._actions))


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_exploratory_action_generated_when_thresholds_met(monkeypatch):
    monkeypatch.setenv("DISCOVERY_MIN_VOLUME_USD", "50000")
    monkeypatch.setenv("DISCOVERY_MIN_LIQUIDITY_USD", "75000")

    candidate = TokenCandidate(
        token="TokenA",
        source="discovery",
        discovered_at=123.0,
        metadata={
            "price": 1.25,
            "volume": 120000,
            "liquidity": 150000,
            "symbol": "TKNA",
            "sources": ["helius"],
        },
    )
    scored = ScoredToken(token="TokenA", score=0.91, rank=1, candidate=candidate)

    service = EvaluationService(
        asyncio.Queue(),
        asyncio.Queue(),
        _StubAgentManager(),
        Portfolio(path=None),
        cache_ttl=0.0,
    )

    result = await service._evaluate_token(scored, asyncio.Semaphore(1))
    assert len(result.actions) == 1
    action = result.actions[0]
    assert action["type"] == "exploratory"
    assert action["side"] == "buy"
    assert action["price"] == pytest.approx(1.25)
    metadata = action.get("metadata", {})
    assert metadata.get("predicted_score") == pytest.approx(0.91)
    assert metadata.get("liquidity") == pytest.approx(150000)
    assert metadata.get("volume") == pytest.approx(120000)


@pytest.mark.anyio
async def test_exploratory_action_respects_volume_and_liquidity(monkeypatch):
    monkeypatch.setenv("DISCOVERY_MIN_VOLUME_USD", "50000")
    monkeypatch.setenv("DISCOVERY_MIN_LIQUIDITY_USD", "75000")

    candidate = TokenCandidate(
        token="TokenB",
        source="discovery",
        discovered_at=456.0,
        metadata={"price": 0.5, "volume": 1000, "liquidity": 1000},
    )
    scored = ScoredToken(token="TokenB", score=0.85, rank=1, candidate=candidate)

    service = EvaluationService(
        asyncio.Queue(),
        asyncio.Queue(),
        _StubAgentManager(),
        Portfolio(path=None),
        cache_ttl=0.0,
    )

    result = await service._evaluate_token(scored, asyncio.Semaphore(1))
    assert result.actions == []
