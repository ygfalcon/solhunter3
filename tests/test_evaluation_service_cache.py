import asyncio
from types import SimpleNamespace

import pytest

from solhunter_zero.pipeline.evaluation_service import EvaluationService
from solhunter_zero.pipeline.types import ScoredToken, TokenCandidate
from solhunter_zero.portfolio import Portfolio


class _CountingAgent:
    def __init__(self, actions=None):
        self.actions = actions or [
            {"type": "swap", "metadata": {"note": "from agent"}},
        ]
        self.calls = 0

    async def evaluate_with_swarm(self, token, portfolio):
        _ = (token, portfolio)
        self.calls += 1
        return SimpleNamespace(actions=list(self.actions))


@pytest.fixture
def anyio_backend():
    return "asyncio"


def _scored(
    token: str,
    score: float,
    rank: int,
    price: float,
    *,
    discovered_at: float = 0.0,
    metadata: dict | None = None,
) -> ScoredToken:
    payload = {"price": price}
    if metadata:
        payload.update(metadata)
    candidate = TokenCandidate(
        token=token,
        source="discovery",
        discovered_at=discovered_at,
        metadata=payload,
    )
    return ScoredToken(token=token, score=score, rank=rank, candidate=candidate)


@pytest.mark.anyio
async def test_evaluation_cache_hit_for_duplicate_signature():
    agent = _CountingAgent()
    service = EvaluationService(
        asyncio.Queue(),
        asyncio.Queue(),
        agent,
        Portfolio(path=None),
        cache_ttl=30.0,
    )

    first = await service._evaluate_token(_scored("TokenX", 0.91, 3, 1.23))
    dup = await service._evaluate_token(_scored("TokenX", 0.91, 3, 1.23))

    assert agent.calls == 1
    assert not first.cached
    assert dup.cached
    assert dup.metadata["score"] == pytest.approx(0.91)
    assert dup.metadata["rank"] == 3
    assert dup.metadata["price_band"] == first.metadata["price_band"]


@pytest.mark.anyio
async def test_evaluation_cache_busts_when_score_band_changes():
    agent = _CountingAgent()
    service = EvaluationService(
        asyncio.Queue(),
        asyncio.Queue(),
        agent,
        Portfolio(path=None),
        cache_ttl=30.0,
    )

    await service._evaluate_token(_scored("TokenY", 0.70, 5, 0.42))
    refreshed = await service._evaluate_token(_scored("TokenY", 0.96, 5, 0.42))

    assert agent.calls == 2
    assert not refreshed.cached
    assert refreshed.metadata["score"] == pytest.approx(0.96)
    assert refreshed.metadata["price_band"] == "1e-1"


@pytest.mark.anyio
async def test_evaluation_cache_busts_when_metadata_changes():
    agent = _CountingAgent()
    service = EvaluationService(
        asyncio.Queue(),
        asyncio.Queue(),
        agent,
        Portfolio(path=None),
        cache_ttl=30.0,
    )

    await service._evaluate_token(
        _scored("TokenZ", 0.82, 9, 2.5, metadata={"volume": 123, "sources": ["a"]})
    )
    refreshed = await service._evaluate_token(
        _scored("TokenZ", 0.82, 9, 2.5, metadata={"volume": 456, "sources": ["a"]})
    )

    assert agent.calls == 2
    assert not refreshed.cached
