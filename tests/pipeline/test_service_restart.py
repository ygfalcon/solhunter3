import asyncio

import pytest

from solhunter_zero.pipeline.discovery_service import DiscoveryService
from solhunter_zero.pipeline.scoring_service import ScoringService
from solhunter_zero.pipeline.types import TokenCandidate


class _DummyPortfolio:
    price_history: dict[str, list[float]] = {}
    risk_metrics: dict[str, float] = {}


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_scoring_service_restart(monkeypatch):
    monkeypatch.setenv("SCORING_COOLDOWN", "0")
    input_queue: asyncio.Queue[list[TokenCandidate]] = asyncio.Queue()
    output_queue: asyncio.Queue = asyncio.Queue()
    portfolio = _DummyPortfolio()
    service = ScoringService(input_queue, output_queue, portfolio, cooldown=0, workers=1)

    await service.start()
    await asyncio.sleep(0)
    assert len(service._worker_tasks) == 1

    first = TokenCandidate(
        token="token-1",
        source="test",
        discovered_at=0.0,
        metadata={"profile": {"trend_score": 0.0, "volume_score": 0.0}},
    )
    await input_queue.put([first])
    scored_first = await asyncio.wait_for(output_queue.get(), timeout=2.0)
    assert scored_first.token == "token-1"

    await service.stop()

    await service.start()

    second = TokenCandidate(
        token="token-2",
        source="test",
        discovered_at=0.0,
        metadata={"profile": {"trend_score": 0.0, "volume_score": 0.0}},
    )
    await input_queue.put([second])
    scored_second = await asyncio.wait_for(output_queue.get(), timeout=2.0)
    assert scored_second.token == "token-2"

    await service.stop()

    monkeypatch.delenv("SCORING_COOLDOWN", raising=False)


@pytest.mark.anyio
async def test_scoring_service_single_worker_env(monkeypatch):
    monkeypatch.setenv("SCORING_COOLDOWN", "0")
    monkeypatch.setenv("SCORING_WORKERS", "1")
    input_queue: asyncio.Queue[list[TokenCandidate]] = asyncio.Queue()
    output_queue: asyncio.Queue = asyncio.Queue()
    portfolio = _DummyPortfolio()
    service = ScoringService(input_queue, output_queue, portfolio, cooldown=0)

    await service.start()
    await asyncio.sleep(0)
    try:
        assert len(service._worker_tasks) == 1
    finally:
        await service.stop()
        monkeypatch.delenv("SCORING_COOLDOWN", raising=False)
        monkeypatch.delenv("SCORING_WORKERS", raising=False)


@pytest.mark.anyio
async def test_discovery_service_restart(monkeypatch):
    queue: asyncio.Queue[list[TokenCandidate]] = asyncio.Queue()
    service = DiscoveryService(
        queue,
        interval=0.01,
        cache_ttl=0.0,
        empty_cache_ttl=0.0,
        backoff_factor=1.0,
        offline=True,
    )
    service._primed = True

    counter = 0

    async def fake_fetch(self, *, agent=None):
        nonlocal counter
        counter += 1
        token = f"token-{counter}"
        self._last_fetch_fresh = True
        return [token], {token: {}}

    monkeypatch.setattr(
        service,
        "_fetch",
        fake_fetch.__get__(service, DiscoveryService),
    )

    await service.start()

    first_batch = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert [cand.token for cand in first_batch] == ["token-1"]
    queue.task_done()

    await service.stop()

    await service.start()

    second_batch = await asyncio.wait_for(queue.get(), timeout=1.0)
    assert [cand.token for cand in second_batch] == ["token-2"]
    queue.task_done()

    await service.stop()
