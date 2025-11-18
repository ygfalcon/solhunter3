import asyncio
from collections import Counter

import pytest

from solhunter_zero.golden_pipeline.discovery import DiscoveryStage
from solhunter_zero.golden_pipeline.types import DiscoveryCandidate

from tests.golden_pipeline.conftest import BASE58_MINTS


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_discovery_sources_deduplicate(golden_harness):
    events = golden_harness.discovery_events
    sources = Counter(event["source"] for event in events)

    assert sources["das"] == 2  # two unique mints via DAS
    assert sources["das_timeout"] == 3

    accepted_mints = {
        event["mint"]
        for event in events
        if event.get("accepted")
    }
    assert accepted_mints == {
        BASE58_MINTS["alpha"],
        BASE58_MINTS["beta"],
    }

    for source in ("fallback", "mempool", "amm", "pumpfun", "replay"):
        rejected = [event for event in events if event["source"] == source]
        assert rejected and all(event.get("accepted") is False for event in rejected)

    stage = golden_harness.pipeline._discovery_stage  # type: ignore[attr-defined]
    assert stage.seen_recently(BASE58_MINTS["alpha"]) is True
    assert stage.seen_recently(BASE58_MINTS["beta"]) is True

    metrics = golden_harness.pipeline.metrics_snapshot()
    success_stats = metrics.get("discovery.success_total", {})
    failure_stats = metrics.get("discovery.failure_total", {})
    dedupe_stats = metrics.get("discovery.dedupe_drops", {})
    breaker_stats = metrics.get("discovery.breaker_openings", {})

    assert (success_stats.get("max") or 0.0) >= 2.0
    assert (failure_stats.get("max") or 0.0) >= 3.0
    assert (dedupe_stats.get("max") or 0.0) >= 1.0
    assert (breaker_stats.get("max") or 0.0) >= 1.0


@pytest.mark.anyio
async def test_discovery_candidates_emit_concurrently():
    gate = asyncio.Event()
    concurrency = 0
    max_concurrency = 0

    async def emit(_: DiscoveryCandidate) -> None:
        nonlocal concurrency, max_concurrency
        await gate.wait()
        concurrency += 1
        max_concurrency = max(max_concurrency, concurrency)
        await asyncio.sleep(0.05)
        concurrency -= 1

    stage = DiscoveryStage(emit)
    candidates = [
        DiscoveryCandidate(mint=BASE58_MINTS["alpha"], asof=0.0),
        DiscoveryCandidate(mint=BASE58_MINTS["beta"], asof=0.0),
    ]

    tasks = [asyncio.create_task(stage.submit(candidate)) for candidate in candidates]

    # Ensure both submissions reach the emit gate before releasing them.
    await asyncio.sleep(0.01)
    gate.set()

    results = await asyncio.gather(*tasks)

    assert results == [True, True]
    assert max_concurrency >= 2
