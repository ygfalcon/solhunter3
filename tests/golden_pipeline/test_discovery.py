from collections import Counter

from tests.golden_pipeline.conftest import BASE58_MINTS


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
