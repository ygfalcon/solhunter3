from tests.golden_pipeline.conftest import (
    BASE58_MINTS,
    SCENARIO_PAYLOADS,
    STREAMS,
)


def test_bootstrap_runtime_advertises_readiness(golden_harness, fake_broker):
    # The in-process broker should respond to health checks and expose all
    # streams the Golden pipeline publishes to.
    assert fake_broker.streams()
    assert fake_broker.closed_streams == ()
    assert isinstance(golden_harness.metadata_requests, list)

    expected_streams = {
        STREAMS.discovery_candidates,
        STREAMS.token_snapshot,
        STREAMS.market_ohlcv,
        STREAMS.market_depth,
        STREAMS.golden_snapshot,
        STREAMS.trade_suggested,
        STREAMS.vote_decisions,
        STREAMS.virtual_fills,
    }
    assert expected_streams.issubset(set(fake_broker.streams()))

    snapshots = golden_harness.golden_snapshots
    assert snapshots, "pipeline never produced a golden snapshot"

    metrics = golden_harness.pipeline.metrics_snapshot()
    latency = metrics.get("latency_ms", {})
    depth_staleness = metrics.get("depth_staleness_ms", {})
    candle_age = metrics.get("candle_age_ms", {})
    assert latency.get("count", 0.0) >= 1.0
    assert depth_staleness.get("count", 0.0) >= 1.0
    assert candle_age.get("count", 0.0) >= 1.0

    # Provider coverage is reflected by recorded metadata fetch batches.
    providers_seen = {tuple(batch) for batch in golden_harness.metadata_requests}
    expected_batches = {
        (SCENARIO_PAYLOADS["discovery_plan"]["das"][0],),
        (SCENARIO_PAYLOADS["discovery_plan"]["das"][1],),
        (BASE58_MINTS["breaker"],),
    }
    assert providers_seen == expected_batches
