from tests.golden_pipeline.conftest import (
    SCENARIO_PAYLOADS,
    STREAMS,
    approx,
    assert_tolerant_dict,
)


def test_market_enrichment_combines_depth(fake_broker, golden_harness):
    depth_events = fake_broker.events[STREAMS.market_depth]
    assert depth_events, "no depth events recorded"

    expected_mid = (
        SCENARIO_PAYLOADS["depth_snapshots"][0].mid * SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["1"]
        + SCENARIO_PAYLOADS["depth_snapshots"][1].mid * SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["1"]
    ) / (
        SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["1"]
        + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["1"]
    )
    combined_depth = next(event for event in depth_events if event["mid_usd"] == approx(expected_mid))

    assert combined_depth["spread_bps"] == approx(min(f.spread_bps for f in SCENARIO_PAYLOADS["depth_snapshots"]))
    assert_tolerant_dict(
        combined_depth["depth_pct"],
        {
            "1": SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["1"]
            + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["1"],
            "2": SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["2"]
            + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["2"],
            "5": SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["5"]
            + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["5"],
        },
    )

    bars = fake_broker.events[STREAMS.market_ohlcv]
    assert len(bars) == 1
    bar = bars[0]
    assert bar["trades"] == 2
    assert bar["buyers"] == 1
    assert bar["vol_usd"] == approx(abs(SCENARIO_PAYLOADS["tape_events"][0].amount_quote) + abs(SCENARIO_PAYLOADS["tape_events"][1].amount_quote))

    snapshot = next(
        snap for snap in golden_harness.golden_snapshots if snap.px["mid_usd"] == approx(expected_mid)
    )
    assert snapshot.metrics["latency_ms"] > 0.0
    assert snapshot.metrics["depth_staleness_ms"] > 0.0
    assert snapshot.metrics["latency_ms"] == approx(snapshot.metrics["depth_staleness_ms"])
