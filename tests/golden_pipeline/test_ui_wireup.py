from tests.golden_pipeline.conftest import BASE58_MINTS, STREAMS


def test_ui_panels_receive_data(fake_broker, golden_harness):
    required_streams = {
        STREAMS.discovery_candidates,
        STREAMS.token_snapshot,
        STREAMS.market_ohlcv,
        STREAMS.market_depth,
        STREAMS.golden_snapshot,
        STREAMS.trade_suggested,
        STREAMS.vote_decisions,
        STREAMS.virtual_fills,
    }
    for stream in required_streams:
        assert stream in fake_broker.events
        assert fake_broker.events[stream], f"stream {stream} was empty"

    discovery_payloads = fake_broker.events[STREAMS.discovery_candidates]
    discovered_mints = {payload["mint"] for payload in discovery_payloads}
    assert discovered_mints == {BASE58_MINTS["alpha"], BASE58_MINTS["beta"]}

    bus_snapshot_hashes = [payload["hash"] for payload in fake_broker.events[STREAMS.golden_snapshot]]
    stored_snapshot_hashes = [snapshot.hash for snapshot in golden_harness.golden_snapshots]
    assert bus_snapshot_hashes == stored_snapshot_hashes

    suggestion_inputs = {payload["inputs_hash"] for payload in fake_broker.events[STREAMS.trade_suggested]}
    assert suggestion_inputs == set(bus_snapshot_hashes)

    for snapshot in golden_harness.golden_snapshots:
        assert snapshot.metrics["latency_ms"] > 0.0
        assert snapshot.metrics["depth_staleness_ms"] > 0.0

    summary = golden_harness.summary()
    pnl_summary = summary["paper_pnl"]
    assert pnl_summary["count"] == len(golden_harness.virtual_pnls)
    assert abs(pnl_summary["latest_unrealized"]) < 50.0
