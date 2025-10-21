import pytest

pytest_plugins = ["tests.golden_pipeline.synth_seed"]


def test_ui_smoke_synth_values(runtime, bus, kv, synth_seed):
    synth_seed()
    runtime.wait_for_websockets()

    discovery_console = runtime.ui_state.snapshot_discovery_console()
    assert len(discovery_console["candidates"]) >= 5

    golden_snapshot = runtime.ui_state.snapshot_golden_snapshots()
    snapshots = golden_snapshot.get("snapshots", [])
    assert len(snapshots) == 7
    assert all((entry.get("liq") or 0.0) > 0.0 for entry in snapshots)

    suggestions = runtime.ui_state.snapshot_suggestions()
    assert len(suggestions.get("suggestions", [])) >= 2

    shadow = runtime.ui_state.shadow_provider()
    assert len(shadow.get("virtual_fills", [])) == 1
    assert shadow.get("paper_positions")
    assert shadow["paper_positions"][0]["unrealized_usd"] > 0.0

    status = runtime.ui_state.snapshot_status()
    for key in ("bus_latency_ms", "ohlcv_lag_ms", "depth_lag_ms", "golden_lag_ms"):
        value = status.get(key)
        assert value is not None and 0.0 < value < 500.0
    assert status.get("event_bus") is True
    assert status.get("trading_loop") is True
    assert status.get("loop_state") == "running"

    summary = runtime.summary_snapshot()
    assert summary.get("suggestions_5m") == 3
    assert summary.get("open_vote_windows") == 2
    assert summary.get("golden_hashes") == 7
    assert summary.get("acceptance_rate") == pytest.approx(66.7, rel=1e-3)
    pnl = summary.get("paper_pnl", {})
    assert pnl.get("pnl_1d") == pytest.approx(64.0, rel=1e-3)

    vote_state = runtime.ui_state.snapshot_vote_windows()
    assert len(vote_state.get("windows", [])) >= 2
