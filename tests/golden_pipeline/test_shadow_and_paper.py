from tests.golden_pipeline.conftest import SCENARIO_PAYLOADS, approx


def test_shadow_execution_and_paper_positions(golden_harness):
    snapshots = {snap.hash for snap in golden_harness.golden_snapshots}

    fills = golden_harness.virtual_fills
    assert len(fills) == len(golden_harness.decisions)
    for fill in fills:
        assert fill.snapshot_hash in snapshots
        assert fill.route == "VIRTUAL"
        assert fill.slippage_bps <= SCENARIO_PAYLOADS["orderbook_path"]["max_slippage_bps"]

    pnls = golden_harness.virtual_pnls
    assert len(pnls) == len(fills)
    latest = pnls[-1]
    assert abs(latest.realized_usd) > 0.0
    assert abs(latest.unrealized_usd) < 50.0
