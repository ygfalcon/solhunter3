from tests.golden_pipeline.conftest import approx


def test_compact_summary_report(golden_harness):
    summary = golden_harness.summary()

    assert summary["discoveries_by_source"] == {"das": 2}
    assert summary["snapshots_by_hash"] == {
        "19c3c76dc1d836df63a8883650ca943ec965bf59fb057aa96417d99730aed2aa": 1,
        "842a1f1710fb0ec196302cf94ccb28a591ced001b46d23c55f6edb145edb515b": 1,
        "1c296bfbaf73fb81116630271ba34c633a60ef2dbd2d6c47bfccee2d6dd2b93e": 1,
    }
    assert summary["suggestions_by_agent"] == {"momentum_v1": 3, "meanrev_v1": 3}
    assert summary["decisions_by_side"] == {"buy": 3}
    assert summary["shadow_fills_by_venue"] == {"VIRTUAL": 3}

    pnl = summary["paper_pnl"]
    assert pnl["count"] == 3
    assert pnl["latest_realized"] == approx(-8.922500000000314, rel=1e-6, abs_tol=1e-6)
    assert pnl["latest_unrealized"] == approx(-10.077148994763776, rel=1e-6, abs_tol=1e-6)
