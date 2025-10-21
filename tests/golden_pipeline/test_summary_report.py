from tests.golden_pipeline.conftest import approx


def test_compact_summary_report(golden_harness):
    summary = golden_harness.summary()

    assert summary["discoveries_by_source"] == {"das": 2}
    assert summary["snapshots_by_hash"] == {
        "abded387b62174dd99cc6653cbb52c4f5a2ea62fd02e65153c2adde717622fbd": 1,
        "c0cc8f83bb275ce9fbc8342249a38288cdbd625bdda5e388fa51ab19f435d485": 1,
        "b0a26be535aed6a1c9e605cfcf3f965495e5593099b649bf573921dec0191f11": 1,
    }
    assert summary["suggestions_by_agent"] == {"momentum_v1": 3, "meanrev_v1": 3}
    assert summary["decisions_by_side"] == {"buy": 3}
    assert summary["shadow_fills_by_venue"] == {"VIRTUAL": 3}

    pnl = summary["paper_pnl"]
    assert pnl["count"] == 3
    assert pnl["latest_realized"] == approx(-8.922500000000314, rel=1e-6, abs_tol=1e-6)
    assert pnl["latest_unrealized"] == approx(-10.077148994763776, rel=1e-6, abs_tol=1e-6)
