from tests.golden_pipeline.conftest import BASE58_MINTS, SCENARIO_PAYLOADS, approx


EXPECTED_FILLS = [
    {
        "order_id": "045f185d043f32c54d82af2166f9f0707d6591972fabced678107f8b97785fe8",
        "qty_base": 2569.7211155378486,
        "price_usd": 1.0056817,
        "fees_usd": 1.032,
        "slippage_bps": 16.75,
        "snapshot_hash": "abded387b62174dd99cc6653cbb52c4f5a2ea62fd02e65153c2adde717622fbd",
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "7adc3c8df93b910e5327ba2a8f143ea8214371f51b706ac34f67787e6cdd7219",
        "qty_base": 4289.704708699122,
        "price_usd": 1.00407902,
        "fees_usd": 1.72,
        "slippage_bps": 16.75,
        "snapshot_hash": "c0cc8f83bb275ce9fbc8342249a38288cdbd625bdda5e388fa51ab19f435d485",
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "ee393b8aaf7191ed20ba7a5736704e018eabe5d5e8bb6426745821849855136f",
        "qty_base": 4269.261318506751,
        "price_usd": 1.0088870600000002,
        "fees_usd": 1.72,
        "slippage_bps": 16.75,
        "snapshot_hash": "b0a26be535aed6a1c9e605cfcf3f965495e5593099b649bf573921dec0191f11",
        "ts": 1_700_000_381.425,
    },
]


EXPECTED_PNLS = [
    {
        "order_id": "045f185d043f32c54d82af2166f9f0707d6591972fabced678107f8b97785fe8",
        "snapshot_hash": "abded387b62174dd99cc6653cbb52c4f5a2ea62fd02e65153c2adde717622fbd",
        "realized_usd": -5.353500000000021,
        "unrealized_usd": -9.481500000000047,
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "7adc3c8df93b910e5327ba2a8f143ea8214371f51b706ac34f67787e6cdd7219",
        "snapshot_hash": "c0cc8f83bb275ce9fbc8342249a38288cdbd625bdda5e388fa51ab19f435d485",
        "realized_usd": -8.922500000000372,
        "unrealized_usd": -28.01379760956416,
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "ee393b8aaf7191ed20ba7a5736704e018eabe5d5e8bb6426745821849855136f",
        "snapshot_hash": "b0a26be535aed6a1c9e605cfcf3f965495e5593099b649bf573921dec0191f11",
        "realized_usd": -8.922500000000314,
        "unrealized_usd": -10.077148994763776,
        "ts": 1_700_000_381.425,
    },
]


def test_virtual_trading_results(golden_harness):
    mint_alpha = BASE58_MINTS["alpha"]

    fills = golden_harness.virtual_fills
    assert len(fills) == len(EXPECTED_FILLS)

    for fill, expected in zip(fills, EXPECTED_FILLS):
        assert fill.order_id == expected["order_id"]
        assert fill.mint == mint_alpha
        assert fill.side == "buy"
        assert fill.qty_base == approx(expected["qty_base"])
        assert fill.price_usd == approx(expected["price_usd"])
        assert fill.fees_usd == approx(expected["fees_usd"])
        assert fill.slippage_bps == approx(expected["slippage_bps"])
        assert fill.snapshot_hash == expected["snapshot_hash"]
        assert fill.route == "VIRTUAL"
        assert fill.ts == approx(expected["ts"], rel=1e-9, abs_tol=1e-9)

    pnls = golden_harness.virtual_pnls
    assert len(pnls) == len(EXPECTED_PNLS)

    for pnl, expected in zip(pnls, EXPECTED_PNLS):
        assert pnl.order_id == expected["order_id"]
        assert pnl.mint == mint_alpha
        assert pnl.snapshot_hash == expected["snapshot_hash"]
        assert pnl.realized_usd == approx(expected["realized_usd"])
        assert pnl.unrealized_usd == approx(expected["unrealized_usd"])
        assert pnl.ts == approx(expected["ts"], rel=1e-9, abs_tol=1e-9)

    # Ensure the scenario definition remains in sync with expectations.
    assert SCENARIO_PAYLOADS["orderbook_path"]["max_slippage_bps"] >= max(
        fill.slippage_bps for fill in fills
    )
