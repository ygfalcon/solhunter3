from tests.golden_pipeline.conftest import BASE58_MINTS, SCENARIO_PAYLOADS, approx


EXPECTED_FILLS = [
    {
        "order_id": "ae15944204ffcda40a7bb5fbc3b5b62844ed42cfd787e78e54f85c6801e270ee",
        "qty_base": 2569.7211155378486,
        "price_usd": 1.0056817,
        "fees_usd": 1.032,
        "slippage_bps": 16.75,
        "snapshot_hash": "19c3c76dc1d836df63a8883650ca943ec965bf59fb057aa96417d99730aed2aa",
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "8eb685e7739201917246bbaac6276d643a73d0c9a9d0ec4d38e807f16e074070",
        "qty_base": 4289.704708699122,
        "price_usd": 1.00407902,
        "fees_usd": 1.72,
        "slippage_bps": 16.75,
        "snapshot_hash": "842a1f1710fb0ec196302cf94ccb28a591ced001b46d23c55f6edb145edb515b",
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "4078629059bddc52aa538fcf7320628146211927e2026fd091146e8f232eb46b",
        "qty_base": 4269.261318506751,
        "price_usd": 1.0088870600000002,
        "fees_usd": 1.72,
        "slippage_bps": 16.75,
        "snapshot_hash": "1c296bfbaf73fb81116630271ba34c633a60ef2dbd2d6c47bfccee2d6dd2b93e",
        "ts": 1_700_000_381.425,
    },
]


EXPECTED_PNLS = [
    {
        "order_id": "ae15944204ffcda40a7bb5fbc3b5b62844ed42cfd787e78e54f85c6801e270ee",
        "snapshot_hash": "19c3c76dc1d836df63a8883650ca943ec965bf59fb057aa96417d99730aed2aa",
        "realized_usd": -5.353500000000021,
        "unrealized_usd": -9.481500000000047,
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "8eb685e7739201917246bbaac6276d643a73d0c9a9d0ec4d38e807f16e074070",
        "snapshot_hash": "842a1f1710fb0ec196302cf94ccb28a591ced001b46d23c55f6edb145edb515b",
        "realized_usd": -8.922500000000372,
        "unrealized_usd": -28.01379760956416,
        "ts": 1_700_000_380.755,
    },
    {
        "order_id": "4078629059bddc52aa538fcf7320628146211927e2026fd091146e8f232eb46b",
        "snapshot_hash": "1c296bfbaf73fb81116630271ba34c633a60ef2dbd2d6c47bfccee2d6dd2b93e",
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
