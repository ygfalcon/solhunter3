import json

import pytest

from solhunter_zero.golden_pipeline.types import GOLDEN_SNAPSHOT_SCHEMA_VERSION

from .conftest import BASE58_MINTS, SCENARIO_PAYLOADS, STREAMS


def test_end_to_end_golden_snapshot_contract(golden_harness, fake_broker) -> None:
    events = fake_broker.events[STREAMS.golden_snapshot]
    assert events, "expected at least one golden snapshot"

    snapshot = events[-1]
    assert snapshot["schema_version"] == GOLDEN_SNAPSHOT_SCHEMA_VERSION
    assert snapshot["mint"] == BASE58_MINTS["alpha"]

    required_keys = {
        "mint",
        "asof",
        "meta",
        "px",
        "liq",
        "ohlcv5m",
        "hash",
        "content_hash",
        "idempotency_key",
        "schema_version",
        "px_mid_usd",
        "liq_depth_1pct_usd",
    }
    missing = required_keys.difference(snapshot)
    assert not missing, f"missing keys: {sorted(missing)}"

    px = snapshot["px"]
    for key in ("mid_usd", "bid_usd", "ask_usd", "spread_bps", "ts"):
        assert key in px, f"px missing {key}"

    assert snapshot["px_mid_usd"] == pytest.approx(px["mid_usd"])
    liq = snapshot["liq"]
    assert "depth_usd_by_pct" in liq
    depth_map = liq["depth_usd_by_pct"]
    assert isinstance(depth_map, dict)
    assert "1" in depth_map or 1 in depth_map
    depth_1 = None
    for key in ("1", 1, "1.0"):
        if key in depth_map:
            depth_1 = float(depth_map[key])
            break
    assert depth_1 is not None
    assert snapshot["liq_depth_1pct_usd"] == pytest.approx(depth_1)

    meta = snapshot["meta"]
    expected_meta = SCENARIO_PAYLOADS["metadata"][snapshot["mint"]]
    assert meta["symbol"] == expected_meta["symbol"]
    assert meta["decimals"] == expected_meta["decimals"]

    assert snapshot["content_hash"]
    assert snapshot["idempotency_key"]

    json.dumps(snapshot)
