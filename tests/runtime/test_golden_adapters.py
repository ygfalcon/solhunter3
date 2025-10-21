import pytest

from solhunter_zero.runtime import golden_adapters


def test_normalize_ohlcv_payload_handles_flat_and_nested(monkeypatch):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(golden_adapters, "publish", lambda topic, payload: calls.append((topic, payload)))

    flat_payload = {
        "close": "1.23",
        "volume": "456.0",
        "buyers": "7",
        "sellers": "3",
        "schema_version": "2.0",
        "content_hash": "hash",
    }
    nested_payload = {
        "c": "1.23",
        "vol_usd": "456.0",
        "buyers": "7",
        "sellers": "3",
        "content_hash": "hash",
    }

    flat = golden_adapters.normalize_ohlcv_payload(flat_payload, reader="test")
    nested = golden_adapters.normalize_ohlcv_payload(nested_payload, reader="test")

    assert flat.close == pytest.approx(1.23)
    assert flat.volume_usd == pytest.approx(456.0)
    assert nested.close == pytest.approx(1.23)
    assert nested.volume_usd == pytest.approx(456.0)
    assert flat.buyers == nested.buyers == 7
    assert flat.sellers == nested.sellers == 3
    assert nested.schema_version is None
    assert flat.schema_version == "2.0"
    assert nested.content_hash == flat.content_hash == "hash"
    assert any(topic == "schema_mismatch_total" for topic, _ in calls)


def test_normalize_golden_snapshot_handles_flat_and_nested(monkeypatch):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(golden_adapters, "publish", lambda topic, payload: calls.append((topic, payload)))

    flat_payload = {
        "px_mid_usd": "1.5",
        "liq_depth_1pct_usd": "2000",
        "schema_version": "2.0",
        "content_hash": "hash",
    }
    nested_payload = {
        "px": {"mid_usd": "1.5"},
        "liq": {"depth_pct": {"1": "2000"}},
        "content_hash": "hash",
    }

    flat = golden_adapters.normalize_golden_snapshot(flat_payload, reader="test")
    nested = golden_adapters.normalize_golden_snapshot(nested_payload, reader="test")

    assert flat.mid_usd == pytest.approx(1.5)
    assert flat.depth_1pct_usd == pytest.approx(2000.0)
    assert nested.mid_usd == pytest.approx(1.5)
    assert nested.depth_1pct_usd == pytest.approx(2000.0)
    assert flat.schema_version == "2.0"
    assert nested.schema_version is None
    assert flat.content_hash == nested.content_hash == "hash"
    assert any("fallback_px_mid_usd" in payload.get("issues", []) for _, payload in calls)
