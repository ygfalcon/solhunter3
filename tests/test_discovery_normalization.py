import pytest

from solhunter_zero.event_bus import _normalize_discovery_entries


def test_normalize_discovery_entries_dedupes_tokens_by_score():
    payload = {
        "tokens": [
            {"mint": "MintA", "score": 1.0},
            {"mint": "MintA", "score": 3.5},
            {"mint": "MintB", "score": 2.0},
        ]
    }

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 2
    entry_map = {e["mint"]: e for e in entries}
    assert entry_map["MintA"]["score"] == 3.5
    assert entry_map["MintB"]["score"] == 2.0


def test_normalize_discovery_entries_prefers_latest_timestamp():
    payload = {
        "entries": [
            {"mint": "MintC", "ts": 10},
            {"mint": "MintC", "ts": 20},
        ]
    }

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    assert entries[0]["ts"] == 20


def test_normalize_discovery_entries_dedupes_root_payloads():
    payload = [
        {"mint": "MintD", "score": 2.0, "ts": 5},
        {"mint": "MintD", "score": 2.0, "ts": 15},
        {"mint": "MintD", "score": 1.0, "ts": 25},
    ]

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    assert entries[0]["score"] == 2.0
    assert entries[0]["ts"] == 15

