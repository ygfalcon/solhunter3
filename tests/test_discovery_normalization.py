import logging
import time

import pytest

from solhunter_zero import event_bus
from solhunter_zero.event_bus import _normalize_discovery_entries


VALID_MINT_A = "a" * 32
VALID_MINT_B = "b" * 32
VALID_MINT_C = "c" * 32


def test_normalize_discovery_entries_dedupes_tokens_by_score():
    payload = {
        "tokens": [
            {"mint": VALID_MINT_A, "score": 1.0},
            {"mint": VALID_MINT_A, "score": 3.5},
            {"mint": VALID_MINT_B, "score": 2.0},
        ]
    }

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 2
    entry_map = {e["mint"]: e for e in entries}
    assert entry_map[VALID_MINT_A]["score"] == 3.5
    assert entry_map[VALID_MINT_B]["score"] == 2.0


def test_normalize_discovery_entries_prefers_latest_timestamp():
    payload = {
        "entries": [
            {"mint": VALID_MINT_C, "ts": 10},
            {"mint": VALID_MINT_C, "ts": 20},
        ]
    }

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    assert entries[0]["ts"] == 20


def test_normalize_discovery_entries_dedupes_root_payloads():
    payload = [
        {"mint": VALID_MINT_A, "score": 2.0, "ts": 5},
        {"mint": VALID_MINT_A, "score": 2.0, "ts": 15},
        {"mint": VALID_MINT_A, "score": 1.0, "ts": 25},
    ]

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    assert entries[0]["score"] == 2.0
    assert entries[0]["ts"] == 15


def test_normalize_discovery_entries_filters_invalid_mints(caplog):
    payload = {
        "tokens": [
            {"mint": "invalid!!"},
            {"mint": VALID_MINT_A, "score": 2.0},
        ]
    }

    with caplog.at_level(logging.WARNING):
        entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    assert entries[0]["mint"] == VALID_MINT_A
    assert entries[0]["score"] == 2.0
    assert isinstance(entries[0]["ts"], float)
    assert any("invalid mint" in record.message for record in caplog.records)


def test_normalize_discovery_entries_filters_empty_like_values(caplog):
    payload = [
        "   ",
        "",
        None,
        {"mint": "\n\t"},
        VALID_MINT_B,
    ]

    with caplog.at_level(logging.WARNING):
        entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    assert entries[0]["mint"] == VALID_MINT_B
    assert isinstance(entries[0]["ts"], float)
    assert any("empty mint" in record.message for record in caplog.records)


def test_normalize_discovery_entries_injects_fallback_timestamp(monkeypatch):
    fixed_now = 1234.5
    monkeypatch.setattr(time, "time", lambda: fixed_now)

    payload = {
        "tokens": [
            {"mint": VALID_MINT_A, "score": 1.0},
            {"mint": VALID_MINT_B},
        ]
    }

    entries = _normalize_discovery_entries(payload)

    assert all(entry["ts"] == fixed_now for entry in entries)
    assert all(entry.get("attributes", {}).get("ts_source") == "fallback_now" for entry in entries)


def test_normalize_discovery_entries_preserves_attributes_with_fallback(monkeypatch):
    fixed_now = 9876.5
    monkeypatch.setattr(event_bus.time, "time", lambda: fixed_now)

    payload = {
        "entries": [
            {
                "mint": VALID_MINT_C,
                "attributes": {"note": "from scanner"},
            },
        ]
    }

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    entry = entries[0]
    assert entry["ts"] == fixed_now
    assert entry["attributes"]["note"] == "from scanner"
    assert entry["attributes"]["ts_source"] == "fallback_now"


def test_normalize_discovery_entries_retains_tags_by_source():
    payload = {
        "source": "scanner",
        "tags": ["root", "shared"],
        "entries": [
            {
                "mint": VALID_MINT_A,
                "source": "child",
                "tags": ["child", "shared"],
                "tags_by_source": {"deep": ["nested"]},
            }
        ],
    }

    entries = _normalize_discovery_entries(payload)

    assert len(entries) == 1
    entry = entries[0]
    assert set(entry["tags"]) == {"root", "shared", "child", "nested"}
    assert entry["tags_by_source"]["scanner"] == ["root", "shared"]
    assert entry["tags_by_source"]["child"] == ["child", "shared"]
    assert entry["tags_by_source"]["deep"] == ["nested"]

