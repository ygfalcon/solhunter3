from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

from solhunter_zero import scanner_onchain


FIXTURE_PATH = Path(__file__).parent / "data" / "das_cursor_replay.json"


def _reset_discovery_state() -> None:
    scanner_onchain._DISCOVERY_STATE_MEMORY.clear()
    scanner_onchain._SEEN_LOCAL_CACHE.clear()
    scanner_onchain._DISCOVERY_HEALTH.last_success_ts = None
    scanner_onchain._DISCOVERY_HEALTH.last_error = None
    scanner_onchain._DISCOVERY_HEALTH.last_error_ts = None
    scanner_onchain._DISCOVERY_HEALTH.last_cursor = None
    scanner_onchain._DISCOVERY_HEALTH.breaker_open_until = 0.0
    scanner_onchain._DISCOVERY_HEALTH.consecutive_failures = 0
    scanner_onchain._DISCOVERY_BUDGET = scanner_onchain._DiscoveryBudget(
        scanner_onchain._DISCOVERY_REQUESTS_PER_MIN
    )


def test_replay_cursor_checkpoint_and_fallback_dedup(monkeypatch):
    fixture = json.loads(FIXTURE_PATH.read_text())

    _reset_discovery_state()

    now = time.monotonic()
    cursor_expires = now + scanner_onchain._DISCOVERY_RECENT_TTL
    scanner_onchain._DISCOVERY_STATE_MEMORY["discovery:cursor"] = (
        fixture["cursor"],
        cursor_expires,
    )
    for mint in fixture["seen_mints"]:
        scanner_onchain._DISCOVERY_STATE_MEMORY[f"discovery:seen:{mint}"] = (
            "1",
            cursor_expires,
        )

    monkeypatch.setenv("ONCHAIN_USE_DAS", "1")
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_MIN_ACCOUNTS", 1)
    monkeypatch.setattr(scanner_onchain, "MAX_PROGRAM_SCAN_ACCOUNTS", 5)
    monkeypatch.setattr(scanner_onchain, "_ONCHAIN_SCAN_OVERFETCH", 1.0)
    monkeypatch.setattr(scanner_onchain, "_DISCOVERY_OVERFETCH_MULT", 1.0)

    session_calls: list[str] = []

    async def fake_get_session():
        session_calls.append("connect")
        return object()

    async def failing_search(session, cursor=None, limit=None):  # noqa: ARG001
        assert cursor == int(fixture["cursor"])
        raise RuntimeError("session reset")

    gpa_calls: list[int] = []

    async def fake_program_scan(*args, **kwargs):  # noqa: ARG001
        gpa_calls.append(1)
        return fixture["gpa_result"], False

    monkeypatch.setattr(scanner_onchain, "get_session", fake_get_session)
    monkeypatch.setattr(scanner_onchain, "search_fungible_recent", failing_search)
    monkeypatch.setattr(scanner_onchain, "_perform_program_scan", fake_program_scan)

    results = asyncio.run(scanner_onchain.scan_tokens_onchain("http://node"))

    assert session_calls == ["connect"]
    assert len(gpa_calls) >= 1
    assert results == ["9xzVnKqD9L6s7YQDhCjTiMQuuLHfXEGuVYBnNvKcJste"]
    stored_cursor = scanner_onchain._DISCOVERY_STATE_MEMORY.get("discovery:cursor")
    assert stored_cursor is not None and stored_cursor[0] == fixture["cursor"]
