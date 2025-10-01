"""Integration tests for the lightweight startup gates."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def test_start_all_respects_ui_selftest_exit_code(monkeypatch):
    import start_all

    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("start_all.ui_selftest", return_value=2):
        with pytest.raises(SystemExit) as exc:
            start_all.main()
        assert exc.value.code == 2


def test_start_all_blocks_when_rl_unhealthy(monkeypatch):
    """RL daemon is mandatory: unhealthy RL must block startup."""

    import start_all

    # Bypass preflight success
    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")  # don't require Redis in this unit test
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")  # don't poll UI in this unit test
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("start_all.ui_selftest", return_value=0):
        # Prevent long waits: force wait_for to return unhealthy quickly
        with patch("start_all.wait_for", return_value=(False, "down")):
            with pytest.raises(SystemExit) as exc:
                start_all.main()
            # Any non-zero code is fine; message should mention RL gate
            assert "RL daemon gate failed" in str(exc.value)


def test_start_all_allows_when_rl_healthy(monkeypatch):
    """Healthy RL gate must pass."""

    import start_all

    # Bypass preflight success
    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("start_all.ui_selftest", return_value=0):
        # Make RL gate healthy; also don't block on other waits
        with patch("start_all.wait_for", return_value=(True, "ok")):
            try:
                start_all.main()
            except SystemExit as e:
                # If later gates exit, ensure it's NOT the RL gate
                assert "RL daemon gate failed" not in str(e)

