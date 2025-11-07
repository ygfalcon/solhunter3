import types

import pytest

from datetime import datetime, timezone
import types

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_collect_status_handles_depth_proc_without_poll():
    runtime = trading_runtime.TradingRuntime()
    runtime.depth_proc = types.SimpleNamespace()

    runtime._collect_rl_status = lambda: {}
    runtime._collect_iteration = lambda: {}
    runtime.activity.snapshot = lambda: []

    status = runtime._collect_status()

    assert status["depth_service"] is True


def test_collect_discovery_includes_backoff(monkeypatch):
    runtime = trading_runtime.TradingRuntime()
    runtime._use_new_pipeline = True

    now = 1_000.0
    cooldown = 30.0
    cooldown_until = now + cooldown
    last_fetch_ts = now - 5.0

    class DummyPipeline:
        @staticmethod
        def discovery_snapshot() -> dict:
            return {
                "current_backoff": 12.5,
                "cooldown_until": cooldown_until,
                "cooldown_remaining": cooldown,
                "cooldown_active": True,
                "consecutive_empty": 2,
                "last_fetch_ts": last_fetch_ts,
                "last_fetch_count": 0,
                "last_fetch_empty": True,
            }

    runtime.pipeline = DummyPipeline()

    with runtime._discovery_lock:
        runtime._recent_tokens.clear()
        runtime._recent_tokens.appendleft("AAA")

    with runtime._iteration_lock:
        runtime._last_iteration = {"tokens_used": ["BBB"]}

    monkeypatch.setattr(trading_runtime.time, "time", lambda: now)

    discovery = runtime._collect_discovery()

    assert discovery["backoff_seconds"] == pytest.approx(12.5)
    assert discovery["cooldown_remaining"] == pytest.approx(cooldown)
    assert discovery["cooldown_until"] == pytest.approx(cooldown_until)
    assert discovery["cooldown_active"] is True
    assert discovery["consecutive_empty_fetches"] == 2
    assert discovery["last_fetch_count"] == 0
    assert discovery["last_fetch_empty"] is True
    expected_expiry = (
        datetime.fromtimestamp(cooldown_until, timezone.utc).isoformat().replace("+00:00", "Z")
    )
    expected_last_fetch = (
        datetime.fromtimestamp(last_fetch_ts, timezone.utc).isoformat().replace("+00:00", "Z")
    )
    assert discovery["cooldown_expires_at"] == expected_expiry
    assert discovery["last_fetch_at"] == expected_last_fetch
