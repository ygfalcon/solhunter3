import asyncio
import time

from solhunter_zero import token_scanner


def test_scan_tokens_async_reuses_cache_during_cooldown(monkeypatch):
    # Reset module state to predictable defaults
    monkeypatch.setattr(token_scanner, "_LAST_TRENDING_RESULT", {"mints": [], "metadata": {}, "timestamp": 0.0})
    monkeypatch.setattr(token_scanner, "_FAILURE_COUNT", 0)
    monkeypatch.setattr(token_scanner, "_COOLDOWN_UNTIL", 0.0)
    token_scanner.TRENDING_METADATA.clear()

    # Trigger cooldown quickly for the test scenario
    monkeypatch.setattr(token_scanner, "_FAILURE_THRESHOLD", 1)
    monkeypatch.setattr(token_scanner, "_FAILURE_COOLDOWN", 60.0)

    success_calls = {"helius": 0, "birdeye": 0}

    async def helius_success(session, *, limit):
        success_calls["helius"] += 1
        return [
            {
                "address": "mint-success",
                "rank": 0,
                "source": "helius",
                "sources": ["helius"],
            }
        ]

    async def birdeye_skip(session, api_key, *, limit, offset):
        success_calls["birdeye"] += 1
        return []

    monkeypatch.setattr(token_scanner, "_helius_trending", helius_success)
    monkeypatch.setattr(token_scanner, "_birdeye_trending", birdeye_skip)

    first = asyncio.run(token_scanner.scan_tokens_async(limit=1))
    assert first == ["mint-success"]
    assert success_calls == {"helius": 1, "birdeye": 0}
    assert token_scanner._FAILURE_COUNT == 0
    assert token_scanner._COOLDOWN_UNTIL == 0.0

    failure_calls = {"helius": 0, "birdeye": 0}

    async def helius_failure(session, *, limit):
        failure_calls["helius"] += 1
        return []

    async def birdeye_failure(session, api_key, *, limit, offset):
        failure_calls["birdeye"] += 1
        return []

    monkeypatch.setattr(token_scanner, "_helius_trending", helius_failure)
    monkeypatch.setattr(token_scanner, "_birdeye_trending", birdeye_failure)

    second = asyncio.run(token_scanner.scan_tokens_async(limit=1))
    assert second == ["mint-success"]  # cached result is reused
    assert failure_calls == {"helius": 1, "birdeye": 1}
    assert token_scanner._FAILURE_COUNT == 1
    assert token_scanner._COOLDOWN_UNTIL > time.time()

    async def helius_should_not_run(*args, **kwargs):  # pragma: no cover - defensive guard
        raise AssertionError("helius_trending should not be called during cooldown")

    async def birdeye_should_not_run(*args, **kwargs):  # pragma: no cover - defensive guard
        raise AssertionError("birdeye_trending should not be called during cooldown")

    monkeypatch.setattr(token_scanner, "_helius_trending", helius_should_not_run)
    monkeypatch.setattr(token_scanner, "_birdeye_trending", birdeye_should_not_run)

    third = asyncio.run(token_scanner.scan_tokens_async(limit=1))
    assert third == ["mint-success"]
    assert token_scanner.TRENDING_METADATA["mint-success"]["sources"] == ["helius"]
    # Timestamp should refresh when cached values are reused
    assert token_scanner._LAST_TRENDING_RESULT["timestamp"] >= token_scanner._COOLDOWN_UNTIL - token_scanner._FAILURE_COOLDOWN
