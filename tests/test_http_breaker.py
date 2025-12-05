import asyncio
import importlib

import pytest


def test_shared_http_breaker_opens_and_recovers(monkeypatch):
    monkeypatch.setenv("DISCOVERY_HTTP_BREAKER_FAILS", "1")
    monkeypatch.setenv("DISCOVERY_HTTP_BREAKER_WINDOW", "1.0")
    monkeypatch.setenv("DISCOVERY_HTTP_BREAKER_COOLDOWN", "0.2")

    from solhunter_zero import http as http_mod

    http = importlib.reload(http_mod)

    async def _exercise() -> None:
        async def _fail_once() -> None:
            async with http.provider_request("https://api.test.local"):
                raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await _fail_once()

        state = http.http_breaker_state()
        assert state["open"] is True
        assert state["openings"] >= 1

        with pytest.raises(http.HostCircuitOpenError):
            async with http.provider_request("https://api.test.local"):
                pass

        await asyncio.sleep(1.05)
        assert http.http_breaker_state()["open"] is False

        async with http.provider_request("https://api.test.local"):
            pass

        final_state = http.http_breaker_state()
        assert final_state["failure_count"] == 0

    asyncio.run(_exercise())

