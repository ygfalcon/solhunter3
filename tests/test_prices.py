
import asyncio
from typing import Any

import aiohttp

from solhunter_zero import http
from solhunter_zero import prices


def setup_function(_):
    http._session = None
    prices.PRICE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)


async def _dummy_session():
    class DummySession:  # pragma: no cover - simple container
        pass

    return DummySession()


async def _empty_provider(*_args, **_kwargs):
    return {}


def test_fetch_token_prices_uses_helius_first(monkeypatch):
    calls: list[str] = []

    async def fake_helius(session, tokens):
        calls.append("helius")
        return {"tok": 2.0}

    async def fake_dex(session, tokens):
        calls.append("dex")
        return {}

    monkeypatch.setattr(prices, "get_session", _dummy_session)
    monkeypatch.setattr(prices, "_fetch_prices_helius", fake_helius)
    monkeypatch.setattr(prices, "_fetch_prices_dexscreener", fake_dex)

    result = prices.fetch_token_prices(["tok"])

    assert result == {"tok": 2.0}
    assert prices.get_cached_price("tok") == 2.0
    assert calls == ["helius"]


def test_fetch_token_prices_dexscreener_fallback(monkeypatch):
    calls: list[str] = []

    async def fake_helius(session, tokens):
        calls.append("helius")
        return {}

    async def fake_dex(session, tokens):
        calls.append("dex")
        return {"tok": 4.0}

    monkeypatch.setattr(prices, "get_session", _dummy_session)
    monkeypatch.setattr(prices, "_fetch_prices_helius", fake_helius)
    monkeypatch.setattr(prices, "_fetch_prices_dexscreener", fake_dex)

    result = prices.fetch_token_prices(["tok"])

    assert result == {"tok": 4.0}
    assert prices.get_cached_price("tok") == 4.0
    assert calls == ["helius", "dex"]


def test_fetch_token_prices_async_error(monkeypatch):
    warnings = []

    async def fake_helius(session, tokens):
        raise aiohttp.ClientError("boom")

    async def fake_dex(session, tokens):
        return {}

    def fake_warning(*args, **kwargs):
        warnings.append((args, kwargs))

    monkeypatch.setattr(prices, "get_session", _dummy_session)
    monkeypatch.setattr(prices, "_fetch_prices_helius", fake_helius)
    monkeypatch.setattr(prices, "_fetch_prices_dexscreener", fake_dex)
    monkeypatch.setattr(prices.logger, "warning", fake_warning)

    result = asyncio.run(prices.fetch_token_prices_async(["tok"]))

    assert result == {}
    assert warnings, "expected warning when providers fail"


def test_price_cache_and_session_reuse(monkeypatch):
    calls = {"helius": 0}

    async def fake_helius(session, tokens):
        calls["helius"] += 1
        return {"tok": 1.0}

    monkeypatch.setattr(prices, "get_session", _dummy_session)
    monkeypatch.setattr(prices, "_fetch_prices_helius", fake_helius)
    monkeypatch.setattr(prices, "_fetch_prices_dexscreener", _empty_provider)

    result1 = prices.fetch_token_prices(["tok"])
    result2 = asyncio.run(prices.fetch_token_prices_async(["tok"]))

    assert result1 == {"tok": 1.0}
    assert result2 == {"tok": 1.0}
    assert calls["helius"] == 1


def test_warm_cache(monkeypatch):
    async def fake_helius(session, tokens):
        return {"tok": 3.0}

    monkeypatch.setattr(prices, "get_session", _dummy_session)
    monkeypatch.setattr(prices, "_fetch_prices_helius", fake_helius)
    monkeypatch.setattr(prices, "_fetch_prices_dexscreener", _empty_provider)

    prices.warm_cache(["tok"])

    assert prices.get_cached_price("tok") == 3.0


def test_helius_market_price_fetch(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_request_json(
        session,
        url,
        provider,
        *,
        params=None,
        headers=None,
        json=None,
        method="GET",
    ):
        captured["params"] = params
        captured["provider"] = provider
        return {"result": {"tok": {"price": "1.23"}}}

    monkeypatch.setattr(prices, "_request_json", fake_request_json)

    async def _run():
        result = await prices._fetch_prices_helius_market(object(), ["tok"])
        assert result == {"tok": 1.23}

    asyncio.run(_run())

    params = captured.get("params")
    assert params is not None
    assert any(key == "ids[]" and value == "tok" for key, value in params)
    assert captured.get("provider") == "Helius (Market)"
