import asyncio
import time
from typing import Dict, List, Tuple

import aiohttp
from aiohttp.client_reqrep import RequestInfo
from yarl import URL

from solhunter_zero import http, prices


SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9"
OBSCURE_MINT = "4k3Dyjzvzp8eMZWUXb6BVmytDVMizjofoifgLv6hSDcP"


def setup_function(_):
    http._session = None
    prices.PRICE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)
    prices.QUOTE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)
    prices.reset_provider_health()


def _make_quote(price: float, source: str, quality: str = "aggregate") -> prices.PriceQuote:
    return prices.PriceQuote(price_usd=price, source=source, asof=prices._now_ms(), quality=quality)


async def _empty_provider(*_args, **_kwargs):
    return {}


def test_fill_rate_resolves_with_multiple_providers(monkeypatch):
    calls: List[Tuple[str, Tuple[str, ...]]] = []

    async def fake_jupiter(session, tokens):
        calls.append(("jupiter", tuple(tokens)))
        return {
            SOL_MINT: _make_quote(24.0, "jupiter"),
            USDC_MINT: _make_quote(1.0, "jupiter"),
        }

    async def fake_dex(session, tokens):
        calls.append(("dexscreener", tuple(tokens)))
        return {OBSCURE_MINT: _make_quote(0.42, "dexscreener")}

    async def fake_pyth(session, tokens):
        calls.append(("pyth", tuple(tokens)))
        return {SOL_MINT: _make_quote(23.5, "pyth", quality="authoritative")}

    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", fake_dex)
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", fake_pyth)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    result = asyncio.run(
        prices.fetch_price_quotes_async([SOL_MINT, USDC_MINT, OBSCURE_MINT])
    )

    assert result[SOL_MINT].source == "pyth"
    assert result[USDC_MINT].source == "jupiter"
    assert result[OBSCURE_MINT].source == "dexscreener"
    assert calls[0][0] == "jupiter"
    assert any(name == "pyth" for name, _ in calls)


def test_birdeye_auth_failure_cooldown_and_recovery(monkeypatch):
    calls: List[Tuple[str, Tuple[str, ...]]] = []

    request_info = RequestInfo(
        url=URL("https://public-api.birdeye.so/defi/multi_price"),
        method="GET",
        headers={},
        real_url=URL("https://public-api.birdeye.so/defi/multi_price"),
    )
    exc = aiohttp.ClientResponseError(
        request_info,
        (),
        status=401,
        message="Unauthorized",
        headers={},
    )

    async def failing_birdeye(session, tokens):
        calls.append(("birdeye_fail", tuple(tokens)))
        raise exc

    async def fallback_jupiter(session, tokens):
        calls.append(("jupiter", tuple(tokens)))
        return {SOL_MINT: _make_quote(22.0, "jupiter")}

    monkeypatch.setenv("BIRDEYE_API_KEY", "bad-key")
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", failing_birdeye)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fallback_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    quotes = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    assert quotes[SOL_MINT].source == "jupiter"
    assert calls[0][0] == "birdeye_fail"
    assert prices.PROVIDER_HEALTH["birdeye"].in_cooldown()

    async def healthy_birdeye(session, tokens):
        calls.append(("birdeye_ok", tuple(tokens)))
        return {SOL_MINT: _make_quote(23.0, "birdeye")}

    monkeypatch.setenv("BIRDEYE_API_KEY", "fresh-key")
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", healthy_birdeye)
    prices.PRICE_CACHE.clear()
    prices.QUOTE_CACHE.clear()

    quotes = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    assert calls[-1][0] == "birdeye_ok"
    assert quotes[SOL_MINT].source == "birdeye"
    assert not prices.PROVIDER_HEALTH["birdeye"].in_cooldown()


def test_birdeye_429_cooldown_skip(monkeypatch):
    calls: List[str] = []
    retry_headers: Dict[str, str] = {"Retry-After": "7"}

    request_info = RequestInfo(
        url=URL("https://public-api.birdeye.so/defi/multi_price"),
        method="GET",
        headers={},
        real_url=URL("https://public-api.birdeye.so/defi/multi_price"),
    )
    rate_exc = aiohttp.ClientResponseError(
        request_info,
        (),
        status=429,
        message="Too Many Requests",
        headers=retry_headers,
    )

    fake_time = [0.0]

    def fake_monotonic():
        return fake_time[0]

    async def flapping_birdeye(session, tokens):
        calls.append("birdeye")
        raise rate_exc

    async def steady_jupiter(session, tokens):
        calls.append("jupiter")
        return {SOL_MINT: _make_quote(20.0, "jupiter")}

    monkeypatch.setenv("BIRDEYE_API_KEY", "rate-key")
    monkeypatch.setattr(prices, "_monotonic", fake_monotonic)
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", flapping_birdeye)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", steady_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    first = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    assert first[SOL_MINT].source == "jupiter"
    assert calls.count("birdeye") == 1

    fake_time[0] = 1.0
    second = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    assert second[SOL_MINT].source == "jupiter"
    assert calls.count("birdeye") == 1


def test_pyth_overwrites_major_tokens(monkeypatch):
    async def fake_jupiter(session, tokens):
        return {SOL_MINT: _make_quote(24.0, "jupiter")}

    async def fake_pyth(session, tokens):
        return {SOL_MINT: _make_quote(25.0, "pyth", quality="authoritative")}

    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", fake_pyth)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    quotes = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    assert quotes[SOL_MINT].source == "pyth"
    assert quotes[SOL_MINT].price_usd == 25.0


def test_cache_hits_skip_network(monkeypatch):
    calls = {"jupiter": 0}

    async def fake_jupiter(session, tokens):
        calls["jupiter"] += 1
        return {SOL_MINT: _make_quote(20.0, "jupiter")}

    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    first = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    second = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))

    assert first[SOL_MINT].price_usd == 20.0
    assert second[SOL_MINT].price_usd == 20.0
    assert calls["jupiter"] == 1


def test_timeout_path_uses_next_provider(monkeypatch):
    async def timeout_jupiter(session, tokens):
        raise asyncio.TimeoutError()

    async def fallback_dex(session, tokens):
        return {SOL_MINT: _make_quote(21.0, "dexscreener")}

    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", timeout_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", fallback_dex)
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    start = time.perf_counter()
    quotes = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    elapsed = time.perf_counter() - start

    assert quotes[SOL_MINT].source == "dexscreener"
    assert elapsed < 1.0
    assert prices.PROVIDER_HEALTH["jupiter"].consecutive_failures >= 1


def test_fetch_token_prices_async_returns_floats(monkeypatch):
    async def fake_jupiter(session, tokens):
        return {SOL_MINT: _make_quote(19.0, "jupiter")}

    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    result = asyncio.run(prices.fetch_token_prices_async([SOL_MINT]))
    assert result == {SOL_MINT: 19.0}
    assert prices.get_cached_price(SOL_MINT) == 19.0
