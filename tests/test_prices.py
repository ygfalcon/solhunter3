import asyncio
import time
from typing import Dict, List, Tuple

from solhunter_zero import http, prices


SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9"
OBSCURE_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"


def setup_function(_):
    http._session = None
    prices.PRICE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)
    prices.QUOTE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)
    prices.BATCH_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)
    prices.reset_provider_health()


def _make_quote(
    price: float,
    source: str,
    *,
    quality: str = "aggregate",
    degraded: bool = False,
    staleness_ms: float | None = None,
    confidence: float | None = None,
) -> prices.PriceQuote:
    return prices.PriceQuote(
        price_usd=price,
        source=source,
        asof=prices._now_ms(),
        quality=quality,
        degraded=degraded,
        staleness_ms=staleness_ms,
        confidence=confidence,
    )


async def _empty_provider(*_args, **_kwargs):
    return {}


def test_invalid_tokens_are_filtered(monkeypatch):
    calls: List[Tuple[str, Tuple[str, ...]]] = []

    async def fake_jupiter(session, tokens):
        calls.append(("jupiter", tuple(tokens)))
        return {SOL_MINT: _make_quote(24.0, "jupiter")}

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    quotes = asyncio.run(prices.fetch_price_quotes_async([" ", "invalid", SOL_MINT, SOL_MINT]))

    assert SOL_MINT in quotes
    assert calls == [("jupiter", (SOL_MINT,))]


def test_cascade_prefers_jupiter_then_pyth_then_dex(monkeypatch):
    calls: List[Tuple[str, Tuple[str, ...]]] = []

    async def fake_jupiter(session, tokens):
        calls.append(("jupiter", tuple(tokens)))
        # Jupiter only serves the stable coin to force fallbacks.
        return {USDC_MINT: _make_quote(1.0, "jupiter")}

    async def fake_pyth(session, tokens):
        calls.append(("pyth", tuple(tokens)))
        return {
            SOL_MINT: _make_quote(
                23.5,
                "pyth",
                quality="authoritative",
                degraded=True,
                staleness_ms=250.0,
                confidence=0.02,
            )
        }

    async def fake_dex(session, tokens):
        calls.append(("dexscreener", tuple(tokens)))
        return {
            OBSCURE_MINT: _make_quote(
                0.42,
                "dexscreener",
                degraded=True,
            )
        }

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", fake_pyth)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", fake_dex)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    result = asyncio.run(
        prices.fetch_price_quotes_async([SOL_MINT, USDC_MINT, OBSCURE_MINT])
    )

    assert result[USDC_MINT].source == "jupiter"
    assert not result[USDC_MINT].degraded
    assert result[SOL_MINT].source == "pyth"
    assert result[SOL_MINT].degraded is True
    assert result[OBSCURE_MINT].source == "dexscreener"
    assert result[OBSCURE_MINT].degraded is True
    assert [name for name, _ in calls] == ["jupiter", "pyth", "dexscreener"]


def test_pyth_fallback_marks_staleness(monkeypatch):
    async def fake_jupiter(session, tokens):
        return {}

    async def fake_pyth(session, tokens):
        return {
            SOL_MINT: _make_quote(
                25.0,
                "pyth",
                quality="authoritative",
                degraded=True,
                staleness_ms=1000.0,
                confidence=0.05,
            )
        }

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", fake_pyth)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    quotes = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    quote = quotes[SOL_MINT]
    assert quote.source == "pyth"
    assert quote.degraded is True
    assert quote.staleness_ms == 1000.0
    assert quote.confidence == 0.05


def test_jupiter_success_blocks_pyth_override(monkeypatch):
    async def fake_jupiter(session, tokens):
        return {SOL_MINT: _make_quote(24.0, "jupiter")}

    async def fake_pyth(session, tokens):
        return {
            SOL_MINT: _make_quote(
                25.0,
                "pyth",
                quality="authoritative",
                degraded=True,
            )
        }

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", fake_pyth)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    quotes = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    assert quotes[SOL_MINT].source == "jupiter"
    assert quotes[SOL_MINT].price_usd == 24.0
    assert quotes[SOL_MINT].degraded is False


def test_cache_hits_skip_network(monkeypatch):
    calls = {"jupiter": 0}

    async def fake_jupiter(session, tokens):
        calls["jupiter"] += 1
        return {SOL_MINT: _make_quote(20.0, "jupiter")}

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    first = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))
    second = asyncio.run(prices.fetch_price_quotes_async([SOL_MINT]))

    assert first[SOL_MINT].price_usd == 20.0
    assert second[SOL_MINT].price_usd == 20.0
    assert calls["jupiter"] == 1


def test_batch_cache_ignores_token_order(monkeypatch):
    calls = {"jupiter": 0}

    async def fake_jupiter(session, tokens):
        calls["jupiter"] += 1
        return {
            SOL_MINT: _make_quote(20.0, "jupiter"),
            USDC_MINT: _make_quote(1.0, "jupiter"),
        }

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    asyncio.run(prices.fetch_price_quotes_async([SOL_MINT, USDC_MINT]))
    prices.PRICE_CACHE.clear()
    prices.QUOTE_CACHE.clear()

    quotes = asyncio.run(prices.fetch_price_quotes_async([USDC_MINT, SOL_MINT]))

    assert quotes[USDC_MINT].source == "jupiter"
    assert quotes[SOL_MINT].source == "jupiter"
    assert calls["jupiter"] == 1


def test_timeout_path_uses_next_provider(monkeypatch):
    async def timeout_jupiter(session, tokens):
        raise asyncio.TimeoutError()

    async def fallback_dex(session, tokens):
        return {SOL_MINT: _make_quote(21.0, "dexscreener")}

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", timeout_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", fallback_dex)
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

    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", fake_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)

    result = asyncio.run(prices.fetch_token_prices_async([SOL_MINT]))
    assert result == {SOL_MINT: 19.0}
    assert prices.get_cached_price(SOL_MINT) == 19.0
