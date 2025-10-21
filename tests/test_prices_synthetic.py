from __future__ import annotations

import asyncio

from solhunter_zero import prices


async def _fetch(mints: list[str]) -> dict[str, prices.PriceQuote]:
    return await prices.fetch_price_quotes_async(mints)


def test_synthetic_provider_uses_legacy_env(monkeypatch):
    mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9"
    monkeypatch.setenv("PRICE_PROVIDERS", "synthetic")
    monkeypatch.delenv("SYNTHETIC_PRICE_HINTS", raising=False)
    monkeypatch.setenv("SYNTHETIC_HINTS", f'{{"{mint}": 1.0}}')
    prices._rebuild_provider_tables()
    prices.PRICE_CACHE.clear()
    prices.QUOTE_CACHE.clear()
    prices.BATCH_CACHE.clear()
    result = asyncio.run(_fetch([mint]))
    quote = result[mint]
    assert quote.source == "synthetic"
    assert quote.price_usd == 1.0
