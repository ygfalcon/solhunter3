import asyncio

import pytest

from solhunter_zero import main as main_module
from solhunter_zero import prices


def test_warm_price_cache_skips_cached_tokens(monkeypatch):
    prices.PRICE_CACHE.clear()
    prices.update_price_cache("cached", 5.0)

    fetched = []

    async def fake_fetch(tokens):
        fetched.append(tuple(tokens))
        return {tok: 1.0 for tok in tokens}

    monkeypatch.setattr(main_module.prices, "fetch_token_prices_async", fake_fetch)

    sleep_calls: list[float] = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    asyncio.run(
        main_module._warm_price_cache_background(
            ["cached", "missing"], delay=0.1, batch_size=1
        )
    )

    assert fetched == [("missing",)]
    assert sleep_calls == []


def test_warm_price_cache_throttle(monkeypatch):
    prices.PRICE_CACHE.clear()

    fetched = []

    async def fake_fetch(tokens):
        fetched.append(tuple(tokens))
        return {tok: 1.0 for tok in tokens}

    monkeypatch.setattr(main_module.prices, "fetch_token_prices_async", fake_fetch)

    sleep_calls: list[float] = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(main_module.asyncio, "sleep", fake_sleep)

    asyncio.run(
        main_module._warm_price_cache_background(
            ["a", "b", "c"], delay=0.2, batch_size=1
        )
    )

    assert fetched == [("a",), ("b",), ("c",)]
    assert sleep_calls == [0.2, 0.2]
