import asyncio
import time

import solhunter_zero.arbitrage as arb
from solhunter_zero.event_bus import publish

async def const_price(_):
    return 1.0

async def high_price(_):
    return 1.2


def test_route_cache_reuse(monkeypatch):
    monkeypatch.setattr(arb, "USE_DEPTH_STREAM", False)
    monkeypatch.setattr(arb, "USE_SERVICE_ROUTE", False)
    monkeypatch.setattr(arb.depth_client, "snapshot", lambda t: ({}, 0.0))

    calls = {"count": 0}

    def slow_route(prices, amount, **_):
        calls["count"] += 1
        time.sleep(0.05)
        names = list(prices.keys())
        return [names[0], names[1]], 0.1

    monkeypatch.setattr(arb, "_best_route", slow_route)
    arb.invalidate_route()

    start = time.perf_counter()
    asyncio.run(
        arb.detect_and_execute_arbitrage("tok", [const_price, high_price], threshold=0.0, amount=1.0)
    )
    first = time.perf_counter() - start
    assert calls["count"] == 1

    start = time.perf_counter()
    asyncio.run(
        arb.detect_and_execute_arbitrage("tok", [const_price, high_price], threshold=0.0, amount=1.0)
    )
    second = time.perf_counter() - start
    assert calls["count"] == 1
    assert second < first


def test_cache_invalidation(monkeypatch):
    monkeypatch.setattr(arb, "USE_DEPTH_STREAM", False)
    monkeypatch.setattr(arb, "USE_SERVICE_ROUTE", False)
    monkeypatch.setattr(arb.depth_client, "snapshot", lambda t: ({}, 0.0))

    calls = {"count": 0}

    def slow_route(prices, amount, **_):
        calls["count"] += 1
        names = list(prices.keys())
        return [names[0], names[1]], 0.1

    monkeypatch.setattr(arb, "_best_route", slow_route)
    arb.invalidate_route()

    asyncio.run(
        arb.detect_and_execute_arbitrage("tok", [const_price, high_price], threshold=0.0, amount=1.0)
    )
    assert calls["count"] == 1

    publish("depth_update", {"tok": {"depth": 2.0}})

    asyncio.run(
        arb.detect_and_execute_arbitrage("tok", [const_price, high_price], threshold=0.0, amount=1.0)
    )
    assert calls["count"] == 2
