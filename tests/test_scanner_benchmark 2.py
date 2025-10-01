import asyncio
import time

import solhunter_zero.token_scanner as scanner
from solhunter_zero import resource_monitor as rm


def _fake_task():
    async def _inner(*_a, **_k):
        await asyncio.sleep(0.05)
        return ["x"]
    return _inner


def test_scan_tokens_async_speed(monkeypatch):
    monkeypatch.setattr(scanner.TokenScanner, "scan", lambda *a, **k: _fake_task()())
    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", _fake_task())
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", _fake_task())
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", _fake_task())
    monkeypatch.setattr(scanner, "_fetch_dex_ws_tokens", _fake_task())
    monkeypatch.setattr(scanner, "DEX_LISTING_WS_URL", "ws://dex")
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 2)
    monkeypatch.setattr(rm, "get_cpu_usage", lambda: 0.0)

    start = time.perf_counter()
    asyncio.run(scanner.scan_tokens_async(max_concurrency=2))
    duration = time.perf_counter() - start

    assert duration < 0.2
