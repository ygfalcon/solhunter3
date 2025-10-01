import asyncio
import importlib
import sys
import types

if importlib.util.find_spec("solana") is None:
    sys.modules.setdefault("solana", types.ModuleType("solana"))
    sys.modules["solana.rpc"] = types.ModuleType("rpc")
    sys.modules["solana.rpc.api"] = types.SimpleNamespace(Client=object)
    sys.modules["solana.rpc.async_api"] = types.SimpleNamespace(AsyncClient=object)

from solhunter_zero import scanner_common, http


def setup_function(_):
    http._session = None
    scanner_common.TREND_CACHE = scanner_common.TTLCache(
        maxsize=1, ttl=scanner_common.TREND_CACHE_TTL
    )


def test_fetch_trending_tokens_cached(monkeypatch):
    calls = {}

    class FakeResp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return {"trending": [{"address": "tokbonk"}]}

        def raise_for_status(self):
            pass

    class FakeSession:
        def __init__(self):
            calls["sessions"] = calls.get("sessions", 0) + 1

        def get(self, url, timeout=10):
            calls["url"] = url
            calls["gets"] = calls.get("gets", 0) + 1
            return FakeResp()

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())

    result1 = asyncio.run(scanner_common.fetch_trending_tokens_async())
    result2 = asyncio.run(scanner_common.fetch_trending_tokens_async())

    assert result1 == ["tokbonk"]
    assert result2 == ["tokbonk"]
    assert calls["sessions"] == 1
    assert calls["gets"] == 1
