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
import pytest


def setup_function(_):
    http._session = None
    scanner_common.LISTING_CACHE = scanner_common.TTLCache(
        maxsize=3, ttl=scanner_common.LISTING_CACHE_TTL
    )


@pytest.mark.parametrize(
    "func_name",
    [
        "fetch_raydium_listings_async",
        "fetch_orca_listings_async",
        "fetch_phoenix_listings_async",
    ],
)
def test_fetch_listings_cached(monkeypatch, func_name):
    calls = {}

    class FakeResp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return {"data": [{"address": "tokbonk"}]}

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

    fn = getattr(scanner_common, func_name)

    result1 = asyncio.run(fn())
    result2 = asyncio.run(fn())

    assert result1 == ["tokbonk"]
    assert result2 == ["tokbonk"]
    assert calls["sessions"] == 1
    assert calls["gets"] == 1
