import asyncio
from typing import Any, Mapping, Sequence

from solhunter_zero import dex_scanner, token_scanner as scanner, scanner_common
from solhunter_zero.event_bus import subscribe


class FakeAsyncClient:
    def __init__(self, url):
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get_program_accounts(self, program_id, encoding="jsonParsed"):
        assert encoding == "jsonParsed"
        assert program_id == dex_scanner.DEX_PROGRAM_ID
        return {
            "result": [
                {"account": {"data": {"parsed": {"info": {"tokenA": {"mint": "abcbonk"}, "tokenB": {"mint": "x"}}}}}},
                {"account": {"data": {"parsed": {"info": {"tokenA": {"mint": "y"}, "tokenB": {"mint": "zzzBONK"}}}}}},
            ]
        }


def test_scan_new_pools(monkeypatch):
    captured = {}

    def fake_client(url):
        captured["url"] = url
        return FakeAsyncClient(url)

    monkeypatch.setattr(dex_scanner, "AsyncClient", fake_client)
    tokens = asyncio.run(dex_scanner.scan_new_pools("http://node"))
    assert captured["url"] == "http://node"
    assert tokens == ["abcbonk", "zzzBONK"]


def test_scanner_method_pools(monkeypatch):
    monkeypatch.setattr(dex_scanner, "scan_new_pools_sync", lambda url: ["tokbonk"])
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: (_ for _ in ()).throw(AssertionError("birdeye")))
    async def fake_trend():
        return []
    async def fr():
        return []
    async def fo():
        return []
    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", fake_trend)
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", fr)
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", fo)
    scanner_common.SOLANA_RPC_URL = "http://node"
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(scanner.scan_tokens(method="pools"))
    unsub()
    assert tokens == ["tokbonk"]
    assert _event_mints(events) == [tokens]


def test_scanner_async_method_pools(monkeypatch):
    async def fake_scan(url):
        return ["tokbonk"]

    monkeypatch.setattr(dex_scanner, "scan_new_pools", fake_scan)
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: (_ for _ in ()).throw(AssertionError("birdeye")))
    async def fake_trend():
        return []
    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", fake_trend)
    async def fr():
        return []
    async def fo():
        return []
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", fr)
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", fo)
    scanner_common.SOLANA_RPC_URL = "http://node"
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    result = asyncio.run(scanner.scan_tokens_async(method="pools"))
    unsub()
    assert result == ["tokbonk"]
    assert events == [result]


def _event_mints(events: Sequence[Sequence[Any]]) -> list[list[str]]:
    batches: list[list[str]] = []
    for batch in events:
        row: list[str] = []
        for item in batch:
            if isinstance(item, Mapping):
                for key in ("mint", "token", "address"):
                    value = item.get(key)
                    if isinstance(value, str) and value:
                        row.append(value)
                        break
                else:
                    raw = item.get("mint")
                    if isinstance(raw, str) and raw:
                        row.append(raw)
            else:
                text = str(item)
                if text:
                    row.append(text)
        batches.append(row)
    return batches
