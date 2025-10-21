import asyncio
import types

import pytest

from aiohttp import ClientTimeout

from solhunter_zero import token_discovery as td


@pytest.mark.asyncio
async def test_discover_candidates_prioritises_scores(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    bird1 = "So11111111111111111111111111111111111111112"
    bird2 = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"

    class FakeResp:
        status = 200

        def __init__(self, payload):
            self._payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, *, params=None, headers=None, timeout=None):
            self.calls.append(url)
            payload = {
                "data": {
                    "tokens": [
                        {
                            "address": bird1,
                            "name": "Bird One",
                            "symbol": "B1",
                            "v24hUSD": 200000,
                            "liquidity": 300000,
                            "price": 1.2,
                            "v24hChangePercent": 5.0,
                        },
                        {
                            "address": bird2,
                            "name": "Bird Two",
                            "symbol": "B2",
                            "v24hUSD": 80000,
                            "liquidity": 90000,
                            "price": 0.8,
                            "v24hChangePercent": -2.0,
                        },
                    ],
                    "total": 2,
                }
            }
            return FakeResp(payload)

    fake_session = FakeSession()

    async def get_session():
        return fake_session

    monkeypatch.setattr(td, "get_session", get_session)
    monkeypatch.setattr(td, "fetch_trending_tokens_async", lambda: [bird2, "trend_only"])

    async def _no_tokens(*, session=None):
        _ = session
        return []

    async def _noop_enrich(_candidates, *, addresses=None):
        _ = addresses
        return None

    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", _no_tokens)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", _no_tokens)
    monkeypatch.setattr(td, "_fetch_dexlab_tokens", _no_tokens)
    monkeypatch.setattr(td, "_enrich_with_solscan", _noop_enrich)

    mem_mint = "E7vCh2szgdWzxubAEANe1yoyWP7JfVv5sWpQXXAUP8Av"

    async def fake_mempool(_rpc_url, threshold):
        _ = threshold
        yield {
            "address": mem_mint,
            "score": 2.0,
            "volume": 15000,
                "liquidity": 40000,
                "momentum": 0.2,
            }

    async def mempool_gen(rpc_url, threshold):
        agen = fake_mempool(rpc_url, threshold)
        async for item in agen:
            yield item

    monkeypatch.setattr(td, "stream_ranked_mempool_tokens_with_depth", mempool_gen)

    results = await td.discover_candidates("https://rpc", limit=3, mempool_threshold=0.0)
    addresses = [r["address"] for r in results]

    assert len(results) <= 3
    assert addresses[0] == mem_mint
    assert set(addresses) >= {bird1, bird2, mem_mint}
    assert results[0]["sources"] == ["mempool"]
    assert any("birdeye" in r["sources"] for r in results)
    assert len(fake_session.calls) == 1


@pytest.mark.asyncio
async def test_discover_candidates_merges_new_sources(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    monkeypatch.setattr(td, "_ENABLE_MEMPOOL", False)
    monkeypatch.setattr(td, "_ENABLE_DEXSCREENER", True)
    monkeypatch.setattr(td, "_ENABLE_METEORA", True)
    monkeypatch.setattr(td, "_ENABLE_DEXLAB", True)

    async def fake_bird():
        return []

    async def fake_collect(*args, **kwargs):
        return {}

    dex_mint = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"
    lab_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    async def fake_dexscreener(*, session=None):
        _ = session
        return [
            {
                "address": dex_mint,
                "symbol": "DEX",
                "name": "Dex Token",
                "liquidity": 12000,
                "volume": 18000,
                "price": 1.5,
                "price_change": 2.0,
            }
        ]

    async def fake_meteora(*, session=None):
        _ = session
        return [
            {
                "address": dex_mint,
                "symbol": "MT",
                "name": "Meteora Token",
                "liquidity": 4000,
                "volume": 3000,
            }
        ]

    async def fake_dexlab(*, session=None):
        _ = session
        return [
            {
                "address": lab_mint,
                "symbol": "",
                "name": "",
                "liquidity": 0,
                "volume": 0,
            }
        ]

    async def fake_enrich(candidates, *, addresses=None):
        _ = addresses
        entry = candidates.get(lab_mint)
        if entry is not None:
            entry["symbol"] = "LAB"
            entry["name"] = "DexLab Token"
            entry.setdefault("sources", set()).add("solscan")

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)
    monkeypatch.setattr(td, "_collect_mempool_signals", fake_collect)
    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", fake_dexscreener)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", fake_meteora)
    monkeypatch.setattr(td, "_fetch_dexlab_tokens", fake_dexlab)
    monkeypatch.setattr(td, "_enrich_with_solscan", fake_enrich)

    results = await td.discover_candidates("https://rpc", limit=5, mempool_threshold=0.0)

    addresses = {r["address"]: r for r in results}

    assert dex_mint in addresses
    assert lab_mint in addresses

    dex_sources = set(addresses[dex_mint]["sources"])
    assert {"dexscreener", "meteora"}.issubset(dex_sources)

    lab_entry = addresses[lab_mint]
    assert "dexlab" in lab_entry["sources"]
    assert "solscan" in lab_entry["sources"]
    assert lab_entry["name"] == "DexLab Token"


def test_warm_cache_skips_without_birdeye_key(monkeypatch):
    # Ensure environment does not provide a BirdEye key and guard short-circuits.
    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "")

    thread_called = {"created": False, "started": False}

    class DummyThread:
        def __init__(self, *args, **kwargs):
            thread_called["created"] = True

        def start(self):
            thread_called["started"] = True

    monkeypatch.setattr(td.threading, "Thread", DummyThread)

    td.warm_cache(rpc_url="")

    assert thread_called["created"] is False
    assert thread_called["started"] is False


@pytest.mark.asyncio
async def test_discover_candidates_shared_session_timeouts_and_cleanup(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    monkeypatch.setattr(td, "_ENABLE_MEMPOOL", False)
    monkeypatch.setattr(td, "_ENABLE_DEXSCREENER", True)
    monkeypatch.setattr(td, "_ENABLE_METEORA", True)
    monkeypatch.setattr(td, "_ENABLE_DEXLAB", True)

    async def fake_bird():
        return []

    async def fake_enrich(_candidates, *, addresses=None):
        _ = addresses
        return None

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)
    monkeypatch.setattr(td, "_enrich_with_solscan", fake_enrich)

    dex_mint = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"
    meteora_mint = "E7vCh2szgdWzxubAEANe1yoyWP7JfVv5sWpQXXAUP8Av"
    dexlab_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    payloads = {
        td._DEXSCREENER_URL: {
            "pairs": [
                {
                    "baseToken": {
                        "address": dex_mint,
                        "symbol": "DEX",
                        "name": "Dex Token",
                    },
                    "liquidity": {"usd": 12000},
                    "volume": {"h24": 18000},
                    "priceUsd": 1.23,
                }
            ]
        },
        td._METEORA_POOLS_URL: {
            "pools": [
                {
                    "tokenMint": meteora_mint,
                    "tokenSymbol": "MT",
                    "tokenName": "Meteora Token",
                    "liquidity": {"usd": 9000},
                    "volume24h": {"usd": 7000},
                }
            ]
        },
        td._DEXLAB_LIST_URL: [
            {
                "mint": dexlab_mint,
                "symbol": "DL",
                "name": "DexLab Token",
                "liquidity": 5000,
                "volume24h": 4000,
            }
        ],
    }

    class FakeResponse:
        status = 200

        def __init__(self, payload):
            self._payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        async def json(self, content_type=None):
            _ = content_type
            return self._payload

    class FakeSession:
        def __init__(self, mapping):
            self._mapping = mapping
            self.calls = []
            self.enter_count = 0
            self.exit_count = 0
            self.closed = False

        async def __aenter__(self):
            self.enter_count += 1
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.exit_count += 1
            self.closed = True
            return False

        def get(self, url, *, params=None, headers=None, timeout=None):
            _ = params, headers
            self.calls.append({"url": url, "timeout": timeout})
            payload = self._mapping.get(url, {})
            return FakeResponse(payload)

    fake_session = FakeSession(payloads)

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return fake_session

    monkeypatch.setattr(td, "get_session", fake_get_session)

    results = await td.discover_candidates("https://rpc", limit=5, mempool_threshold=0.0)

    assert len(fake_session.calls) == 3

    urls_seen = {call["url"] for call in fake_session.calls}
    assert {td._DEXSCREENER_URL, td._METEORA_POOLS_URL, td._DEXLAB_LIST_URL} <= urls_seen

    timeouts = {call["url"]: call["timeout"] for call in fake_session.calls}
    assert isinstance(timeouts[td._DEXSCREENER_URL], ClientTimeout)
    assert isinstance(timeouts[td._METEORA_POOLS_URL], ClientTimeout)
    assert isinstance(timeouts[td._DEXLAB_LIST_URL], ClientTimeout)
    assert timeouts[td._DEXSCREENER_URL].total == td._DEXSCREENER_TIMEOUT
    assert timeouts[td._METEORA_POOLS_URL].total == td._METEORA_TIMEOUT
    assert timeouts[td._DEXLAB_LIST_URL].total == td._DEXLAB_TIMEOUT

    assert fake_session.enter_count == 1
    assert fake_session.exit_count == 1
    assert fake_session.closed is True

    addresses = {entry["address"] for entry in results}
    assert {dex_mint, meteora_mint, dexlab_mint} <= addresses
