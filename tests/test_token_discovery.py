import asyncio
import types

import pytest

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

    async def _no_tokens():
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

    async def fake_bird():
        return []

    async def fake_collect(*args, **kwargs):
        return {}

    dex_mint = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"
    lab_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    async def fake_dexscreener():
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

    async def fake_meteora():
        return [
            {
                "address": dex_mint,
                "symbol": "MT",
                "name": "Meteora Token",
                "liquidity": 4000,
                "volume": 3000,
            }
        ]

    async def fake_dexlab():
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
