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

        def get(self, url, headers=None, timeout=10, params=None):
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

    async def fake_mempool(_rpc_url, threshold):
        _ = threshold
        yield {
            "address": "memToken",
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
    assert addresses[0] == "memToken"
    assert set(addresses) >= {bird1, bird2, "memToken"}
    assert results[0]["sources"] == ["mempool"]
    assert any("birdeye" in r["sources"] for r in results)
    assert len(fake_session.calls) == 1


@pytest.mark.asyncio
async def test_collect_mempool_signals_times_out(monkeypatch, caplog):
    class SleepyStream:
        def __init__(self):
            self.calls = 0
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            self.calls += 1
            await asyncio.sleep(0.2)
            return {"address": f"slow-{self.calls}"}

        async def aclose(self):
            self.closed = True

    sleepy = SleepyStream()

    monkeypatch.setattr(td, "_MEMPOOL_TIMEOUT", 0.05)
    monkeypatch.setattr(td, "_MEMPOOL_TIMEOUT_RETRIES", 2)
    monkeypatch.setattr(
        td,
        "stream_ranked_mempool_tokens_with_depth",
        lambda *_args, **_kwargs: sleepy,
    )

    caplog.set_level("DEBUG")

    scores = await td._collect_mempool_signals("https://rpc", threshold=0.0)

    assert scores == {}
    assert sleepy.calls >= td._MEMPOOL_TIMEOUT_RETRIES
    assert sleepy.closed
    assert any("timed out" in rec.message for rec in caplog.records)
