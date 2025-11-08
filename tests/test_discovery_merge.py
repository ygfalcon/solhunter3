import asyncio
import math

import pytest

from solhunter_zero import discovery as discovery_mod
from solhunter_zero.discovery import merge_sources


class DummyGen:
    def __init__(self, items):
        self._it = iter(items)

    async def __anext__(self):
        return next(self._it)

    def __aiter__(self):
        return self

    async def aclose(self):
        pass


async def fake_stream(*a, **k):
    for item in [
        {"address": "a", "volume": 1.0, "liquidity": 3.0},
        {"address": "d", "volume": 4.0, "liquidity": 2.0},
    ]:
        yield item


def test_merge_sources(monkeypatch):
    async def fake_trend(limit=None):
        return ["a", "b"]

    async def fake_onchain(url, return_metrics=False):
        return [
            {"address": "b", "volume": 5.0, "liquidity": 1.0},
            {"address": "c", "volume": 2.0, "liquidity": 1.0},
        ]

    async def fake_metric_vol(token, url):
        return {"a": 10.0, "b": 0.0}.get(token, 0.0)

    async def fake_metric_liq(token, url):
        return {"a": 5.0, "b": 0.0}.get(token, 0.0)

    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr("solhunter_zero.discovery.scan_tokens_onchain", fake_onchain)
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_volume_onchain_async",
        fake_metric_vol,
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_liquidity_onchain_async",
        fake_metric_liq,
    )

    result = asyncio.run(merge_sources("rpc", ws_url="ws://rpc"))
    addresses = [e["address"] for e in result]
    assert addresses[0] == "a"
    assert set(addresses) == {"a", "b", "c", "d"}


def test_latest_detail_payload_overrides_and_updates_score(monkeypatch):
    async def fake_trend(limit=None):
        return ["a"]

    async def fake_onchain(url, return_metrics=False):
        return []

    async def fake_collect(_rpc_url, *, limit, threshold, ws_url=None):
        _ = (limit, threshold, ws_url)
        return [
            {
                "address": "a",
                "liquidity": 100.0,
                "volume": 200.0,
                "price": 2.5,
                "combined_score": 0.4,
                "sources": ["mempool"],
                "source": "mempool",
            }
        ]

    async def fake_metric_vol(token, url):
        _ = (token, url)
        return 50.0

    async def fake_metric_liq(token, url):
        _ = (token, url)
        return 10.0

    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr("solhunter_zero.discovery.scan_tokens_onchain", fake_onchain)
    monkeypatch.setattr(
        discovery_mod,
        "_collect_mempool_candidates",
        fake_collect,
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_volume_onchain_async",
        fake_metric_vol,
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_liquidity_onchain_async",
        fake_metric_liq,
    )

    monkeypatch.setitem(
        discovery_mod.TRENDING_METADATA,
        "a",
        {
            "symbol": "AAA",
            "price": 1.0,
            "liquidity": 20.0,
            "volume": 75.0,
            "score": 0.1,
        },
    )

    result = asyncio.run(merge_sources("rpc"))
    entry = next(item for item in result if item["address"] == "a")

    assert entry["price"] == pytest.approx(2.5)
    assert entry["liquidity"] == pytest.approx(100.0)
    assert entry["volume"] == pytest.approx(200.0)
    assert entry["detail_source"] == "mempool"
    assert entry["detail_sources"] == ["trending", "mempool"]

    expected_base = math.log1p(100.0) * 0.55 + math.log1p(200.0) * 0.45
    expected_score = expected_base + 0.4 * 5.0 + 0.1
    assert entry["score"] == pytest.approx(expected_score)


def test_merge_sources_retries_mempool(monkeypatch):
    captured = {"limit": None}

    async def fake_trend(limit=None):
        captured["limit"] = limit
        return []

    async def fake_onchain(url, return_metrics=False):
        return []

    async def fake_stream(*_a, **_k):
        yield {"address": "m", "volume": 1.0, "liquidity": 1.0}

    attempts = {"n": 0}
    real_wait_for = asyncio.wait_for

    async def flaky_wait_for(awaitable, timeout):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise asyncio.TimeoutError
        return await real_wait_for(awaitable, timeout)

    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.scan_tokens_onchain", fake_onchain
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr(asyncio, "wait_for", flaky_wait_for)

    result = asyncio.run(merge_sources("rpc", limit=1, ws_url="ws://rpc"))
    assert [entry["address"] for entry in result] == ["m"]
    assert captured["limit"] == 1


def test_merge_sources_allows_limits_above_default(monkeypatch):
    tokens = [f"mint-{idx}" for idx in range(10)]

    captured_limit = {"value": None}

    async def fake_trend(limit=None):
        captured_limit["value"] = limit
        return tokens[: limit if limit is not None else len(tokens)]

    async def fake_onchain(url, return_metrics=False):
        return []

    async def fake_collect(*_a, **_k):
        return []

    async def metric_stub(*_a, **_k):
        return 0.0

    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr("solhunter_zero.discovery.scan_tokens_onchain", fake_onchain)
    monkeypatch.setattr(
        "solhunter_zero.discovery._collect_mempool_candidates", fake_collect
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_volume_onchain_async",
        metric_stub,
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_liquidity_onchain_async",
        metric_stub,
    )

    monkeypatch.setattr("solhunter_zero.discovery._DEFAULT_LIMIT", 5)

    result = asyncio.run(merge_sources("rpc", limit=10))
    assert len(result) == 10
    assert captured_limit["value"] == 10


def test_merge_sources_respects_metric_batch(monkeypatch):
    tokens = [f"mint-{idx}" for idx in range(5)]

    async def fake_trend(limit=None):
        return tokens

    async def fake_onchain(url, return_metrics=False):
        return []

    async def fake_collect(*_a, **_k):
        return []

    def metric_stub(counter):
        async def _stub(token, url):
            counter["active"] += 1
            counter["max"] = max(counter["max"], counter["active"])
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            counter["active"] -= 1
            return 1.0

        return _stub

    volume_counter = {"active": 0, "max": 0}
    liquidity_counter = {"active": 0, "max": 0}

    monkeypatch.setattr("solhunter_zero.discovery._METRIC_BATCH_SIZE", 2)
    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr("solhunter_zero.discovery.scan_tokens_onchain", fake_onchain)
    monkeypatch.setattr(
        "solhunter_zero.discovery._collect_mempool_candidates", fake_collect
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_volume_onchain_async",
        metric_stub(volume_counter),
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.onchain_metrics.fetch_liquidity_onchain_async",
        metric_stub(liquidity_counter),
    )

    result = asyncio.run(merge_sources("rpc", limit=len(tokens)))
    assert [entry["address"] for entry in result][: len(tokens)] == tokens
    assert volume_counter["max"] == 2
    assert liquidity_counter["max"] == 2


def test_fetch_trending_tokens_async_allows_limits_above_default(monkeypatch):
    tokens = [f"mint-{idx}" for idx in range(12)]

    captured = {"limit": None}

    async def fake_scan_tokens_async(limit=None):
        captured["limit"] = limit
        return tokens[: limit if limit is not None else len(tokens)]

    async def fake_enrich_tokens_async(items):
        return items

    monkeypatch.setattr(
        "solhunter_zero.discovery.scan_tokens_async", fake_scan_tokens_async
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.enrich_tokens_async", fake_enrich_tokens_async
    )
    monkeypatch.setattr("solhunter_zero.discovery._DEFAULT_LIMIT", 5)

    result = asyncio.run(
        discovery_mod.fetch_trending_tokens_async(limit=12)
    )

    assert captured["limit"] == 12
    assert len(result) == 12
    assert result == tokens
