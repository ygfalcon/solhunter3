import asyncio
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


def test_merge_sources_retries_mempool(monkeypatch):
    async def fake_trend(limit=None):
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
