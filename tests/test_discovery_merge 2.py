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


async def test_merge_sources(monkeypatch):
    async def fake_trend():
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

    result = asyncio.run(merge_sources("rpc"))
    addresses = [e["address"] for e in result]
    assert addresses[0] == "a"
    assert set(addresses) == {"a", "b", "c", "d"}
