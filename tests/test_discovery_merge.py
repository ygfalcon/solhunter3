import asyncio
from solhunter_zero.discovery import merge_sources


from types import SimpleNamespace


VALID_MINT_A = "MintAphex1111111111111111111111111111111"
VALID_MINT_B = "MintBeton11111111111111111111111111111111"
VALID_MINT_C = "MintBreakr11111111111111111111111111111111"
VALID_MINT_D = "MintDepth11111111111111111111111111111111"


async def fake_stream(*a, **k):
    for item in [
        {"address": VALID_MINT_A, "volume": 1.0, "liquidity": 3.0},
        {"address": VALID_MINT_D, "volume": 4.0, "liquidity": 2.0},
    ]:
        yield item


def test_merge_sources(monkeypatch):
    async def fake_trend(limit=None):
        return [VALID_MINT_A, VALID_MINT_B]

    async def fake_onchain(url, return_metrics=False, max_tokens=None):
        return [
            {"address": VALID_MINT_B, "volume": 5.0, "liquidity": 1.0},
            {"address": VALID_MINT_C, "volume": 2.0, "liquidity": 1.0},
        ]

    async def fake_metric_vol(token, url):
        return {VALID_MINT_A: 10.0, VALID_MINT_B: 0.0}.get(token, 0.0)

    async def fake_metric_liq(token, url):
        return {VALID_MINT_A: 5.0, VALID_MINT_B: 0.0}.get(token, 0.0)

    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr("solhunter_zero.discovery.scan_tokens_onchain", fake_onchain)
    monkeypatch.setattr("solhunter_zero.discovery.validate_mint", lambda _: True)
    monkeypatch.setattr("solhunter_zero.discovery.normalize_candidate", lambda addr: addr)
    monkeypatch.setattr(
        "solhunter_zero.discovery.token_scanner",
        SimpleNamespace(TRENDING_METADATA={}),
        raising=False,
    )
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
    assert set(addresses) == {
        VALID_MINT_A,
        VALID_MINT_B,
        VALID_MINT_C,
        VALID_MINT_D,
    }


def test_merge_sources_includes_depth(monkeypatch):
    async def fake_trend(limit=None):
        return []

    async def fake_onchain(url, return_metrics=False, max_tokens=None):
        return []

    async def fake_stream(url, **_):
        yield {
            "address": VALID_MINT_D,
            "depth": 12.5,
            "depth_tx_rate": 3.2,
            "depth_history": [1.0, 2.0],
        }

    monkeypatch.setattr(
        "solhunter_zero.discovery.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr(
        "solhunter_zero.discovery.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr("solhunter_zero.discovery.scan_tokens_onchain", fake_onchain)
    monkeypatch.setattr("solhunter_zero.discovery.validate_mint", lambda _: True)
    monkeypatch.setattr("solhunter_zero.discovery.normalize_candidate", lambda addr: addr)
    monkeypatch.setattr(
        "solhunter_zero.discovery.token_scanner",
        SimpleNamespace(TRENDING_METADATA={}),
        raising=False,
    )

    result = asyncio.run(merge_sources("rpc", limit=1, mempool_threshold=0.0))
    assert result, "expected at least one merged entry"
    merged = next(
        (row for row in result if row.get("address") == VALID_MINT_D),
        None,
    )
    assert merged is not None
    assert merged.get("depth") == 12.5
    assert merged.get("depth_tx_rate") == 3.2
    assert merged.get("depth_history") == [1.0, 2.0]
