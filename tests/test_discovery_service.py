import asyncio

import pytest

from solhunter_zero.pipeline import discovery_service as discovery_mod


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_fetch_respects_empty_cooldown(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(
        queue,
        interval=0.1,
        cache_ttl=5.0,
        empty_cache_ttl=2.0,
        backoff_factor=2.0,
    )

    now = {"value": 1000.0}

    def fake_time() -> float:
        return now["value"]

    monkeypatch.setattr(discovery_mod.time, "time", fake_time)

    call_count = {"value": 0}

    async def fake_discover(self, **_: object) -> list[str]:
        call_count["value"] += 1
        self.last_details = {}
        return []

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    # Initial fetch should call the agent once and enter cooldown.
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert call_count["value"] == 1

    # Subsequent calls before cooldown expiry should not trigger the agent again.
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert call_count["value"] == 1

    now["value"] += 1.0
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert call_count["value"] == 1

    # Once cooldown expires the agent is invoked again.
    now["value"] += 2.0
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert call_count["value"] == 2


@pytest.mark.anyio
async def test_empty_backoff_grows_and_resets(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(
        queue,
        interval=0.1,
        cache_ttl=3.0,
        empty_cache_ttl=1.0,
        backoff_factor=2.0,
        max_backoff=10.0,
    )

    now = {"value": 0.0}

    def fake_time() -> float:
        return now["value"]

    monkeypatch.setattr(discovery_mod.time, "time", fake_time)

    responses = [([], {}), ([], {}), (["tok"], {"tok": {"liquidity": 1.0}})]

    async def fake_discover(self, **_: object) -> list[str]:
        tokens, details = responses.pop(0)
        self.last_details = details
        return list(tokens)

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    # First empty result applies base cooldown.
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert service._cooldown_until == pytest.approx(now["value"] + service.empty_cache_ttl)

    # Still within cooldown -> agent not called again.
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert len(responses) == 2

    # Advance time to trigger second fetch which is also empty.
    now["value"] = service._cooldown_until
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    expected_backoff = service.empty_cache_ttl * service.backoff_factor
    assert service._cooldown_until == pytest.approx(now["value"] + expected_backoff)

    # Advance into backoff but not beyond -> still cached response.
    now["value"] += expected_backoff / 2
    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}
    assert len(responses) == 1

    # After full backoff expires we get a non-empty batch and cooldown resets to cache_ttl.
    now["value"] = service._cooldown_until
    tokens, details = await service._fetch()
    assert tokens == ["tok"]
    assert details == {"tok": {"liquidity": 1.0}}
    assert service._consecutive_empty == 0
    assert service._cooldown_until == pytest.approx(now["value"] + service.cache_ttl)


@pytest.mark.anyio
async def test_emit_tokens_publishes_event(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    events: list[list[str]] = []

    def fake_publish(topic, payload, *args, **kwargs):
        if topic == "token_discovered":
            events.append(list(payload))

    monkeypatch.setattr(
        "solhunter_zero.pipeline.discovery_service.publish", fake_publish
    )

    await service._emit_tokens(["TokA", "TokB"], fresh=True)
    batch = await queue.get()
    assert [candidate.token for candidate in batch] == ["TokA", "TokB"]
    assert events == [["TokA", "TokB"]]

    await service._emit_tokens(["TokA", "TokB"], fresh=False)
    assert events == [["TokA", "TokB"]]


@pytest.mark.anyio
async def test_metadata_merges_trending_and_details(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    token = "TestToken"
    trending = {
        "symbol": "TT",
        "price": 1.5,
        "liquidity": 10.0,
        "sources": ["trending"],
        "score": 42,
        "rank": 7,
    }
    detail_payload = {
        "combined_score": 3.2,
        "liquidity": 55.0,
        "volume": 99.0,
        "sources": ["mempool"],
        "source": "mempool",
    }

    monkeypatch.setitem(discovery_mod.TRENDING_METADATA, token, trending)
    service._last_details = {token: detail_payload}

    batch = service._build_candidates([token])
    assert len(batch) == 1
    metadata = batch[0].metadata

    # Existing TRENDING fields are preserved.
    assert metadata["symbol"] == "TT"
    assert metadata["liquidity"] == pytest.approx(10.0)
    assert metadata["discovery_score"] == pytest.approx(42.0)
    assert metadata["trending_rank"] == 7

    # Additional sources from details are merged without duplicates.
    assert sorted(metadata["sources"]) == ["mempool", "trending"]

    # Detail payload contributes extra metrics when not present.
    assert metadata["volume"] == pytest.approx(99.0)
    assert metadata["mempool_score"] == pytest.approx(3.2)

    monkeypatch.delitem(discovery_mod.TRENDING_METADATA, token, raising=False)
