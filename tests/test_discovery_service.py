import asyncio

import pytest

from solhunter_zero.pipeline import discovery_service as discovery_mod
from solhunter_zero.token_scanner import TRENDING_METADATA


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
        return []

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    # Initial fetch should call the agent once and enter cooldown.
    assert await service._fetch() == []
    assert call_count["value"] == 1

    # Subsequent calls before cooldown expiry should not trigger the agent again.
    assert await service._fetch() == []
    assert call_count["value"] == 1

    now["value"] += 1.0
    assert await service._fetch() == []
    assert call_count["value"] == 1

    # Once cooldown expires the agent is invoked again.
    now["value"] += 2.0
    assert await service._fetch() == []
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

    responses = [[], [], ["tok"]]

    async def fake_discover(self, **_: object) -> list[str]:
        return list(responses.pop(0))

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    # First empty result applies base cooldown.
    assert await service._fetch() == []
    assert service._cooldown_until == pytest.approx(now["value"] + service.empty_cache_ttl)

    # Still within cooldown -> agent not called again.
    assert await service._fetch() == []
    assert len(responses) == 2

    # Advance time to trigger second fetch which is also empty.
    now["value"] = service._cooldown_until
    assert await service._fetch() == []
    expected_backoff = service.empty_cache_ttl * service.backoff_factor
    assert service._cooldown_until == pytest.approx(now["value"] + expected_backoff)

    # Advance into backoff but not beyond -> still cached response.
    now["value"] += expected_backoff / 2
    assert await service._fetch() == []
    assert len(responses) == 1

    # After full backoff expires we get a non-empty batch and cooldown resets to cache_ttl.
    now["value"] = service._cooldown_until
    assert await service._fetch() == ["tok"]
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


def test_build_candidates_uses_metadata_sources():
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)
    service._agent.last_method = "websocket"
    service._agent.last_details = {
        "MintA": {"sources": {"mempool", "helius"}, "symbol": "MNTA"}
    }
    TRENDING_METADATA["MintA"] = {"sources": ["birdeye"], "name": "Mint Alpha"}
    try:
        batch = service._build_candidates(["MintA"])
        assert len(batch) == 1
        candidate = batch[0]
        assert candidate.source == "mempool"
        assert candidate.metadata["sources"] == ["mempool", "helius", "birdeye"]
        assert candidate.metadata["symbol"] == "MNTA"
        assert candidate.metadata["name"] == "Mint Alpha"
    finally:
        TRENDING_METADATA.pop("MintA", None)


def test_build_candidates_aggregates_unknown_sources():
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)
    service._agent.last_method = "websocket"
    service._agent.last_details = {"MintB": {"sources": ["Foo", "Bar"]}}
    try:
        batch = service._build_candidates(["MintB"])
        assert len(batch) == 1
        candidate = batch[0]
        assert candidate.source == "multi:bar+foo"
        assert candidate.metadata["sources"] == ["bar", "foo"]
    finally:
        TRENDING_METADATA.pop("MintB", None)


def test_build_candidates_falls_back_to_last_method():
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)
    service._agent.last_method = "file"
    service._agent.last_details = {}
    batch = service._build_candidates(["MintC"])
    assert len(batch) == 1
    candidate = batch[0]
    assert candidate.source == "file"
