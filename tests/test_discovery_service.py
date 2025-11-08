import asyncio
import time

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
async def test_snapshot_reports_backoff_state(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(
        queue,
        interval=0.1,
        cache_ttl=3.0,
        empty_cache_ttl=2.0,
        backoff_factor=2.0,
    )

    now = {"value": 50.0}

    def fake_time() -> float:
        return now["value"]

    monkeypatch.setattr(discovery_mod.time, "time", fake_time)

    responses = [([], {}), (["tok"], {"tok": {"liquidity": 1.0}})]

    async def fake_discover(self, **_: object) -> list[str]:
        tokens, details = responses.pop(0)
        self.last_details = details
        return list(tokens)

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    tokens, details = await service._fetch()
    assert tokens == []
    assert details == {}

    snap = service.snapshot()
    assert snap["current_backoff"] == pytest.approx(service.empty_cache_ttl)
    assert snap["cooldown_until"] == pytest.approx(now["value"] + service.empty_cache_ttl)
    assert snap["cooldown_active"] is True
    assert snap["consecutive_empty"] == 1
    assert snap["cooldown_remaining"] == pytest.approx(service.empty_cache_ttl)

    now["value"] += 1.0
    snap = service.snapshot()
    assert snap["cooldown_remaining"] == pytest.approx(service.empty_cache_ttl - 1.0)

    now["value"] = service._cooldown_until
    tokens, details = await service._fetch()
    assert tokens == ["tok"]
    assert details == {"tok": {"liquidity": 1.0}}

    snap = service.snapshot()
    assert snap["current_backoff"] == 0.0
    assert snap["cooldown_active"] is False
    assert snap["cooldown_until"] is None
    assert snap["last_fetch_empty"] is False


@pytest.mark.anyio
async def test_startup_clone_timeout(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(
        queue,
        interval=0.1,
        cache_ttl=0.0,
        startup_clones=2,
        startup_clone_timeout=0.05,
    )

    emitted: list[list[str]] = []

    async def fake_emit(self, tokens, *, fresh: bool) -> None:
        emitted.append([str(tok) for tok in tokens])

    monkeypatch.setattr(discovery_mod.DiscoveryService, "_emit_tokens", fake_emit)

    cancelled = {"value": False}

    async def fake_clone(self, idx: int) -> tuple[list[str], dict[str, dict[str, object]]]:
        if idx == 0:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled["value"] = True
                raise
        return (["fast"], {"fast": {"source": "quick"}})

    monkeypatch.setattr(discovery_mod.DiscoveryService, "_clone_fetch", fake_clone)

    async def fake_run(self) -> None:
        await self._stopped.wait()

    monkeypatch.setattr(discovery_mod.DiscoveryService, "_run", fake_run)

    await service.start()

    assert service._primed is True
    assert service._last_tokens == ["fast"]
    assert emitted == [["fast"]]
    assert cancelled["value"] is True

    await service.stop()


@pytest.mark.anyio
async def test_emit_tokens_publishes_event(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    events: list[dict[str, object]] = []

    def fake_publish(topic, payload, *args, **kwargs):
        if topic == "token_discovered":
            events.append(dict(payload))

    monkeypatch.setattr(
        "solhunter_zero.pipeline.discovery_service.publish", fake_publish
    )

    await service._emit_tokens(["TokA", "TokB"], fresh=True)
    batch = await queue.get()
    assert [candidate.token for candidate in batch] == ["TokA", "TokB"]
    assert events == [
        {
            "tokens": ["TokA", "TokB"],
            "metadata_refresh": False,
            "changed_tokens": [],
        }
    ]

    await service._emit_tokens(["TokA", "TokB"], fresh=False)
    assert len(events) == 1


@pytest.mark.anyio
async def test_metadata_refresh_bypasses_duplicate_guard(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    events: list[dict[str, object]] = []

    def fake_publish(topic, payload, *args, **kwargs):
        if topic == "token_discovered":
            events.append(dict(payload))

    monkeypatch.setattr(
        "solhunter_zero.pipeline.discovery_service.publish", fake_publish
    )

    token = "MetaTok"
    initial_meta = {"liquidity": 10.0, "score": 2.0, "rank": 1}
    updated_meta = {"liquidity": 25.0, "score": 5.0, "rank": 1}

    discovery_mod.TRENDING_METADATA[token] = dict(initial_meta)
    await service._emit_tokens([token], fresh=True)
    first_batch = await queue.get()
    assert first_batch[0].metadata["liquidity"] == pytest.approx(10.0)
    events.clear()

    discovery_mod.TRENDING_METADATA[token] = dict(updated_meta)
    await service._emit_tokens([token], fresh=False)
    second_batch = await queue.get()

    assert second_batch[0].metadata["liquidity"] == pytest.approx(25.0)
    assert len(events) == 1
    payload = events[0]
    assert payload["metadata_refresh"] is True
    assert payload["changed_tokens"] == [token]
    assert payload["tokens"] == [token]

    monkeypatch.delitem(discovery_mod.TRENDING_METADATA, token, raising=False)


@pytest.mark.anyio
async def test_metadata_changes_emit_when_fresh(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    events: list[dict[str, object]] = []

    def fake_publish(topic, payload, *args, **kwargs):
        if topic == "token_discovered":
            events.append(dict(payload))

    monkeypatch.setattr(
        "solhunter_zero.pipeline.discovery_service.publish", fake_publish
    )

    token = "FreshMeta"
    initial_meta = {"liquidity": 5.0, "score": 1.0}
    updated_meta = {"liquidity": 15.0, "score": 3.0}

    discovery_mod.TRENDING_METADATA[token] = dict(initial_meta)
    await service._emit_tokens([token], fresh=False)
    await queue.get()
    events.clear()

    discovery_mod.TRENDING_METADATA[token] = dict(updated_meta)
    await service._emit_tokens([token], fresh=True)
    second_batch = await queue.get()

    assert second_batch[0].metadata["liquidity"] == pytest.approx(15.0)
    assert events == [
        {
            "tokens": [token],
            "metadata_refresh": False,
            "changed_tokens": [token],
        }
    ]

    monkeypatch.delitem(discovery_mod.TRENDING_METADATA, token, raising=False)


@pytest.mark.anyio
async def test_fetch_switches_method_when_override_changes(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    service = discovery_mod.DiscoveryService(
        queue,
        interval=0.1,
        cache_ttl=0.0,
        empty_cache_ttl=0.0,
        backoff_factor=1.0,
    )

    captured_methods: list[str | None] = []

    async def fake_discover(self, **kwargs):
        captured_methods.append(kwargs.get("method"))
        self.last_details = {}
        return []

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    monkeypatch.setattr(
        discovery_mod.discovery_state,
        "current_method",
        lambda **_: "helius",
    )
    await service._fetch()

    monkeypatch.setattr(
        discovery_mod.discovery_state,
        "current_method",
        lambda **_: "mempool",
    )
    await service._fetch()

    assert captured_methods == ["helius", "mempool"]


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
        "detail_source": "mempool",
        "detail_sources": ["trending", "mempool"],
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
    assert metadata["detail_source"] == "mempool"
    assert metadata["detail_sources"] == ["trending", "mempool"]

    monkeypatch.delitem(discovery_mod.TRENDING_METADATA, token, raising=False)


@pytest.mark.anyio
async def test_fetch_refreshes_agent_when_rpc_env_changes(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    monkeypatch.setenv("SOLANA_RPC_URL", "https://rpc.initial")

    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    seen_urls: list[str] = []

    async def fake_discover(self, **_: object) -> list[str]:
        seen_urls.append(self.rpc_url)
        self.last_details = {}
        return ["tok"]

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    tokens, details = await service._fetch()
    assert tokens == ["tok"]
    assert details == {}
    assert seen_urls[-1] == "https://rpc.initial"

    monkeypatch.setenv("SOLANA_RPC_URL", "https://rpc.updated")

    tokens, details = await service._fetch()
    assert tokens == ["tok"]
    assert details == {}
    assert seen_urls[-1] == "https://rpc.updated"


@pytest.mark.anyio
async def test_fetch_refreshes_agent_when_method_env_changes(monkeypatch):
    queue: asyncio.Queue = asyncio.Queue()
    monkeypatch.setenv("DISCOVERY_METHOD", "helius")

    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    seen_methods: list[str] = []

    async def fake_discover(self, **_: object) -> list[str]:
        seen_methods.append(self.default_method)
        self.last_details = {}
        return ["tok"]

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    tokens, details = await service._fetch()
    assert tokens == ["tok"]
    assert details == {}
    assert seen_methods[-1] == "helius"

    monkeypatch.setenv("DISCOVERY_METHOD", "mempool")

    tokens, details = await service._fetch()
    assert tokens == ["tok"]
    assert details == {}
    assert seen_methods[-1] == "mempool"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("env_key", "initial", "updated"),
    [
        ("DISCOVERY_LIMIT", "10", "25"),
        ("DISCOVERY_CACHE_TTL", "5", "15"),
        ("TOKEN_DISCOVERY_BACKOFF", "1", "3"),
        ("TOKEN_DISCOVERY_RETRIES", "2", "4"),
        ("MEMPOOL_SCORE_THRESHOLD", "0.2", "0.4"),
    ],
)
async def test_fetch_refreshes_service_state_for_tuning_env_changes(
    monkeypatch, env_key: str, initial: str, updated: str
) -> None:
    queue: asyncio.Queue = asyncio.Queue()
    monkeypatch.setenv(env_key, initial)

    service = discovery_mod.DiscoveryService(queue, interval=0.1, cache_ttl=0.0)

    state_snapshots: list[dict[str, object]] = []

    async def fake_discover(self, **_: object) -> list[str]:
        state_snapshots.append(
            {
                "cooldown": service._cooldown_until,
                "last_tokens": list(service._last_tokens),
                "last_details": dict(service._last_details),
                "last_emitted": list(service._last_emitted),
                "consecutive_empty": service._consecutive_empty,
                "current_backoff": service._current_backoff,
                "last_fetch_ts": service._last_fetch_ts,
                "metadata_snapshot": dict(service._last_metadata_snapshot),
                "last_fetch_fresh": service._last_fetch_fresh,
            }
        )
        self.last_details = {}
        return [f"tok-{len(state_snapshots)}"]

    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    tokens, details = await service._fetch()
    assert tokens == ["tok-1"]
    assert details == {}

    service._cooldown_until = time.time() + 60.0
    service._last_tokens = ["cached-token"]
    service._last_details = {"cached-token": {"liquidity": 123.0}}
    service._last_emitted = ["cached-token"]
    service._consecutive_empty = 7
    service._current_backoff = 99.0
    service._last_fetch_ts = 12345.6
    service._last_metadata_snapshot = {"cached-token": {"source": "cache"}}
    service._last_fetch_fresh = False

    refresh_calls: list[bool] = []
    original_refresh = service.refresh

    def tracking_refresh() -> None:
        refresh_calls.append(True)
        original_refresh()

    monkeypatch.setattr(service, "refresh", tracking_refresh)

    monkeypatch.setenv(env_key, updated)

    tokens, details = await service._fetch()
    assert tokens == ["tok-2"]
    assert details == {}
    assert refresh_calls == [True]
    assert len(state_snapshots) == 2

    snapshot = state_snapshots[1]
    assert snapshot["cooldown"] == 0.0
    assert snapshot["last_tokens"] == []
    assert snapshot["last_details"] == {}
    assert snapshot["last_emitted"] == []
    assert snapshot["consecutive_empty"] == 0
    assert snapshot["current_backoff"] == 0.0
    assert snapshot["last_fetch_ts"] == 0.0
    assert snapshot["metadata_snapshot"] == {}
    assert snapshot["last_fetch_fresh"] is True
