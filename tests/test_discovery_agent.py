import asyncio
import sys
import time
import math
import types

if "base58" not in sys.modules:
    def _fake_b58decode(value):
        if not isinstance(value, str):
            raise TypeError("value must be str")
        if not (32 <= len(value.strip()) <= 44):
            raise ValueError("invalid length")
        return b"\x00" * 32

    sys.modules["base58"] = types.SimpleNamespace(b58decode=_fake_b58decode)

from solhunter_zero.agents import discovery as discovery_mod
from solhunter_zero.agents.discovery import DiscoveryAgent


VALID_MINT = "So11111111111111111111111111111111111111112"


def _reset_cache():
    discovery_mod._CACHE.update(
        {
            "tokens": [],
            "ts": 0.0,
            "limit": 0,
            "method": "",
            "rpc_identity": "",
            "details": {},
        }
    )


async def fake_stream(url, **_):
    yield {"address": VALID_MINT, "score": 12.0}


def test_stream_mempool_events(monkeypatch):
    _reset_cache()
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )

    agent = DiscoveryAgent()

    async def run():
        gen = agent.stream_mempool_events("ws://node")
        data = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return data

    data = asyncio.run(run())
    assert data["address"] == VALID_MINT


def test_invalid_env_limit(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_LIMIT", "fast")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.limit == 60
    assert "Invalid DISCOVERY_LIMIT" in caplog.text


def test_invalid_cache_ttl_env(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.delenv("FAST_PIPELINE_MODE", raising=False)
    monkeypatch.delenv("FAST_DISCOVERY_CACHE_TTL", raising=False)
    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "fast")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.cache_ttl == 45.0
    assert "Invalid DISCOVERY_CACHE_TTL" in caplog.text


def test_negative_env_limit(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_LIMIT", "-5")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.limit == 0
    assert "below minimum" in caplog.text


def test_env_limit_above_cap(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_LIMIT", "500")
    monkeypatch.setenv("DISCOVERY_LIMIT_CAP", "200")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.limit == 200
    assert agent.limit_cap == 200
    assert "above maximum" in caplog.text


def test_invalid_retry_env(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("TOKEN_DISCOVERY_RETRIES", "not-a-number")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.max_attempts == 2
    assert "Invalid TOKEN_DISCOVERY_RETRIES" in caplog.text

    caplog.clear()
    monkeypatch.setenv("TOKEN_DISCOVERY_RETRIES", "0")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.max_attempts == 2
    assert "below minimum" in caplog.text

    monkeypatch.delenv("TOKEN_DISCOVERY_RETRIES", raising=False)


def test_zero_env_limit_disables_discovery(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_LIMIT", "0")

    called = False

    async def fail_scan(*_args, **_kwargs):
        nonlocal called
        called = True
        return ["unexpected"]

    monkeypatch.setattr(discovery_mod, "scan_tokens_async", fail_scan)

    with caplog.at_level("INFO", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()
        tokens = asyncio.run(agent.discover_tokens())

    assert agent.limit == 0
    assert tokens == []
    assert called is False
    assert "Discovery disabled" in caplog.text


def test_social_limit_invalid_env_uses_default(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_SOCIAL_LIMIT", "nope")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.social_limit == 12
    assert "Invalid DISCOVERY_SOCIAL_LIMIT" in caplog.text


def test_social_min_mentions_below_min_uses_default(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_SOCIAL_MIN_MENTIONS", "0")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.social_min_mentions == 2
    assert "DISCOVERY_SOCIAL_MIN_MENTIONS" in caplog.text


def test_social_sample_limit_invalid_env_uses_default(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("DISCOVERY_SOCIAL_SAMPLE_LIMIT", "bad")

    with caplog.at_level("WARNING", logger="solhunter_zero.agents.discovery"):
        agent = DiscoveryAgent()

    assert agent.social_sample_limit == 3
    assert "Invalid DISCOVERY_SOCIAL_SAMPLE_LIMIT" in caplog.text


def test_collect_mempool_times_out(monkeypatch, caplog):
    _reset_cache()

    class NeverYield:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(1.0)
            return {"address": VALID_MINT}

        async def aclose(self):
            return None

    def never_stream(*_args, **_kwargs):
        return NeverYield()

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.stream_ranked_mempool_tokens_with_depth",
        never_stream,
    )
    monkeypatch.setattr(discovery_mod, "_MEMPOOL_TIMEOUT", 0.01)
    monkeypatch.setattr(discovery_mod, "_MEMPOOL_TIMEOUT_RETRIES", 1)
    monkeypatch.setenv("DISCOVERY_MEMPOOL_MAX_WAIT", "0.05")
    monkeypatch.setenv("TOKEN_DISCOVERY_BACKOFF", "0.05")

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("WARNING"):
            return await agent._collect_mempool()

    start = time.perf_counter()
    tokens, details = asyncio.run(run())
    elapsed = time.perf_counter() - start

    assert tokens == []
    assert details == {}
    assert elapsed < 0.5
    assert "Mempool stream yielded no events" in caplog.text


def test_collect_mempool_waits_for_slow_stream(monkeypatch, caplog):
    _reset_cache()

    class SlowYield:
        def __init__(self):
            self._released = False
            self._start: float | None = None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._released:
                raise StopAsyncIteration
            if self._start is None:
                self._start = time.perf_counter()
            try:
                while time.perf_counter() - self._start < 0.045:
                    await asyncio.sleep(0.005)
            except asyncio.CancelledError:
                raise
            self._released = True
            return {"address": VALID_MINT, "score": 9.5}

        async def aclose(self):
            return None

    def slow_stream(*_args, **_kwargs):
        return SlowYield()

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.stream_ranked_mempool_tokens_with_depth",
        slow_stream,
    )
    monkeypatch.setattr(discovery_mod, "_MEMPOOL_TIMEOUT", 0.01)
    monkeypatch.setattr(discovery_mod, "_MEMPOOL_TIMEOUT_RETRIES", 1)
    monkeypatch.setenv("DISCOVERY_MEMPOOL_MAX_WAIT", "0.2")
    monkeypatch.setenv("TOKEN_DISCOVERY_BACKOFF", "0.0")

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("DEBUG", logger="solhunter_zero.agents.discovery"):
            return await agent._collect_mempool()

    start = time.perf_counter()
    tokens, details = asyncio.run(run())
    elapsed = time.perf_counter() - start

    assert tokens == [VALID_MINT]
    assert details[VALID_MINT]["address"] == VALID_MINT
    assert elapsed >= 0.045
    assert "waiting for events" in caplog.text


def test_collect_mempool_honours_default_wait(monkeypatch):
    _reset_cache()

    class DelayedYield:
        def __init__(self, delay: float = 1.2) -> None:
            self._delay = delay
            self._emitted = False
            self._start: float | None = None

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._emitted:
                raise StopAsyncIteration
            if self._start is None:
                self._start = time.perf_counter()
            try:
                while time.perf_counter() - self._start < self._delay:
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                raise
            self._emitted = True
            return {"address": VALID_MINT, "score": 5.5}

        async def aclose(self):
            return None

    def delayed_stream(*_args, **_kwargs):
        return DelayedYield()

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.stream_ranked_mempool_tokens_with_depth",
        delayed_stream,
    )
    monkeypatch.setattr(discovery_mod, "_MEMPOOL_TIMEOUT", 0.25)
    monkeypatch.setattr(discovery_mod, "_MEMPOOL_TIMEOUT_RETRIES", 1)
    monkeypatch.delenv("DISCOVERY_MEMPOOL_MAX_WAIT", raising=False)
    monkeypatch.setenv("TOKEN_DISCOVERY_BACKOFF", "0.1")

    agent = DiscoveryAgent()

    async def run():
        start = time.perf_counter()
        tokens, details = await agent._collect_mempool()
        elapsed = time.perf_counter() - start
        return elapsed, tokens, details

    elapsed, tokens, details = asyncio.run(run())

    assert elapsed >= 1.0
    assert elapsed < 5.0
    assert tokens == [VALID_MINT]
    assert details[VALID_MINT]["address"] == VALID_MINT


def test_discover_tokens_retries_on_empty_scan(monkeypatch, caplog):
    _reset_cache()
    calls = []

    async def fake_scan(*a, **k):
        calls.append(None)
        return [] if len(calls) == 1 else [VALID_MINT]

    sleep_calls: list[float] = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    async def fake_enrich(tokens, rpc_url=None):
        return list(tokens)

    async def fake_merge(*a, **k):
        return []

    async def fake_mempool(self):
        return [], {}

    def fake_fallback(self):
        return []

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.scan_tokens_async", fake_scan
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.asyncio.sleep", fake_sleep
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.enrich_tokens_async", fake_enrich
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )
    monkeypatch.setattr(DiscoveryAgent, "_collect_mempool", fake_mempool)
    monkeypatch.setattr(DiscoveryAgent, "_fallback_tokens", fake_fallback)
    monkeypatch.setenv("TOKEN_DISCOVERY_BACKOFF", "1.5")

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("WARNING"):
            return await agent.discover_tokens()

    tokens = asyncio.run(run())
    assert tokens == [VALID_MINT]
    assert len(calls) == 2
    assert sleep_calls == [1.5]
    assert "No tokens discovered" in caplog.text


def test_discover_tokens_retries_on_empty_merge(monkeypatch, caplog):
    _reset_cache()
    calls = []

    async def fake_merge(url, mempool_threshold=0.0):
        calls.append(None)
        if len(calls) == 1:
            return []
        return [{"address": VALID_MINT}]

    sleep_calls: list[float] = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.scan_tokens_async",
        lambda *a, **k: (_ for _ in ()).throw(AssertionError("scan should not be called")),
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.asyncio.sleep", fake_sleep
    )
    monkeypatch.setenv("TOKEN_DISCOVERY_BACKOFF", "0")

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("WARNING"):
            return await agent.discover_tokens(offline=False, method="websocket")

    tokens = asyncio.run(run())
    assert tokens == [VALID_MINT]
    assert len(calls) == 2
    assert sleep_calls == [0.0]
    assert "No tokens discovered" in caplog.text


def test_discover_tokens_recovers_from_merge_exception(monkeypatch, caplog):
    _reset_cache()

    async def fake_merge(*_, **__):
        raise ConnectionError("network down")

    async def fake_collect_mempool(self):
        return [VALID_MINT], {VALID_MINT: {"address": VALID_MINT}}

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )
    monkeypatch.setattr(DiscoveryAgent, "_collect_mempool", fake_collect_mempool)

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("WARNING"):
            return await agent.discover_tokens(method="websocket")

    tokens = asyncio.run(run())
    assert tokens == [VALID_MINT]
    assert "Websocket merge failed" in caplog.text
    assert "Websocket merge yielded no tokens" in caplog.text


def test_discover_once_websocket_omits_unsupported_merge_kwargs(monkeypatch):
    _reset_cache()

    async def fake_merge(rpc_url, *, mempool_threshold):
        assert isinstance(rpc_url, str)
        return [{"address": VALID_MINT}]

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )

    agent = DiscoveryAgent()
    agent.limit = 42

    tokens, details = asyncio.run(
        agent._discover_once(method="websocket", offline=False, token_file=None)
    )

    assert tokens == [VALID_MINT]
    assert details[VALID_MINT]["address"] == VALID_MINT


def test_discover_once_websocket_supports_new_merge_kwargs(monkeypatch):
    _reset_cache()

    received: dict[str, object] = {}

    async def fake_merge(rpc_url, *, limit, mempool_threshold, ws_url):
        received["rpc_url"] = rpc_url
        received["limit"] = limit
        received["mempool_threshold"] = mempool_threshold
        received["ws_url"] = ws_url
        return [{"address": VALID_MINT}]

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )

    agent = DiscoveryAgent()
    agent.limit = 15

    tokens, details = asyncio.run(
        agent._discover_once(method="websocket", offline=False, token_file=None)
    )

    assert tokens == [VALID_MINT]
    assert details[VALID_MINT]["address"] == VALID_MINT
    assert received["limit"] == agent.limit
    assert received["mempool_threshold"] == agent.mempool_threshold
    assert received["ws_url"] == agent.ws_url


def test_discover_tokens_warns_when_birdeye_missing_and_mempool_disabled(
    monkeypatch, caplog
):
    _reset_cache()
    monkeypatch.setenv("BIRDEYE_API_KEY", "")
    monkeypatch.setenv("DISCOVERY_ENABLE_MEMPOOL", "0")
    monkeypatch.setenv("TOKEN_DISCOVERY_RETRIES", "1")

    async def fake_discover_once(self, *, method, offline, token_file):
        _ = method, offline, token_file
        return [VALID_MINT], {VALID_MINT: {"address": VALID_MINT}}

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fake_discover_once)
    monkeypatch.setattr("solhunter_zero.agents.discovery.publish", lambda *_, **__: None)

    agent = DiscoveryAgent()
    agent.birdeye_api_key = ""

    with caplog.at_level("WARNING"):
        tokens = asyncio.run(agent.discover_tokens())

    assert tokens == [VALID_MINT]
    assert (
        "BirdEye API key missing and mempool discovery disabled; continuing with DEX and cached/static discovery sources."
        in caplog.text
    )


def test_discover_tokens_warns_once_when_birdeye_missing(monkeypatch, caplog):
    _reset_cache()
    monkeypatch.setenv("BIRDEYE_API_KEY", "")
    monkeypatch.setenv("DISCOVERY_ENABLE_MEMPOOL", "1")
    monkeypatch.setenv("TOKEN_DISCOVERY_RETRIES", "1")
    monkeypatch.setattr("solhunter_zero.agents.discovery.publish", lambda *_, **__: None)

    async def fake_discover_once(self, *, method, offline, token_file):
        _ = method, offline, token_file
        return [], {}

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fake_discover_once)

    agent = DiscoveryAgent()
    agent.birdeye_api_key = ""

    with caplog.at_level("WARNING"):
        tokens_first = asyncio.run(agent.discover_tokens())

    assert tokens_first
    assert (
        "BirdEye API key missing; continuing discovery with mempool and DEX sources."
        in caplog.text
    )

    caplog.clear()

    with caplog.at_level("WARNING"):
        tokens_second = asyncio.run(agent.discover_tokens())

    assert tokens_second
    assert "continuing discovery with mempool and DEX sources" not in caplog.text


def test_collect_social_mentions(monkeypatch):
    _reset_cache()
    agent = DiscoveryAgent()
    agent.news_feeds = ["http://ok"]
    agent.social_limit = 5
    agent.social_min_mentions = 1

    async def fake_fetch_token_mentions_async(*args, **kwargs):
        return [
            {
                "token": "So11111111111111111111111111111111111111112",
                "mentions": 3,
                "samples": ["mention"],
            }
        ]

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.fetch_token_mentions_async",
        fake_fetch_token_mentions_async,
    )

    result = asyncio.run(agent._collect_social_mentions())
    assert result
    payload = result["So11111111111111111111111111111111111111112"]
    assert payload["social_mentions"] == 3
    assert "social_samples" in payload


def test_discover_tokens_includes_social_mentions(monkeypatch):
    _reset_cache()
    agent = DiscoveryAgent()
    agent.news_feeds = ["http://ok"]
    agent.social_limit = 3
    agent.social_min_mentions = 1

    async def fake_discover_once(self, *, method, offline, token_file):
        return [], {}

    async def fake_collect_social(self):
        return {
            "So11111111111111111111111111111111111111112": {
                "social_mentions": 4,
                "social_rank": 1,
                "sources": {"social"},
            }
        }

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fake_discover_once)
    monkeypatch.setattr(DiscoveryAgent, "_collect_social_mentions", fake_collect_social)

    tokens = asyncio.run(agent.discover_tokens())
    assert tokens == ["So11111111111111111111111111111111111111112"]
    assert agent.last_details[tokens[0]]["social_mentions"] == 4


def test_social_mentions_filter_invalid_tokens_post_merge(monkeypatch):
    _reset_cache()
    agent = DiscoveryAgent()
    agent.social_limit = 5

    async def fake_collect_social(self):
        return {
            VALID_MINT: {
                "social_mentions": 10,
                "sources": {"social"},
            },
            "invalid-mint": {
                "social_mentions": 99,
                "sources": {"social"},
            },
        }

    monkeypatch.setattr(DiscoveryAgent, "_collect_social_mentions", fake_collect_social)

    tokens, details = asyncio.run(
        agent._apply_social_mentions(
            [VALID_MINT],
            {VALID_MINT: {"sources": {"discovery"}}},
        )
    )

    assert tokens == [VALID_MINT]
    assert VALID_MINT in details
    assert "invalid-mint" not in tokens
    assert "invalid-mint" not in details
    assert details[VALID_MINT]["social_mentions"] == 10
    assert details[VALID_MINT]["sources"] == {"discovery", "social"}


def test_discovery_cache_is_scoped_per_rpc(monkeypatch):
    _reset_cache()
    monkeypatch.setenv("SOLANA_RPC_URL", "https://mainnet.rpc.local")

    call_count = {"value": 0}

    async def fake_discover_once(self, *, method, offline, token_file):
        call_count["value"] += 1
        return [VALID_MINT], {}

    async def passthrough_social(self, tokens, details, *, offline=False):
        return tokens, details or {}

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fake_discover_once)
    monkeypatch.setattr(DiscoveryAgent, "_apply_social_mentions", passthrough_social)

    agent = DiscoveryAgent()
    agent.cache_ttl = 60.0

    tokens_mainnet_first = asyncio.run(agent.discover_tokens())
    assert tokens_mainnet_first == [VALID_MINT]
    assert call_count["value"] == 1

    tokens_mainnet_cached = asyncio.run(agent.discover_tokens())
    assert tokens_mainnet_cached == [VALID_MINT]
    assert call_count["value"] == 1

    monkeypatch.setenv("SOLANA_RPC_URL", "https://devnet.rpc.local")
    agent_devnet = DiscoveryAgent()
    agent_devnet.cache_ttl = 60.0

    tokens_devnet = asyncio.run(agent_devnet.discover_tokens())
    assert tokens_devnet == [VALID_MINT]
    assert call_count["value"] == 2

    tokens_devnet_cached = asyncio.run(agent_devnet.discover_tokens())
    assert tokens_devnet_cached == [VALID_MINT]
    assert call_count["value"] == 2


def test_discover_tokens_concurrent_calls_use_cache_lock(monkeypatch):
    _reset_cache()

    async def runner():
        class InstrumentedLock:
            def __init__(self) -> None:
                self.enter_count = 0
                self.max_active = 0
                self._active = 0
                self._lock = asyncio.Lock()

            async def __aenter__(self):
                await self._lock.acquire()
                self.enter_count += 1
                self._active += 1
                if self._active > self.max_active:
                    self.max_active = self._active
                return self

            async def __aexit__(self, exc_type, exc, tb):
                self._active -= 1
                self._lock.release()

        instrumented_lock = InstrumentedLock()
        monkeypatch.setattr(discovery_mod, "_CACHE_LOCK", instrumented_lock)

        agent = DiscoveryAgent()
        agent.cache_ttl = 60.0

        call_counter = {"value": 0}
        first_call_ready = asyncio.Event()
        release_first_call = asyncio.Event()

        async def fake_discover_once(self, *, method, offline, token_file):
            del method, offline, token_file
            call_counter["value"] += 1
            if call_counter["value"] == 1:
                first_call_ready.set()
                await release_first_call.wait()
            return [VALID_MINT], {}

        async def passthrough_social(self, tokens, details, *, offline=False):
            del offline
            return tokens, details or {}

        monkeypatch.setattr(DiscoveryAgent, "_discover_once", fake_discover_once)
        monkeypatch.setattr(DiscoveryAgent, "_apply_social_mentions", passthrough_social)

        first_task = asyncio.create_task(agent.discover_tokens())
        await first_call_ready.wait()
        second_task = asyncio.create_task(agent.discover_tokens())
        await asyncio.sleep(0)
        release_first_call.set()

        results = await asyncio.gather(first_task, second_task)
        return results, instrumented_lock

    results, instrumented_lock = asyncio.run(runner())

    assert results[0] == [VALID_MINT]
    assert results[1] == [VALID_MINT]
    assert instrumented_lock.enter_count >= 4
    assert instrumented_lock.max_active == 1
    assert discovery_mod._CACHE["tokens"] == [VALID_MINT]


def test_discover_tokens_concurrent_cached_reads(monkeypatch):
    _reset_cache()

    async def runner():
        agent = DiscoveryAgent()
        agent.cache_ttl = 60.0

        now = time.time()
        identity = discovery_mod._rpc_identity(agent.rpc_url)

        async with discovery_mod._CACHE_LOCK:
            discovery_mod._CACHE.update(
                {
                    "tokens": [VALID_MINT],
                    "ts": now,
                    "limit": agent.limit,
                    "method": agent.default_method
                    or discovery_mod.DEFAULT_DISCOVERY_METHOD,
                    "rpc_identity": identity,
                    "details": {},
                }
            )

        async def fail_discover_once(self, *, method, offline, token_file):  # pragma: no cover - defensive
            del method, offline, token_file
            raise AssertionError("_discover_once should not be called when cache is valid")

        monkeypatch.setattr(DiscoveryAgent, "_discover_once", fail_discover_once)

        results = await asyncio.gather(
            agent.discover_tokens(),
            agent.discover_tokens(),
        )
        return results

    results = asyncio.run(runner())

    assert results[0] == [VALID_MINT]
    assert results[1] == [VALID_MINT]
    assert discovery_mod._CACHE["tokens"] == [VALID_MINT]


def test_cached_details_hydrate_with_weights(monkeypatch):
    _reset_cache()

    agent = DiscoveryAgent()
    agent.cache_ttl = 60.0
    identity = discovery_mod._rpc_identity(agent.rpc_url)
    now = time.time()
    features = {"alpha": 1.0}

    async def fail_discover_once(self, *, method, offline, token_file):  # pragma: no cover
        raise AssertionError("_discover_once should not be called when cache is valid")

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fail_discover_once)
    monkeypatch.setattr(
        discovery_mod,
        "get_scoring_context",
        lambda: (0.0, {"alpha": 2.0}),
    )

    async def seed_cache():
        async with discovery_mod._CACHE_LOCK:
            discovery_mod._CACHE.update(
                {
                    "tokens": [VALID_MINT],
                    "details": {VALID_MINT: {"score_features": features}},
                    "ts": now,
                    "limit": agent.limit,
                    "method": agent.default_method,
                    "rpc_identity": identity,
                }
            )

    asyncio.run(seed_cache())

    tokens = asyncio.run(agent.discover_tokens())

    assert tokens == [VALID_MINT]
    detail = agent.last_details[VALID_MINT]
    expected_score = 1.0 / (1.0 + math.exp(-2.0))
    assert math.isclose(detail["score"], expected_score, rel_tol=1e-6)
    assert detail["cached"] is True
    assert detail["cache_stale"] is False
    assert "cache" in detail["sources"]
    assert "cache:stale" not in detail["sources"]
    assert detail["score_breakdown"][0]["weight"] == 2.0
    assert detail["score_breakdown"][0]["value"] == 1.0


def test_stale_cache_fallback_preserves_metadata(monkeypatch):
    _reset_cache()

    agent = DiscoveryAgent()
    agent.cache_ttl = 0.5
    agent.max_attempts = 1
    identity = discovery_mod._rpc_identity(agent.rpc_url)
    stale_ts = time.time() - 10.0
    features = {"alpha": 0.5}

    async def empty_discover(self, *, method, offline, token_file):
        return [], {}

    async def passthrough_social(self, tokens, details, *, offline=False):
        return tokens, details or {}

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", empty_discover)
    monkeypatch.setattr(DiscoveryAgent, "_apply_social_mentions", passthrough_social)
    monkeypatch.setattr(
        discovery_mod,
        "get_scoring_context",
        lambda: (0.0, {"alpha": 2.0}),
    )

    async def seed_cache():
        async with discovery_mod._CACHE_LOCK:
            discovery_mod._CACHE.update(
                {
                    "tokens": [VALID_MINT],
                    "details": {VALID_MINT: {"score_features": features}},
                    "ts": stale_ts,
                    "limit": agent.limit,
                    "method": agent.default_method,
                    "rpc_identity": identity,
                }
            )

    asyncio.run(seed_cache())

    tokens = asyncio.run(agent.discover_tokens())

    assert tokens == [VALID_MINT]
    detail = agent.last_details[VALID_MINT]
    expected_score = 1.0 / (1.0 + math.exp(-1.0))
    assert math.isclose(detail["score"], expected_score, rel_tol=1e-6)
    assert detail["cached"] is True
    assert detail["cache_stale"] is True
    assert detail["fallback_reason"] == "cache"
    sources = detail["sources"]
    assert "cache" in sources and "cache:stale" in sources and "fallback" in sources
    assert agent.last_fallback_used is True


def test_fallback_results_do_not_populate_cache(monkeypatch):
    _reset_cache()

    fallback_tokens = ["Fallback111111111111111111111111111111111111111"]
    call_counter = {"value": 0}

    async def fail_discover_once(self, *, method, offline, token_file):
        del method, offline, token_file
        call_counter["value"] += 1
        return [], {}

    def fake_fallback_tokens(self):
        return list(fallback_tokens)

    def fake_static_fallback_tokens(self):
        raise AssertionError("static fallback should not be used when fallback tokens exist")

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fail_discover_once)
    monkeypatch.setattr(DiscoveryAgent, "_fallback_tokens", fake_fallback_tokens)
    monkeypatch.setattr(DiscoveryAgent, "_static_fallback_tokens", fake_static_fallback_tokens)

    agent = DiscoveryAgent()
    agent.cache_ttl = 60.0
    agent.max_attempts = 1

    async def runner():
        first = await agent.discover_tokens()
        second = await agent.discover_tokens()
        return first, second

    first, second = asyncio.run(runner())

    assert first == fallback_tokens
    assert second == fallback_tokens
    assert call_counter["value"] == 2
    assert discovery_mod._CACHE["tokens"] == []
    assert discovery_mod._CACHE["ts"] == 0.0
    assert agent.last_details[first[0]]["fallback_reason"] == "cache"


def test_cached_fallback_cycle_clears_cache(monkeypatch):
    _reset_cache()

    agent = DiscoveryAgent()
    agent.max_attempts = 1

    cached_identity = discovery_mod._rpc_identity(agent.rpc_url)
    cached_tokens = [VALID_MINT]
    cached_details = {VALID_MINT: {"fallback_reason": "cache"}}
    cached_ts = time.time() - (agent.cache_ttl + 5.0)
    discovery_mod._CACHE.update(
        {
            "tokens": list(cached_tokens),
            "ts": cached_ts,
            "limit": agent.limit,
            "method": "websocket",
            "rpc_identity": cached_identity,
            "details": cached_details,
        }
    )

    async def fail_discover_once(self, *, method, offline, token_file):
        del method, offline, token_file
        return [], {}

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fail_discover_once)

    tokens = asyncio.run(agent.discover_tokens())

    assert tokens == cached_tokens
    assert agent.last_fallback_used is True
    assert discovery_mod._CACHE["tokens"] == []
    assert discovery_mod._CACHE["details"] == {}


def test_merge_failure_cache_fallback_details(monkeypatch):
    _reset_cache()

    fallback_tokens = [VALID_MINT]

    async def empty_mempool(self):
        return [], {}

    def fake_fallback_tokens(self):
        return list(fallback_tokens)

    def fake_static_fallback_tokens(self):
        raise AssertionError("static fallback should not be used")

    monkeypatch.setattr(DiscoveryAgent, "_collect_mempool", empty_mempool)
    monkeypatch.setattr(DiscoveryAgent, "_fallback_tokens", fake_fallback_tokens)
    monkeypatch.setattr(DiscoveryAgent, "_static_fallback_tokens", fake_static_fallback_tokens)

    agent = DiscoveryAgent()

    tokens, details = asyncio.run(agent._fallback_after_merge_failure())

    assert tokens == fallback_tokens
    assert details[tokens[0]]["fallback_reason"] == "cache"


def test_merge_failure_static_fallback_details(monkeypatch):
    _reset_cache()

    fallback_tokens: list[str] = []
    static_tokens = [VALID_MINT]
    discovery_mod._CACHE["tokens"] = [VALID_MINT]

    async def empty_mempool(self):
        return [], {}

    def fake_fallback_tokens(self):
        return list(fallback_tokens)

    def fake_static_fallback_tokens(self):
        return list(static_tokens)

    monkeypatch.setattr(DiscoveryAgent, "_collect_mempool", empty_mempool)
    monkeypatch.setattr(DiscoveryAgent, "_fallback_tokens", fake_fallback_tokens)
    monkeypatch.setattr(DiscoveryAgent, "_static_fallback_tokens", fake_static_fallback_tokens)

    agent = DiscoveryAgent()

    tokens, details = asyncio.run(agent._fallback_after_merge_failure())

    assert tokens == static_tokens
    assert details[tokens[0]]["fallback_reason"] == "static"
    _reset_cache()


def test_offline_discovery_skips_social_mentions(monkeypatch, caplog):
    _reset_cache()

    async def fail_collect_social(self):
        raise AssertionError("social mentions should not be collected offline")

    monkeypatch.setattr(DiscoveryAgent, "_collect_social_mentions", fail_collect_social)

    async def fail_scan_tokens_async(*_a, **_k):
        raise AssertionError("scan_tokens_async should not be called offline")

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.scan_tokens_async",
        fail_scan_tokens_async,
    )

    agent = DiscoveryAgent()
    agent.news_feeds = ["https://example.com/feed"]
    cached_identity = discovery_mod._rpc_identity(agent.rpc_url)
    discovery_mod._CACHE.update(
        {
            "tokens": [VALID_MINT],
            "ts": time.time(),
            "limit": agent.limit,
            "method": "",
            "rpc_identity": cached_identity,
            "details": {},
        }
    )

    with caplog.at_level("INFO"):
        tokens = asyncio.run(agent.discover_tokens(offline=True))

    assert tokens == [VALID_MINT]
    assert "offline mode active" in caplog.text
    assert "skipping social mentions" in caplog.text


def test_offline_discovery_returns_static_fallback(monkeypatch, caplog):
    _reset_cache()

    async def fail_scan_tokens_async(*_a, **_k):
        raise AssertionError("scan_tokens_async should not be called offline")

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.scan_tokens_async",
        fail_scan_tokens_async,
    )

    agent = DiscoveryAgent()

    with caplog.at_level("INFO"):
        tokens = asyncio.run(agent.discover_tokens(offline=True))

    expected = [
        discovery_mod.canonical_mint(tok)
        for tok in discovery_mod._STATIC_FALLBACK
        if discovery_mod.validate_mint(discovery_mod.canonical_mint(tok))
    ][: agent.limit]
    assert tokens == expected
    assert "offline mode active" in caplog.text
    assert tokens, "expected static fallback tokens"
    assert agent.last_details[tokens[0]]["fallback_reason"] == "static"


def test_offline_discovery_logs_fallback_reason(monkeypatch):
    _reset_cache()
    agent = DiscoveryAgent()

    monkeypatch.setattr(agent, "_fallback_tokens", lambda: [VALID_MINT])
    monkeypatch.setattr(agent, "_static_fallback_tokens", lambda: [])

    async def passthrough_social(self, tokens, details, *, offline=False):
        del self, offline
        return tokens, details or {}

    monkeypatch.setattr(DiscoveryAgent, "_apply_social_mentions", passthrough_social)

    captured_details: list[str] = []

    def capture_runtime_log(topic, payload):
        if topic == "runtime.log":
            captured_details.append(payload.detail)

    monkeypatch.setattr(discovery_mod, "publish", capture_runtime_log)

    tokens = asyncio.run(agent.discover_tokens(offline=True))

    assert tokens == [VALID_MINT]
    assert captured_details, "expected runtime.log entry"
    assert any("fallback=cache" in entry for entry in captured_details)


def test_discovery_logs_fallback_reason(monkeypatch):
    _reset_cache()
    agent = DiscoveryAgent()
    agent.max_attempts = 1
    agent.backoff = 0.0

    async def no_tokens(self, *, method, offline, token_file):
        del self, method, offline, token_file
        return [], {}

    async def passthrough_social(self, tokens, details, *, offline=False):
        del self, offline
        return tokens, details or {}

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", no_tokens)
    monkeypatch.setattr(DiscoveryAgent, "_apply_social_mentions", passthrough_social)

    monkeypatch.setattr(agent, "_fallback_tokens", lambda: [])
    monkeypatch.setattr(agent, "_static_fallback_tokens", lambda: [VALID_MINT])

    captured_details: list[str] = []

    def capture_runtime_log(topic, payload):
        if topic == "runtime.log":
            captured_details.append(payload.detail)

    monkeypatch.setattr(discovery_mod, "publish", capture_runtime_log)

    tokens = asyncio.run(agent.discover_tokens())

    assert tokens == [VALID_MINT]
    assert captured_details, "expected runtime.log entry"
    assert any("fallback=static" in entry for entry in captured_details)
