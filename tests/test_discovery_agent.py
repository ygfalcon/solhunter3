import asyncio
import sys
import time
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
        {"tokens": [], "ts": 0.0, "limit": 0, "method": "", "rpc_identity": ""}
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

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("DEBUG"):
            return await agent._collect_mempool()

    start = time.perf_counter()
    tokens, details = asyncio.run(run())
    elapsed = time.perf_counter() - start

    assert tokens == []
    assert details == {}
    assert elapsed < 0.5
    assert "Mempool stream timed out" in caplog.text


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


def test_discover_tokens_offline_falls_back_without_network(monkeypatch, caplog):
    _reset_cache()

    fallback_calls: list[int] = []

    async def fail_discover_once(*_args, **_kwargs):
        raise AssertionError("network discovery should not run in offline fallback")

    def fake_fallback(self):
        fallback_calls.append(1)
        return [VALID_MINT]

    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fail_discover_once)
    monkeypatch.setattr(DiscoveryAgent, "_fallback_tokens", fake_fallback)

    agent = DiscoveryAgent()

    with caplog.at_level("WARNING"):
        tokens = asyncio.run(agent.discover_tokens(offline=True))

    assert tokens == [VALID_MINT]
    assert fallback_calls == [1]
    assert agent.last_method == "offline-static"
    assert "offline mode using cached/static fallback seeds" in caplog.text


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


def test_discovery_cache_is_scoped_per_rpc(monkeypatch):
    _reset_cache()
    monkeypatch.setenv("SOLANA_RPC_URL", "https://mainnet.rpc.local")

    call_count = {"value": 0}

    async def fake_discover_once(self, *, method, offline, token_file):
        call_count["value"] += 1
        return [VALID_MINT], {}

    async def passthrough_social(self, tokens, details):
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
