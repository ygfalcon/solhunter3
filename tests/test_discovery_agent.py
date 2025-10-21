import asyncio
import sys
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
    discovery_mod._CACHE.update({"tokens": [], "ts": 0.0, "limit": 0, "method": ""})


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
            return await agent.discover_tokens(offline=True)

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
