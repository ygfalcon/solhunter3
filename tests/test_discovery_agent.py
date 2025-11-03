import asyncio
import os
import time
from typing import Any

from solhunter_zero.agents.discovery import DiscoveryAgent
from solhunter_zero.scanner_common import DEFAULT_SOLANA_RPC, DEFAULT_SOLANA_WS


async def fake_stream(url, **_):
    yield {"address": "tok", "score": 12.0}


def test_stream_mempool_events(monkeypatch):
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
    assert data["address"] == "tok"


def test_stream_mempool_events_prefers_websocket(monkeypatch):
    captured: dict[str, Any] = {}

    async def capture_stream(url, **_):
        captured["url"] = url
        yield {"address": "tok"}

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.stream_ranked_mempool_tokens_with_depth",
        capture_stream,
    )
    monkeypatch.setenv(
        "SOLANA_WS_URL",
        "wss://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
    )

    agent = DiscoveryAgent()

    async def run():
        gen = agent.stream_mempool_events(
            "https://mainnet.helius-rpc.com/?api-key=override"
        )
        data = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return data

    data = asyncio.run(run())
    assert data["address"] == "tok"
    assert captured["url"].startswith("wss://")


def test_discovery_agent_sets_helius_defaults(monkeypatch):
    monkeypatch.delenv("SOLANA_RPC_URL", raising=False)
    monkeypatch.delenv("SOLANA_WS_URL", raising=False)
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.config.get_solana_ws_url", lambda: None
    )

    agent = DiscoveryAgent()

    assert agent.rpc_url == DEFAULT_SOLANA_RPC
    assert agent.ws_url == DEFAULT_SOLANA_WS
    assert os.getenv("SOLANA_RPC_URL") == DEFAULT_SOLANA_RPC
    assert os.getenv("SOLANA_WS_URL") == DEFAULT_SOLANA_WS


def test_discover_tokens_retries_on_empty_scan(monkeypatch, caplog):
    calls = []

    async def fake_scan(*a, **k):
        calls.append(None)
        return [] if len(calls) == 1 else ["tok"]

    sleep_calls: list[float] = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.scan_tokens_async", fake_scan
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.asyncio.sleep", fake_sleep
    )
    monkeypatch.setenv("TOKEN_DISCOVERY_BACKOFF", "1.5")

    agent = DiscoveryAgent()

    async def run():
        with caplog.at_level("WARNING"):
            return await agent.discover_tokens(offline=False)

    tokens = asyncio.run(run())
    assert tokens == ["tok"]
    assert len(calls) == 2
    assert sleep_calls == [1.5]
    assert "No tokens discovered" in caplog.text


def test_discover_tokens_retries_on_empty_merge(monkeypatch, caplog):
    calls = []
    captured: dict[str, Any] = {}

    async def fake_merge(url, *, limit=None, mempool_threshold=0.0, ws_url=None):
        calls.append(None)
        captured["limit"] = limit
        if len(calls) == 1:
            return []
        return [{"address": "tok"}]

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
    assert tokens == ["tok"]
    assert len(calls) == 2
    assert sleep_calls == [0.0]
    assert "No tokens discovered" in caplog.text
    assert captured["limit"] == agent.limit


def test_offline_discovery_uses_fallback(monkeypatch):
    import solhunter_zero.agents.discovery as discovery_mod

    async def fail(*_args, **_kwargs):  # pragma: no cover - defensive
        raise AssertionError("network method should not be called")

    monkeypatch.setattr(discovery_mod, "scan_tokens_async", fail)
    monkeypatch.setattr(discovery_mod, "merge_sources", fail)
    monkeypatch.setattr(discovery_mod, "scan_tokens_onchain", fail)
    monkeypatch.setattr(discovery_mod, "enrich_tokens_async", fail)
    monkeypatch.setattr(discovery_mod, "_CACHE", {"tokens": [], "ts": 0.0, "limit": 0, "method": ""})
    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "0")
    monkeypatch.setenv("TOKEN_DISCOVERY_RETRIES", "1")

    agent = DiscoveryAgent()

    tokens = asyncio.run(agent.discover_tokens(offline=True))

    assert tokens == discovery_mod._STATIC_FALLBACK[: agent.limit]
    assert agent.last_method == "file"


def test_offline_discovery_with_network_override(monkeypatch):
    import solhunter_zero.agents.discovery as discovery_mod

    async def fail(*_args, **_kwargs):  # pragma: no cover - defensive
        raise AssertionError("network method should not be called")

    monkeypatch.setattr(discovery_mod, "scan_tokens_async", fail)
    monkeypatch.setattr(discovery_mod, "merge_sources", fail)
    monkeypatch.setattr(discovery_mod, "scan_tokens_onchain", fail)
    monkeypatch.setattr(discovery_mod, "enrich_tokens_async", fail)
    monkeypatch.setattr(discovery_mod, "_CACHE", {"tokens": [], "ts": 0.0, "limit": 0, "method": ""})
    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "0")
    monkeypatch.setenv("TOKEN_DISCOVERY_RETRIES", "1")

    agent = DiscoveryAgent()

    tokens = asyncio.run(agent.discover_tokens(offline=True, method="helius"))

    assert tokens == discovery_mod._STATIC_FALLBACK[: agent.limit]
    assert agent.last_method == "file"


def test_token_file_cache_updates_on_change(tmp_path, monkeypatch):
    import solhunter_zero.agents.discovery as discovery_mod

    path = tmp_path / "tokens.txt"
    monkeypatch.setattr(
        discovery_mod,
        "_CACHE",
        {
            "tokens": [],
            "ts": 0.0,
            "limit": 0,
            "method": "",
            "token_file": None,
            "token_file_mtime": None,
        },
    )
    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "120")

    agent = DiscoveryAgent()

    path.write_text(
        "TokA11111111111111111111111111111111111111\n"
        "TokB22222222222222222222222222222222222222\n",
        encoding="utf-8",
    )
    first = asyncio.run(agent.discover_tokens(offline=True, token_file=str(path)))
    assert first == [
        "TokA11111111111111111111111111111111111111",
        "TokB22222222222222222222222222222222222222",
    ]

    original_mtime = path.stat().st_mtime
    path.write_text(
        "TokC33333333333333333333333333333333333333\n",
        encoding="utf-8",
    )
    os.utime(path, (original_mtime + 10, original_mtime + 10))

    second = asyncio.run(agent.discover_tokens(offline=True, token_file=str(path)))
    assert second == ["TokC33333333333333333333333333333333333333"]


def test_clone_fetches_bypass_global_cache(monkeypatch):
    import solhunter_zero.agents.discovery as discovery_mod

    call_counter = {"count": 0}

    async def fake_discover_once(self, *, method, offline, token_file):
        call_counter["count"] += 1
        token = f"Tok{call_counter['count']}"
        return [token], {}

    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "300")
    monkeypatch.setattr(
        discovery_mod,
        "_CACHE",
        {
            "tokens": [],
            "ts": 0.0,
            "limit": 0,
            "method": "",
            "token_file": None,
            "token_file_mtime": None,
        },
    )
    monkeypatch.setattr(DiscoveryAgent, "_discover_once", fake_discover_once)

    agent = DiscoveryAgent()

    discovery_mod._CACHE["tokens"] = ["CachedToken"]
    discovery_mod._CACHE["ts"] = time.time()
    discovery_mod._CACHE["limit"] = agent.limit
    discovery_mod._CACHE["method"] = agent.default_method

    first = asyncio.run(agent.discover_tokens(use_cache=False))
    second = asyncio.run(agent.discover_tokens(use_cache=False))

    assert first == ["Tok1"]
    assert second == ["Tok2"]
    assert call_counter["count"] == 2
    assert discovery_mod._CACHE["tokens"] == ["CachedToken"]

    discovery_mod._CACHE["ts"] = 0.0

    third = asyncio.run(agent.discover_tokens())
    assert third == ["Tok3"]
    assert discovery_mod._CACHE["tokens"] == ["Tok3"]

    cached = asyncio.run(agent.discover_tokens())
    assert cached == ["Tok3"]
    assert call_counter["count"] == 3


def test_discovery_agent_passes_ws_url(monkeypatch):
    called: dict[str, Any] = {}

    async def fake_merge(url, *, limit=None, mempool_threshold=0.0, ws_url=None):
        called["rpc"] = url
        called["limit"] = limit
        called["ws"] = ws_url
        return [{"address": "tok"}]

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.config.get_solana_ws_url",
        lambda: "wss://derived.example",
    )
    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "0")

    agent = DiscoveryAgent()

    async def run():
        return await agent.discover_tokens(method="websocket")

    tokens = asyncio.run(run())

    assert tokens == ["tok"]
    assert called["rpc"] == agent.rpc_url
    assert called["ws"] == "wss://derived.example"
    assert called["limit"] == agent.limit


def test_discovery_agent_custom_limit_websocket(monkeypatch):
    captured: dict[str, Any] = {}

    async def fake_merge(url, *, limit=None, mempool_threshold=0.0, ws_url=None):
        captured["rpc"] = url
        captured["limit"] = limit
        captured["ws"] = ws_url
        return [{"address": f"mint{i}"} for i in range(10)]

    monkeypatch.setattr(
        "solhunter_zero.agents.discovery.merge_sources", fake_merge
    )
    monkeypatch.setenv("DISCOVERY_LIMIT", "5")
    monkeypatch.setenv("DISCOVERY_CACHE_TTL", "0")

    agent = DiscoveryAgent()

    async def run():
        return await agent.discover_tokens(method="websocket")

    tokens = asyncio.run(run())

    assert agent.limit == 5
    assert captured["limit"] == 5
    assert len(tokens) == 5
    assert tokens == [f"mint{i}" for i in range(5)]
