import asyncio
import types

import aiohttp
import pytest

from aiohttp import ClientTimeout


class _DummyResponse:
    def __init__(self, status, *, text="", headers=None, reason=""):
        self.status = status
        self._text = text
        self.headers = headers or {}
        self.reason = reason

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, *args, **kwargs):  # pragma: no cover - not expected in tests
        raise AssertionError("json() should not be called on error responses")

    async def text(self):
        return self._text

    def raise_for_status(self):  # pragma: no cover - not expected in tests
        raise AssertionError("raise_for_status() should not be called for dummy error responses")


class _DummySession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, *args, **kwargs):
        if not self._responses:
            raise AssertionError("No more dummy responses available")
        response = self._responses.pop(0)
        self._responses.append(response)
        return response

from solhunter_zero import token_discovery as td


def _configure_env(monkeypatch, **values):
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    td.refresh_runtime_values()


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_token_discovery_importable_under_pytest():
    import importlib

    reloaded = importlib.reload(td)
    assert reloaded is td


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_prioritises_scores(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    bird1 = "So11111111111111111111111111111111111111112"
    bird2 = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"

    async def fake_bird(*, limit=None):
        _ = limit
        return [
            {
                "address": bird1,
                "name": "Bird One",
                "symbol": "B1",
                "liquidity": 300000,
                "volume": 200000,
                "price": 1.2,
                "price_change": 5.0,
                "sources": ["birdeye"],
            },
            {
                "address": bird2,
                "name": "Bird Two",
                "symbol": "B2",
                "liquidity": 90000,
                "volume": 80000,
                "price": 0.8,
                "price_change": -2.0,
                "sources": ["birdeye"],
            },
        ]

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)
    monkeypatch.setattr(td, "fetch_trending_tokens_async", lambda: [bird2, "trend_only"])
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "test-key")
    _configure_env(
        monkeypatch,
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        TRENDING_MIN_LIQUIDITY_USD="0",
    )
    monkeypatch.setattr(td, "is_valid_solana_mint", lambda _addr: True)

    async def _no_tokens(*, session=None):
        _ = session
        return []

    async def _noop_enrich(_candidates, *, addresses=None):
        _ = addresses
        return None

    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", _no_tokens)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", _no_tokens)
    monkeypatch.setattr(td, "_fetch_dexlab_tokens", _no_tokens)
    monkeypatch.setattr(td, "_enrich_with_solscan", _noop_enrich)

    mem_mint = "E7vCh2szgdWzxubAEANe1yoyWP7JfVv5sWpQXXAUP8Av"

    async def fake_mempool(_rpc_url, threshold):
        _ = threshold
        yield {
            "address": mem_mint,
            "score": 2.0,
            "volume": 15000,
                "liquidity": 40000,
                "momentum": 0.2,
            }

    async def mempool_gen(rpc_url, threshold):
        agen = fake_mempool(rpc_url, threshold)
        async for item in agen:
            yield item

    monkeypatch.setattr(td, "stream_ranked_mempool_tokens_with_depth", mempool_gen)

    batches: list[list[dict]] = []
    async for batch in td.discover_candidates(
        "https://rpc", limit=3, mempool_threshold=0.0
    ):
        batches.append(batch)

    results = batches[-1] if batches else []
    addresses = [r["address"] for r in results]

    assert batches, "expected at least one incremental batch"

    assert len(results) <= 3
    assert addresses[0] == mem_mint
    assert set(addresses) >= {bird1, bird2, mem_mint}
    assert results[0]["sources"] == ["mempool"]
    assert any("birdeye" in r["sources"] for r in results)


def test_fetch_birdeye_tokens_missing_key_raises_configuration_error(monkeypatch, caplog):
    td._BIRDEYE_CACHE.clear()
    _configure_env(monkeypatch, DISCOVERY_ENABLE_MEMPOOL="0")
    monkeypatch.setattr(td, "_BIRDEYE_DISABLED_INFO", False)
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "")

    async def run():
        return await td._fetch_birdeye_tokens(limit=5)

    with caplog.at_level("WARNING", logger="solhunter_zero.token_discovery"):
        with pytest.raises(td.DiscoveryConfigurationError) as excinfo:
            asyncio.run(run())

    assert "BirdEye API key missing" in str(excinfo.value)
    assert "BirdEye API key missing" in caplog.text


@pytest.mark.anyio("asyncio")
async def test_fetch_birdeye_tokens_raises_config_error_on_401(monkeypatch):
    td._BIRDEYE_CACHE.clear()
    monkeypatch.setattr(td, "_BIRDEYE_DISABLED_INFO", False)
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "bad-key")

    response = _DummyResponse(401, text="invalid api key", reason="Unauthorized")
    session = _DummySession([response])

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return session

    monkeypatch.setattr(td, "get_session", fake_get_session)

    with pytest.raises(td.DiscoveryConfigurationError) as excinfo:
        await td._fetch_birdeye_tokens(limit=5)

    assert excinfo.value.source == "birdeye"
    assert "401" in str(excinfo.value)
    remediation = excinfo.value.remediation or ""
    assert "api key" in remediation.lower()


@pytest.mark.anyio("asyncio")
async def test_fetch_birdeye_tokens_raises_config_error_on_403(monkeypatch):
    td._BIRDEYE_CACHE.clear()
    monkeypatch.setattr(td, "_BIRDEYE_DISABLED_INFO", False)
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "bad-key")

    response = _DummyResponse(403, text="forbidden", reason="Forbidden")
    session = _DummySession([response])

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return session

    monkeypatch.setattr(td, "get_session", fake_get_session)

    with pytest.raises(td.DiscoveryConfigurationError) as excinfo:
        await td._fetch_birdeye_tokens(limit=5)

    assert excinfo.value.source == "birdeye"
    assert "403" in str(excinfo.value)
    remediation = excinfo.value.remediation or ""
    assert "access" in remediation.lower()


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_limits_birdeye_fetches(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="0",
        DISCOVERY_ENABLE_RAYDIUM="0",
        DISCOVERY_ENABLE_METEORA="0",
        DISCOVERY_ENABLE_DEXLAB="0",
        DISCOVERY_ENABLE_ORCA="0",
        DISCOVERY_ENABLE_SOLSCAN="0",
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        TRENDING_MIN_LIQUIDITY_USD="0",
    )
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "test-key")
    monkeypatch.setattr(td, "_BIRDEYE_DISABLED_INFO", False)
    monkeypatch.setattr(td, "is_valid_solana_mint", lambda _addr: True)

    captured_limits: list[int | None] = []

    async def fake_bird(*, limit=None):
        captured_limits.append(limit)
        total = 15
        tokens = []
        for idx in range(total):
            tokens.append(
                {
                    "address": f"Bird{idx:02d}",
                    "name": f"Bird {idx:02d}",
                    "symbol": f"B{idx:02d}",
                    "liquidity": 100000,
                    "volume": 120000,
                    "price": 1.0 + idx / 100.0,
                    "price_change": 1.0,
                    "sources": ["birdeye"],
                }
            )
        max_items = limit if limit is not None else total
        return tokens[:max_items]

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)

    batches: list[list[dict]] = []
    async for batch in td.discover_candidates(
        "https://rpc", limit=10, mempool_threshold=0.0
    ):
        batches.append(batch)

    assert batches, "expected discovery batches"
    final = batches[-1]

    assert captured_limits == [10]
    assert len(final) == 10
    assert all("birdeye" in item.get("sources", []) for item in final)


@pytest.mark.anyio("asyncio")
async def test_fetch_birdeye_tokens_parses_payload_and_caches(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        DISCOVERY_PAGE_SIZE="2",
    )
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "key")
    monkeypatch.setattr(td, "_BIRDEYE_DISABLED_INFO", False)
    monkeypatch.setattr(td, "is_valid_solana_mint", lambda _addr: True)

    assert td.SETTINGS.min_volume == 0
    assert td.SETTINGS.min_liquidity == 0

    payload = {
        "data": {
            "tokens": [
                {
                    "address": "MintBirdEyeAAA111111111111111111111111111",
                    "symbol": "BIRD1",
                    "name": "Bird One",
                    "v24hUSD": "1000.5",
                    "liquidity": "2000.75",
                    "price": "1.2345",
                    "v24hChangePercent": "4.56",
                },
                {
                    "address": "MintBirdEyeBBB222222222222222222222222222",
                    "symbol": "BIRD2",
                    "name": "Bird Two",
                    "v24hUSD": "500.25",
                    "liquidity": "1500.25",
                    "price": "0.9876",
                    "v24hChangePercent": "-1.23",
                },
            ],
            "total": 2,
        }
    }

    class _JSONResponse:
        status = 200
        headers: dict = {}

        def __init__(self, data):
            self._data = data

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return self._data

        def raise_for_status(self):
            return None

    class _JSONSession:
        def __init__(self, data):
            self._data = data
            self.calls = 0

        def get(self, url, *, params=None, headers=None):
            self.calls += 1
            assert headers is not None and headers.get("X-API-KEY") == "key"
            assert params is not None and params.get("limit") == td.SETTINGS.page_limit
            return _JSONResponse(self._data)

    session = _JSONSession(payload)

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return session

    monkeypatch.setattr(td, "get_session", fake_get_session)

    first = await td._fetch_birdeye_tokens(limit=1)

    assert session.calls == 1
    assert first == [
        {
            "address": "MintBirdEyeAAA111111111111111111111111111",
            "symbol": "BIRD1",
            "name": "Bird One",
            "liquidity": 2000.75,
            "volume": 1000.5,
            "price": 1.2345,
            "price_change": 4.56,
            "sources": ["birdeye"],
        }
    ]

    second = await td._fetch_birdeye_tokens(limit=3)

    assert second is first
    assert session.calls == 1


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_reuses_shared_session(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="1",
        DISCOVERY_ENABLE_METEORA="0",
        DISCOVERY_ENABLE_DEXLAB="0",
        DISCOVERY_ENABLE_RAYDIUM="0",
        DISCOVERY_ENABLE_ORCA="0",
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        TRENDING_MIN_LIQUIDITY_USD="0",
    )

    class FakeSession:
        def __init__(self):
            self.closed = False
            self.close_calls = 0

        def get(self, *args, **kwargs):
            raise AssertionError("HTTP requests not expected in test")

        async def close(self):
            self.closed = True
            self.close_calls += 1

    fake_session = FakeSession()

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return fake_session

    monkeypatch.setattr(td, "get_session", fake_get_session)

    async def fake_birdeye(*, limit=None):
        _ = limit
        return []

    async def fake_collect(*args, **kwargs):
        return {}

    seen_sessions = []

    async def fake_dexscreener(*, session=None):
        assert session is fake_session
        assert not fake_session.closed
        seen_sessions.append(session)
        return [
            {
                "address": "Fake111111111111111111111111111111111111111",
                "symbol": "FAKE",
                "name": "Fake Token",
                "liquidity": 150000,
                "volume": 220000,
                "price": 1.0,
                "price_change": 1.5,
            }
        ]

    async def fake_meteora(*, session=None):
        _ = session
        return []

    async def fake_dexlab(*, session=None):
        _ = session
        return []

    async def fake_enrich(*args, **kwargs):
        return None

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_birdeye)
    monkeypatch.setattr(td, "_collect_mempool_signals", fake_collect)
    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", fake_dexscreener)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", fake_meteora)
    monkeypatch.setattr(td, "_fetch_dexlab_tokens", fake_dexlab)
    monkeypatch.setattr(td, "_enrich_with_solscan", fake_enrich)

    await td.discover_candidates("https://rpc", limit=3, mempool_threshold=0.0)
    first_sessions = list(seen_sessions)
    assert first_sessions, "expected dex sources to run"
    assert all(sess is fake_session for sess in first_sessions)
    assert not fake_session.closed
    assert fake_session.close_calls == 0

    await td.discover_candidates("https://rpc", limit=3, mempool_threshold=0.0)
    second_sessions = seen_sessions[len(first_sessions) :]
    assert second_sessions, "expected dex sources to run again"
    assert all(sess is fake_session for sess in second_sessions)
    assert not fake_session.closed
    assert fake_session.close_calls == 0


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_merges_new_sources(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="1",
        DISCOVERY_ENABLE_METEORA="1",
        DISCOVERY_ENABLE_DEXLAB="1",
        DISCOVERY_ENABLE_ORCA="0",
        DISCOVERY_ENABLE_RAYDIUM="0",
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        TRENDING_MIN_LIQUIDITY_USD="0",
    )
    monkeypatch.setattr(td, "is_valid_solana_mint", lambda _addr: True)

    async def fake_bird(*, limit=None):
        return []

    async def fake_collect(*args, **kwargs):
        return {}

    dex_mint = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"
    lab_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    async def fake_dexscreener(*, session=None):
        _ = session
        return [
            {
                "address": dex_mint,
                "symbol": "DEX",
                "name": "Dex Token",
                "liquidity": 12000,
                "volume": 18000,
                "price": 1.5,
                "price_change": 2.0,
            }
        ]

    async def fake_meteora(*, session=None):
        _ = session
        return [
            {
                "address": dex_mint,
                "symbol": "MT",
                "name": "Meteora Token",
                "liquidity": 4000,
                "volume": 3000,
            }
        ]

    async def fake_dexlab(*, session=None):
        _ = session
        return [
            {
                "address": lab_mint,
                "symbol": "",
                "name": "",
                "liquidity": 0,
                "volume": 0,
            }
        ]

    async def fake_enrich(candidates, *, addresses=None):
        _ = addresses
        entry = candidates.get(lab_mint)
        if entry is not None:
            entry["symbol"] = "LAB"
            entry["name"] = "DexLab Token"
            entry.setdefault("sources", set()).add("solscan")

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)
    monkeypatch.setattr(td, "_collect_mempool_signals", fake_collect)
    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", fake_dexscreener)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", fake_meteora)
    monkeypatch.setattr(td, "_fetch_dexlab_tokens", fake_dexlab)
    monkeypatch.setattr(td, "_enrich_with_solscan", fake_enrich)

    batches: list[list[dict]] = []
    async for batch in td.discover_candidates(
        "https://rpc", limit=5, mempool_threshold=0.0
    ):
        batches.append(batch)

    assert batches, "expected partial discovery batches"

    results = batches[-1] if batches else []

    addresses = {r["address"]: r for r in results}

    assert dex_mint in addresses
    assert lab_mint in addresses

    dex_sources = set(addresses[dex_mint]["sources"])
    assert {"dexscreener", "meteora"}.issubset(dex_sources)

    lab_entry = addresses[lab_mint]
    assert "dexlab" in lab_entry["sources"]
    assert "solscan" in lab_entry["sources"]
    assert lab_entry["name"] == "DexLab Token"


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_falls_back_when_birdeye_disabled(monkeypatch, caplog):
    td._BIRDEYE_CACHE.clear()
    monkeypatch.setattr(td, "_BIRDEYE_DISABLED_INFO", False)
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "")
    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="0",
        DISCOVERY_ENABLE_RAYDIUM="0",
        DISCOVERY_ENABLE_METEORA="0",
        DISCOVERY_ENABLE_DEXLAB="0",
        DISCOVERY_ENABLE_ORCA="0",
        DISCOVERY_ENABLE_SOLSCAN="0",
    )
    monkeypatch.setenv("BIRDEYE_API_KEY", "")

    batches: list[list[dict]] = []
    with caplog.at_level("WARNING"):
        async for batch in td.discover_candidates("https://rpc", limit=3):
            batches.append(batch)
            break

    assert batches, "expected fallback batch"
    final = batches[-1]
    assert final, "fallback batch should contain entries"
    addresses = {item["address"] for item in final}
    assert "So11111111111111111111111111111111111111112" in addresses
    assert all("fallback" in item.get("sources", []) for item in final)
    assert "BirdEye discovery disabled" in caplog.text
    assert "using fallback set" in caplog.text


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_falls_back_when_birdeye_config_error(monkeypatch, caplog):
    td._BIRDEYE_CACHE.clear()
    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="0",
        DISCOVERY_ENABLE_RAYDIUM="0",
        DISCOVERY_ENABLE_METEORA="0",
        DISCOVERY_ENABLE_DEXLAB="0",
        DISCOVERY_ENABLE_ORCA="0",
        DISCOVERY_ENABLE_SOLSCAN="0",
    )
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "configured")

    async def fail_fetch(*, limit=None):
        _ = limit
        raise td.DiscoveryConfigurationError(
            "birdeye",
            "BirdEye request failed with HTTP 401: Unauthorized",
            remediation="Verify BirdEye API key",
        )

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fail_fetch)

    batches: list[list[dict]] = []
    with caplog.at_level("WARNING"):
        async for batch in td.discover_candidates("https://rpc", limit=2):
            batches.append(batch)
            break

    assert batches, "expected fallback batch"
    final = batches[-1]
    assert final, "fallback batch should contain entries"
    assert any("fallback" in entry.get("sources", []) for entry in final)
    assert "BirdEye discovery disabled due to configuration" in caplog.text


def test_warm_cache_skips_without_birdeye_key(monkeypatch):
    # Ensure environment does not provide a BirdEye key and guard short-circuits.
    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.setattr(td, "_resolve_birdeye_api_key", lambda: "")

    thread_called = {"created": False, "started": False}

    class DummyThread:
        def __init__(self, *args, **kwargs):
            thread_called["created"] = True

        def start(self):
            thread_called["started"] = True

    monkeypatch.setattr(td.threading, "Thread", DummyThread)

    td.warm_cache(rpc_url="")

    assert thread_called["created"] is False
    assert thread_called["started"] is False


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_shared_session_timeouts_and_cleanup(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="1",
        DISCOVERY_ENABLE_METEORA="1",
        DISCOVERY_ENABLE_DEXLAB="1",
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        TRENDING_MIN_LIQUIDITY_USD="0",
    )

    async def fake_bird(*, limit=None):
        return []

    async def fake_enrich(_candidates, *, addresses=None):
        _ = addresses
        return None

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)
    monkeypatch.setattr(td, "_enrich_with_solscan", fake_enrich)

    dex_mint = "GoNKc7dBq2oNuvqNEBQw9u5VnXNmeZLk52BEQcJkySU"
    meteora_mint = "E7vCh2szgdWzxubAEANe1yoyWP7JfVv5sWpQXXAUP8Av"
    dexlab_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

    class FakeSession:
        def __init__(self):
            self.enter_count = 0
            self.exit_count = 0
            self.closed = False

        async def __aenter__(self):
            self.enter_count += 1
            return self

        async def __aexit__(self, exc_type, exc, tb):
            self.exit_count += 1
            self.closed = True
            return False

    fake_session = FakeSession()

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return fake_session

    monkeypatch.setattr(td, "get_session", fake_get_session)
    assert td.get_session is fake_get_session

    captured_calls: list[dict] = []

    async def fake_dexscreener(*, session=None):
        captured_calls.append(
            {
                "url": td.SETTINGS.dexscreener_url,
                "timeout": ClientTimeout(total=td.SETTINGS.dexscreener_timeout),
                "session": session,
            }
        )
        return [
            {
                "address": dex_mint,
                "symbol": "DEX",
                "name": "Dex Token",
                "liquidity": 12000,
                "volume": 18000,
                "price": 1.23,
                "price_change": 2.0,
                "sources": ["dexscreener"],
            }
        ]

    async def fake_meteora(*, session=None):
        captured_calls.append(
            {
                "url": td.SETTINGS.meteora_pools_url,
                "timeout": ClientTimeout(total=td.SETTINGS.meteora_timeout),
                "session": session,
            }
        )
        return [
            {
                "address": meteora_mint,
                "symbol": "MT",
                "name": "Meteora Token",
                "liquidity": 9000,
                "volume": 7000,
                "sources": ["meteora"],
            }
        ]

    async def fake_dexlab(*, session=None):
        captured_calls.append(
            {
                "url": td.SETTINGS.dexlab_list_url,
                "timeout": ClientTimeout(total=td.SETTINGS.dexlab_timeout),
                "session": session,
            }
        )
        return [
            {
                "address": dexlab_mint,
                "symbol": "DL",
                "name": "DexLab Token",
                "liquidity": 5000,
                "volume": 4000,
                "sources": ["dexlab"],
            }
        ]

    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", fake_dexscreener)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", fake_meteora)
    monkeypatch.setattr(td, "_fetch_dexlab_tokens", fake_dexlab)

    batches: list[list[dict]] = []
    async for batch in td.discover_candidates(
        "https://rpc", limit=5, mempool_threshold=0.0
    ):
        batches.append(batch)

    assert batches, "expected staged discovery batches"

    results = batches[-1] if batches else []

    assert captured_calls, "expected shared session HTTP calls"

    urls_seen = {call["url"] for call in captured_calls}
    expected_urls = {
        td.SETTINGS.dexscreener_url,
        td.SETTINGS.meteora_pools_url,
        td.SETTINGS.dexlab_list_url,
    }
    assert expected_urls <= urls_seen

    timeouts: dict[str, ClientTimeout] = {}
    for call in captured_calls:
        url = call["url"]
        if url in expected_urls and url not in timeouts:
            timeouts[url] = call["timeout"]
            assert call["session"] is fake_session

    assert set(timeouts) == expected_urls
    assert isinstance(timeouts[td.SETTINGS.dexscreener_url], ClientTimeout)
    assert isinstance(timeouts[td.SETTINGS.meteora_pools_url], ClientTimeout)
    assert isinstance(timeouts[td.SETTINGS.dexlab_list_url], ClientTimeout)
    assert timeouts[td.SETTINGS.dexscreener_url].total == td.SETTINGS.dexscreener_timeout
    assert timeouts[td.SETTINGS.meteora_pools_url].total == td.SETTINGS.meteora_timeout
    assert timeouts[td.SETTINGS.dexlab_list_url].total == td.SETTINGS.dexlab_timeout

    assert fake_session.enter_count == 1
    assert fake_session.exit_count == 1
    assert fake_session.closed is True

    # The shared session requests should not close prematurely.
    addresses = {entry["address"] for entry in results}


@pytest.mark.anyio("asyncio")
async def test_discover_candidates_shared_session_failure_falls_back(monkeypatch, caplog):
    td._BIRDEYE_CACHE.clear()

    _configure_env(
        monkeypatch,
        DISCOVERY_ENABLE_MEMPOOL="0",
        DISCOVERY_ENABLE_DEXSCREENER="1",
        DISCOVERY_ENABLE_METEORA="1",
        DISCOVERY_ENABLE_DEXLAB="0",
        DISCOVERY_ENABLE_ORCA="0",
        DISCOVERY_ENABLE_RAYDIUM="0",
        DISCOVERY_ENABLE_SOLSCAN="0",
        DISCOVERY_MIN_VOLUME_USD="0",
        DISCOVERY_MIN_LIQUIDITY_USD="0",
        TRENDING_MIN_LIQUIDITY_USD="0",
    )

    async def fake_bird(*, limit=None):
        _ = limit
        return []

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_bird)

    class FakeSession:
        def __init__(self) -> None:
            self.closed = False
            self.close_calls = 0

        async def close(self) -> None:
            self.close_calls += 1
            self.closed = True

    partial_session = FakeSession()
    per_task_sessions: list[FakeSession] = []
    call_state = {"count": 0}

    async def fake_get_session(*, timeout=None):
        _ = timeout
        call_state["count"] += 1
        if call_state["count"] == 1:
            err = RuntimeError("boom")
            err.partial_session = partial_session
            raise err
        session = FakeSession()
        per_task_sessions.append(session)
        return session

    monkeypatch.setattr(td, "get_session", fake_get_session)
    assert td.get_session is fake_get_session

    captured_sessions: list[aiohttp.ClientSession | None] = []

    async def fake_dexscreener(*, session=None):
        captured_sessions.append(session)
        return [
            {
                "address": "DexSharedFailureMint",
                "symbol": "DEX",
                "name": "Dex Token",
                "liquidity": 15000,
                "volume": 22000,
                "sources": ["dexscreener"],
            }
        ]

    async def fake_meteora(*, session=None):
        captured_sessions.append(session)
        return [
            {
                "address": "MeteoraSharedFailureMint",
                "symbol": "MT",
                "name": "Meteora Token",
                "liquidity": 9000,
                "volume": 7000,
                "sources": ["meteora"],
            }
        ]

    monkeypatch.setattr(td, "_fetch_dexscreener_tokens", fake_dexscreener)
    monkeypatch.setattr(td, "_fetch_meteora_tokens", fake_meteora)

    batches: list[list[dict]] = []
    with caplog.at_level("WARNING"):
        async for batch in td.discover_candidates("https://rpc", limit=4, mempool_threshold=0.0):
            batches.append(batch)
            if batch:
                break

    assert batches, "expected discovery batches"
    final = batches[-1]
    assert final, "expected candidates from fallback path"

    addresses = {entry["address"] for entry in final}
    assert "So11111111111111111111111111111111111111112" in addresses
    assert "using fallback set" in caplog.text
    assert captured_sessions and all(sess is None for sess in captured_sessions)
    assert partial_session.closed is True
    assert partial_session.close_calls == 1

    for session in per_task_sessions:
        # Fallback sessions are owned by individual tasks; ensure they were not closed here.
        assert session.closed is False
