import asyncio
import importlib


def _reset_state(token_scanner):
    token_scanner.TRENDING_METADATA.clear()
    token_scanner._LAST_TRENDING_RESULT.clear()
    token_scanner._FAILURE_COUNT = 0
    token_scanner._COOLDOWN_UNTIL = 0.0
    token_scanner._DAS_CIRCUIT_OPEN_UNTIL = 0.0
    token_scanner._NEXT_DAS_REQUEST_AT = 0.0


def test_scan_tokens_uses_pump_fallback_when_dex_disabled(monkeypatch):
    monkeypatch.setenv("DEXSCREENER_DISABLED", "1")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "0")
    monkeypatch.setenv("STATIC_SEED_TOKENS", "")
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://example.com/pump")
    monkeypatch.delenv("SOLSCAN_API_KEY", raising=False)

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)
    _reset_state(token_scanner)

    pump_mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"
    flags = {"pump_calls": 0, "dex_calls": 0}

    class DummyResponse:
        def __init__(self, payload):
            self._payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        async def json(self, content_type=None):
            return self._payload

    class DummySession:
        def __init__(self, payload):
            self._payload = payload

        def get(self, url, params=None, timeout=None):
            return DummyResponse(self._payload)

    orig_pump_trending = token_scanner._pump_trending

    async def fake_pump(session, *, limit):
        flags["pump_calls"] += 1
        payload = [
            {
                "mint": pump_mint,
                "name": "Test Coin",
                "symbol": "TST",
                "score": 42,
                "buyers": 17,
                "tweets": 99,
                "sentiment": 0.75,
            }
        ]
        dummy_session = DummySession(payload)
        return await orig_pump_trending(dummy_session, limit=limit)

    async def track_dex(*args, **kwargs):
        flags["dex_calls"] += 1
        return []

    async def fake_enrich(session, addresses):
        return {}

    monkeypatch.setattr(token_scanner, "_pump_trending", fake_pump)
    monkeypatch.setattr(token_scanner, "_dexscreener_new_pairs", track_dex)
    monkeypatch.setattr(token_scanner, "_dexscreener_trending_movers", track_dex)
    monkeypatch.setattr(token_scanner, "_pyth_seed_entries", lambda: [])
    monkeypatch.setattr(token_scanner, "_das_enrich_candidates", fake_enrich)

    async def runner():
        result = await token_scanner._scan_tokens_async_locked(
            limit=3,
            rpc_url="http://localhost",
            enrich=False,
            api_key=None,
        )
        assert result == [pump_mint]
        assert token_scanner.TRENDING_METADATA[pump_mint]["source"] == "pumpfun"
        pump_meta = token_scanner.TRENDING_METADATA[pump_mint]["metadata"]["pumpfun"]
        assert pump_meta["score"] == 42
        assert pump_meta["buyers"] == 17
        assert pump_meta["tweets"] == 99
        assert pump_meta["sentiment"] == 0.75

    asyncio.run(runner())
    assert flags["pump_calls"] == 1
    assert flags["dex_calls"] == 0


def test_static_env_tokens_used_when_all_sources_fail(monkeypatch):
    env_mints = [
        "9wFFujD6jH4KrhzCYXmf4jPQTa1hsJ8qsyYvD5vyGqjA",
        "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
    ]
    monkeypatch.setenv("DEXSCREENER_DISABLED", "1")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "0")
    monkeypatch.setenv("STATIC_SEED_TOKENS", ",".join(env_mints))
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "")
    monkeypatch.delenv("SOLSCAN_API_KEY", raising=False)

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)
    _reset_state(token_scanner)

    async def no_seeds(*args, **kwargs):
        return []

    monkeypatch.setattr(token_scanner, "_collect_trending_seeds", no_seeds)
    monkeypatch.setattr(token_scanner, "_pump_trending", no_seeds)

    async def runner():
        limit = len(env_mints)
        result = await token_scanner._scan_tokens_async_locked(
            limit=limit,
            rpc_url="http://localhost",
            enrich=False,
            api_key=None,
        )
        assert result == env_mints
        for mint in env_mints:
            assert token_scanner.TRENDING_METADATA[mint]["source"] == "static_env"

    asyncio.run(runner())


def test_collect_trending_invokes_multiple_network_sources(monkeypatch):
    monkeypatch.setenv("DEXSCREENER_DISABLED", "0")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "0")
    monkeypatch.setenv("STATIC_SEED_TOKENS", "")
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://example.com/pump")
    monkeypatch.delenv("SOLSCAN_API_KEY", raising=False)

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)
    _reset_state(token_scanner)

    flags = {"dex_new": 0, "dex_trend": 0, "pump": 0}

    dex_new_mint = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9"
    dex_trend_mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
    pump_mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"

    async def fake_new_pairs(session, *, limit):
        flags["dex_new"] += 1
        return [{"address": dex_new_mint}]

    async def fake_trending(session, *, limit):
        flags["dex_trend"] += 1
        return [{"address": dex_trend_mint}]

    async def fake_pump(session, *, limit):
        flags["pump"] += 1
        return [
            {
                "address": pump_mint,
                "source": "pumpfun",
                "sources": ["pumpfun"],
                "rank": 0,
            }
        ]

    async def fake_birdeye(*args, **kwargs):
        return []

    async def fake_enrich(session, addresses):
        return {}

    monkeypatch.setattr(token_scanner, "_dexscreener_new_pairs", fake_new_pairs)
    monkeypatch.setattr(token_scanner, "_dexscreener_trending_movers", fake_trending)
    monkeypatch.setattr(token_scanner, "_pump_trending", fake_pump)
    monkeypatch.setattr(token_scanner, "_birdeye_trending", fake_birdeye)
    monkeypatch.setattr(token_scanner, "_das_enrich_candidates", fake_enrich)

    class DummySession:
        post = object()

    async def runner():
        result = await token_scanner._collect_trending_seeds(
            DummySession(),
            limit=8,
            birdeye_api_key=None,
            rpc_url="http://localhost",
        )
        addresses = [entry["address"] for entry in result]
        assert dex_new_mint in addresses
        assert dex_trend_mint in addresses
        assert pump_mint in addresses

    asyncio.run(runner())
    assert flags["dex_new"] == 1
    assert flags["dex_trend"] == 1
    assert flags["pump"] == 1


def test_static_fallback_defaults_to_usdc(monkeypatch):
    monkeypatch.setenv("DEXSCREENER_DISABLED", "1")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "0")
    monkeypatch.setenv("STATIC_SEED_TOKENS", "")
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "")
    monkeypatch.delenv("SOLSCAN_API_KEY", raising=False)

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)
    _reset_state(token_scanner)

    async def empty(*args, **kwargs):
        return []

    monkeypatch.setattr(token_scanner, "_collect_trending_seeds", empty)
    monkeypatch.setattr(token_scanner, "_pump_trending", empty)
    monkeypatch.setattr(token_scanner, "_pyth_seed_entries", lambda: [])

    async def runner():
        result = await token_scanner._scan_tokens_async_locked(
            limit=3,
            rpc_url="http://localhost",
            enrich=False,
            api_key=None,
        )
        assert result == ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]
        entry = token_scanner.TRENDING_METADATA["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]
        assert entry["source"] == "static"

    asyncio.run(runner())


def test_collect_trending_runs_das_alongside(monkeypatch):
    monkeypatch.setenv("DEXSCREENER_DISABLED", "0")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "1")
    monkeypatch.setenv("STATIC_SEED_TOKENS", "")
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://example.com/pump")
    monkeypatch.setenv("HELIUS_API_KEY", "af30888b-b79f-4b12-b3fd-c5375d5bad2d")
    monkeypatch.setenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d")
    monkeypatch.delenv("SOLSCAN_API_KEY", raising=False)

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)
    _reset_state(token_scanner)
    token_scanner._DAS_CIRCUIT_OPEN_UNTIL = 0.0

    flags = {"helius": 0, "pump": 0}

    helius_mint = "7XSswsRHEPTNYw2XBaLJR5Ret2Rk1ztwP1VhRwX5tNTK"
    pump_mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"

    async def fake_new_pairs(session, *, limit):
        return []

    async def fake_trending(session, *, limit):
        return []

    async def fake_pump(session, *, limit):
        flags["pump"] += 1
        return [
            {
                "address": pump_mint,
                "source": "pumpfun",
                "sources": ["pumpfun"],
                "rank": 0,
            }
        ]

    async def fake_birdeye(*args, **kwargs):
        return []

    async def fake_helius(session, *, limit, rpc_url):
        flags["helius"] += 1
        return [
            {
                "address": helius_mint,
                "source": "helius_search",
                "sources": ["helius_search"],
                "rank": 0,
            }
        ]

    async def fake_enrich(session, addresses):
        return {}

    monkeypatch.setattr(token_scanner, "_dexscreener_new_pairs", fake_new_pairs)
    monkeypatch.setattr(token_scanner, "_dexscreener_trending_movers", fake_trending)
    monkeypatch.setattr(token_scanner, "_pump_trending", fake_pump)
    monkeypatch.setattr(token_scanner, "_birdeye_trending", fake_birdeye)
    monkeypatch.setattr(token_scanner, "_helius_search_assets", fake_helius)
    monkeypatch.setattr(token_scanner, "_pyth_seed_entries", lambda: [])
    monkeypatch.setattr(token_scanner, "_das_enrich_candidates", fake_enrich)

    class DummySession:
        post = object()

    async def runner():
        result = await token_scanner._collect_trending_seeds(
            DummySession(),
            limit=6,
            birdeye_api_key=None,
            rpc_url="https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
        )
        addresses = [entry["address"] for entry in result]
        assert helius_mint in addresses
        assert pump_mint in addresses

    asyncio.run(runner())
    assert flags["helius"] == 1
    assert flags["pump"] == 1
