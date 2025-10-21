import asyncio
import importlib


def _reset_state(token_scanner):
    token_scanner.TRENDING_METADATA.clear()
    token_scanner._LAST_TRENDING_RESULT.clear()
    token_scanner._FAILURE_COUNT = 0
    token_scanner._COOLDOWN_UNTIL = 0.0
    token_scanner._DAS_CIRCUIT_OPEN_UNTIL = 0.0


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

    async def fake_pump(session, *, limit):
        flags["pump_calls"] += 1
        return [
            {
                "address": pump_mint,
                "source": "pumpfun",
                "sources": ["pumpfun"],
                "rank": 0,
            }
        ]

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
