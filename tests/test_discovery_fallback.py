import asyncio
import importlib


def test_collect_trending_uses_pump_when_dex_disabled(monkeypatch):
    monkeypatch.setenv("DEXSCREENER_DISABLED", "1")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "0")
    monkeypatch.setenv("STATIC_SEED_TOKENS", "")
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://example.com/pump")

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)

    pump_mint = "4f3UGt1WUwE6HZ7UFr3W14oPV53H6G6wNWvD0AfgpZX7"

    async def fake_pump(session, *, limit):
        return [
            {
                "address": pump_mint,
                "source": "pumpfun",
                "sources": ["pumpfun"],
                "rank": 0,
            }
        ]

    flags = {"dex_called": False}

    async def track(*args, **kwargs):
        flags["dex_called"] = True
        return []

    monkeypatch.setattr(token_scanner, "_pump_trending", fake_pump)
    monkeypatch.setattr(token_scanner, "_dexscreener_new_pairs", track)
    monkeypatch.setattr(token_scanner, "_dexscreener_trending_movers", track)

    class DummySession:
        pass

    async def runner():
        token_scanner.TRENDING_METADATA.clear()
        token_scanner._LAST_TRENDING_RESULT.clear()
        result = await token_scanner._collect_trending_seeds(
            DummySession(),
            limit=3,
            birdeye_api_key=None,
            rpc_url="",
        )
        assert result == [pump_mint]
        assert token_scanner.TRENDING_METADATA[pump_mint]["source"] == "pumpfun"

    asyncio.run(runner())
    assert flags["dex_called"] is False


def test_collect_trending_static_env_fallback(monkeypatch):
    env_mints = [
        "9wFFujD6jH4KrhzCYXmf4jPQTa1hsJ8qsyYvD5vyGqjA",
        "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",
    ]
    monkeypatch.setenv("DEXSCREENER_DISABLED", "1")
    monkeypatch.setenv("USE_DAS_DISCOVERY", "0")
    monkeypatch.setenv("STATIC_SEED_TOKENS", ",".join(env_mints))
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "")

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)

    async def empty(*args, **kwargs):
        return []

    monkeypatch.setattr(token_scanner, "_pump_trending", empty)
    monkeypatch.setattr(token_scanner, "_dexscreener_new_pairs", empty)
    monkeypatch.setattr(token_scanner, "_dexscreener_trending_movers", empty)

    class DummySession:
        pass

    async def runner():
        token_scanner.TRENDING_METADATA.clear()
        token_scanner._LAST_TRENDING_RESULT.clear()
        result = await token_scanner._collect_trending_seeds(
            DummySession(),
            limit=4,
            birdeye_api_key=None,
            rpc_url="",
        )
        assert result[: len(env_mints)] == env_mints
        for mint in env_mints:
            assert token_scanner.TRENDING_METADATA[mint]["source"] == "static_env"

    asyncio.run(runner())
