import asyncio
import importlib

import pytest


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - interface
        return False

    def raise_for_status(self):  # pragma: no cover - interface
        return None

    async def json(self, content_type=None):
        return self._payload


class _DummySession:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    def get(self, url, params=None, timeout=None):  # pragma: no cover - interface
        self.calls.append((url, params))
        return _DummyResponse(self._payload)


def test_pump_trending_metadata_normalization(monkeypatch):
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://pump.example/leaderboard")

    import solhunter_zero.token_scanner as token_scanner

    token_scanner = importlib.reload(token_scanner)

    payload = [
        {
            "mint": "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
            "name": "Pump Test",
            "symbol": "PT",
            "liquidity": "1234.5",
            "volume_24h": "5000",
            "volume_1h": None,
            "volume_5m": "125.25",
            "market_cap": "98765",
            "price": "0.005",
            "rank": "7",
            "score": "0.91",
            "pumpScore": "0.95",
            "buyers_last_hour": "27",
            "tweets_last_hour": 144,
            "sentiment": "0.66",
        }
    ]

    async def runner():
        session = _DummySession(payload)
        result = await token_scanner._pump_trending(session, limit=1)
        assert len(result) == 1
        entry = result[0]
        pump_meta = entry["metadata"]["pumpfun"]
        assert pump_meta["rank"] == 7
        assert isinstance(pump_meta["rank"], int)
        assert pump_meta["score"] == pytest.approx(0.91)
        assert pump_meta["pumpScore"] == pytest.approx(0.95)
        assert pump_meta["buyersLastHour"] == 27
        assert isinstance(pump_meta["buyersLastHour"], int)
        assert pump_meta["tweetsLastHour"] == 144
        assert isinstance(pump_meta["tweetsLastHour"], int)
        assert pump_meta["sentiment"] == pytest.approx(0.66)
        assert pump_meta["liquidity"] == pytest.approx(1234.5)
        assert pump_meta["volume_24h"] == pytest.approx(5000.0)
        assert pump_meta["volume_5m"] == pytest.approx(125.25)
        assert pump_meta["market_cap"] == pytest.approx(98765.0)
        assert pump_meta["price"] == pytest.approx(0.005)
        assert "volume_1h" not in pump_meta

    asyncio.run(runner())
