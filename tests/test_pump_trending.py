import asyncio
import importlib


class DummyResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status = 200

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
        self.calls = []

    def get(self, url, *, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return DummyResponse(self._payload)


def test_pump_trending_attaches_raw_and_finalizes_sources(monkeypatch):
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://pump.example/api")
    monkeypatch.setenv("PUMP_FUN_TOKENS", "")
    monkeypatch.setenv("STATIC_SEED_TOKENS", "")

    module = importlib.import_module("solhunter_zero.token_scanner")
    token_scanner = importlib.reload(module)

    payload = [
        {
            "mint": "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
            "name": "Pump Token",
            "symbol": "PUMP",
            "liquidity": 123,
            "volume_24h": 456,
        }
    ]

    session = DummySession(payload)

    results = asyncio.run(token_scanner._pump_trending(session, limit=5))

    assert results, "Expected Pump.fun trending to return at least one candidate"
    entry = results[0]

    assert entry["raw"] is payload[0]
    assert entry["sources"] == ["pumpfun"]
    assert "pumpfun" in entry["reasons"]
    assert entry["metadata"]["pumpfun"]["volume_24h"] == 456
