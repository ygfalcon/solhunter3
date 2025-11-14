import asyncio

import pytest

from solhunter_zero import pumpfun
from solhunter_zero.golden_pipeline import momentum
from solhunter_zero.token_scanner import _pump_trending


def _pump_entry(address: str, **overrides):
    entry = {
        "tokenAddress": address,
        "name": "Alpha",
        "symbol": "ALPHA",
        "imageUrl": "https://example.com/a.png",
        "rank": 3,
        "liquidity": 1200.5,
        "volume24h": 5000,
        "volume_1h": 900,
        "volume_5m": 120,
        "marketCap": 75000,
        "price": "0.015",
        "pumpScore": 0.42,
        "buyersLastHour": 15,
        "tweets_last_hour": 4,
        "sentiment": 0.1,
    }
    entry.update(overrides)
    return entry


def test_normalize_pumpfun_payload_canonicalizes_and_dedupes():
    mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"
    payload = [
        _pump_entry(mint, image="https://example.com/ignored.png"),
        {"tokenMint": mint, "name": "Duplicate"},
    ]
    entries = pumpfun.normalize_pumpfun_payload(payload)
    assert [entry["mint"] for entry in entries] == [mint]
    entry = entries[0]
    assert entry["icon"] == "https://example.com/a.png"
    assert entry["volume_24h"] == 5000
    assert entry["market_cap"] == 75000
    assert entry["buyers_last_hour"] == 15
    assert entry["tweets_last_hour"] == 4
    assert entry["score"] == 0.42


def test_normalize_pumpfun_payload_handles_wrapped_payload():
    mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"
    payload = {"tokens": [_pump_entry(mint)]}
    entries = pumpfun.normalize_pumpfun_payload(payload)
    assert len(entries) == 1
    assert entries[0]["mint"] == mint


class _FakeResponse:
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


class _FakeSession:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        return _FakeResponse(self._payload)


def test_pump_trending_uses_normalized_entries(monkeypatch):
    mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"
    payload = [_pump_entry(mint, name="Beta", symbol="BETA")]
    session = _FakeSession(payload)
    monkeypatch.setattr("solhunter_zero.token_scanner.PUMP_LEADERBOARD_URL", "https://example.com/pump")
    tokens = asyncio.run(_pump_trending(session, limit=5))
    assert [token["address"] for token in tokens] == [mint]
    token = tokens[0]
    assert token["name"] == "Beta"
    assert token["symbol"] == "BETA"
    pump_meta = token["metadata"]["pumpfun"]
    assert pump_meta["volume_24h"] == 5000
    assert pump_meta["market_cap"] == 75000


def test_fetch_pumpfun_uses_normalized_entries(monkeypatch):
    async def publish(event: str, comp):  # pragma: no cover - signature only
        return None

    agent = momentum.MomentumAgent(pipeline=object(), publish=publish)

    mint = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"
    payload = {"tokens": [_pump_entry(mint)]}

    async def fake_request_json(self, url, *, host, params=None, headers=None):
        assert host == "pump.fun"
        return payload

    monkeypatch.setattr(agent, "_request_json", fake_request_json.__get__(agent, momentum.MomentumAgent))

    result = asyncio.run(agent._fetch_pumpfun())
    assert mint in result
    assert result[mint]["buyersLastHour"] == 15
    assert result[mint]["tweetsLastHour"] == 4
    assert result[mint]["score"] == 0.42
