import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from solhunter_zero.providers import dexscreener as dexscreener_provider
from solhunter_zero.providers import raydium as raydium_provider


class _StubResponse:
    def __init__(self, payload, *, status=200):
        self._payload = payload
        self.status = status

    async def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"status {self.status}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _StubSession:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []
        self.closed = False

    def get(self, url, *, headers=None, timeout=None):
        self.calls.append({"url": url, "headers": headers or {}, "timeout": timeout})
        return _StubResponse(self._payload)

    async def close(self):
        self.closed = True


@asynccontextmanager
async def _noop_host_request(url: str):
    yield SimpleNamespace()


def test_dexscreener_fetch_picks_top_liquidity(monkeypatch):
    payload = {
        "pairs": [
            {
                "baseToken": {"address": "MintDex", "symbol": "MDX", "name": "Mint Dex"},
                "quoteToken": {"address": "Quote", "symbol": "Q"},
                "priceUsd": "1.23",
                "liquidity": {"usd": "21000"},
                "priceChange": {"m5": 0.002},
                "pairAddress": "pool-main",
                "dexId": "raydium",
                "updatedAt": 1_689_000_000_000,
            },
            {
                "baseToken": {"address": "MintDex", "symbol": "MDX", "name": "Mint Dex"},
                "quoteToken": {"address": "USDC", "symbol": "USDC"},
                "priceUsd": "1.10",
                "liquidity": {"usd": "15000"},
                "priceChange": {"m5": 0.01},
                "pairAddress": "pool-secondary",
                "dexId": "orca",
                "updatedAt": 1_689_000_000_500,
            },
        ]
    }
    session = _StubSession(payload)
    monkeypatch.setattr(dexscreener_provider, "host_request", _noop_host_request)

    result = asyncio.run(
        dexscreener_provider.fetch("MintDex", session=session, timeout=0.1)
    )

    assert result["source"] == "dexscreener"
    assert result["price"] == pytest.approx(1.23)
    assert result["liquidity_usd"] == pytest.approx(21000.0)
    assert result["spread_bps"] == pytest.approx(0.2)
    assert result["as_of"] == 1_689_000_000_000
    assert result["venues"][0]["name"] == "raydium"
    assert session.calls and session.calls[0]["headers"]["Accept"] == "application/json"
    assert session.closed is False


def test_raydium_fetch_normalises_pairs(monkeypatch):
    payload = {
        "data": [
            {
                "baseMint": "MintDex",
                "quoteMint": "Quote",
                "liquidityUsd": "54000",
                "priceUsd": "0.98",
                "createdAt": 1_689_111_000_000,
                "ammId": "pool-ray",
                "name": "Mint Dex",
                "symbol": "MDX",
            },
            {
                "baseMint": "Other",
                "quoteMint": "MintDex",
                "liquidityUsd": "12000",
                "priceUsd": "1.02",
                "createdAt": 1_689_111_500_000,
                "ammId": "pool-ray-2",
                "name": "Other",
                "symbol": "OTH",
            },
        ]
    }
    session = _StubSession(payload)
    monkeypatch.setattr(raydium_provider, "host_request", _noop_host_request)

    result = asyncio.run(
        raydium_provider.fetch("MintDex", session=session, timeout=0.1)
    )

    assert result["source"] == "raydium"
    assert result["price"] == pytest.approx(0.98)
    assert result["liquidity_usd"] == pytest.approx(54000.0)
    assert result["venues"][0]["pair"] == "pool-ray"
    assert result["pairs"] and result["pairs"][0]["mint_base"] == "MintDex"
    assert session.calls and session.calls[0]["headers"]["Accept"] == "application/json"
    assert session.closed is False
