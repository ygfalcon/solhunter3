from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import aiohttp
import pytest

from solhunter_zero import token_scanner


class _FakeResponse:
    def __init__(self, payload: Dict[str, Any], status: int = 200):
        self._payload = payload
        self.status = status

    async def __aenter__(self) -> "_FakeResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def json(self, content_type: str | None = None) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientError(f"status={self.status}")


def _build_payload() -> Dict[str, Any]:
    return {
        "data": {
            "tokens": [
                {
                    "token": {
                        "mint": "MintTrend000000000000000000000000000000001",
                        "symbol": "TREND",
                        "name": "Trend Token",
                    },
                    "metrics": {"price": 1.01, "volumeUSD": 5000},
                },
                {
                    "tokenAddress": "MintTrend000000000000000000000000000000002",
                    "ticker": "MOON",
                    "metrics": {"priceUsd": "0.5", "volume24h": "1200"},
                },
            ]
        }
    }


def _install_fake_client(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    requests: List[Dict[str, Any]] = []

    class _FakeClientSession:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._payload = _build_payload()

        async def __aenter__(self) -> "_FakeClientSession":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        def post(
            self,
            url: str,
            *,
            json: Dict[str, Any] | None = None,
            params: Dict[str, Any] | None = None,
            headers: Dict[str, str] | None = None,
            timeout: float | None = None,
        ) -> _FakeResponse:
            requests.append(
                {
                    "method": "POST",
                    "url": url,
                    "json": json,
                    "params": params,
                    "headers": headers,
                    "timeout": timeout,
                }
            )
            return _FakeResponse(self._payload)

        def get(self, *args: Any, **kwargs: Any) -> None:
            raise AssertionError("GET should not be used in Helius-only flow")

    monkeypatch.setattr(token_scanner.aiohttp, "ClientSession", _FakeClientSession)
    return requests


def test_scan_tokens_async_prefers_helius(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HELIUS_TRENDING_URL", raising=False)
    monkeypatch.delenv("HELIUS_TRENDING_METHOD", raising=False)

    requests = _install_fake_client(monkeypatch)

    birdeye_called = False

    async def _fake_birdeye(*args: Any, **kwargs: Any) -> List[str]:
        nonlocal birdeye_called
        birdeye_called = True
        return []

    monkeypatch.setattr(token_scanner, "_birdeye_trending", _fake_birdeye)

    mints = asyncio.run(token_scanner.scan_tokens_async(limit=2))

    assert birdeye_called is False
    assert mints == [
        "MintTrend000000000000000000000000000000001",
        "MintTrend000000000000000000000000000000002",
    ]

    assert requests, "Expected Helius POST request"
    body = requests[0]["json"]
    assert body["limit"] == 2
    assert body["timeframe"] == "24h"
    assert body.get("offset") == 0

    for mint in mints:
        assert token_scanner.TRENDING_METADATA[mint]["source"] == "helius"
        assert "helius" in token_scanner.TRENDING_METADATA[mint]["sources"]
