from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import aiohttp
import pytest

from solhunter_zero import token_scanner


class _DummyResponse:
    def __init__(self, payload: Dict[str, Any], status: int = 200):
        self._payload = payload
        self.status = status

    async def __aenter__(self) -> "_DummyResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: D401 - signature match
        return None

    async def json(self, content_type: str | None = None) -> Dict[str, Any]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientError(f"status={self.status}")


class _DummySession:
    def __init__(self, payload: Dict[str, Any]):
        self._payload = payload
        self.calls: List[Dict[str, Any]] = []

    def post(
        self,
        url: str,
        *,
        json: Dict[str, Any] | None = None,
        params: Dict[str, Any] | None = None,
        headers: Dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> _DummyResponse:
        self.calls.append(
            {
                "method": "POST",
                "url": url,
                "json": json,
                "params": params,
                "headers": headers,
                "timeout": timeout,
            }
        )
        return _DummyResponse(self._payload)


def test_helius_trending_parses_nested_payload() -> None:
    payload = {
        "data": {
            "tokens": [
                {
                    "token": {
                        "mint": "Mint111111111111111111111111111111111111111",
                        "symbol": "AAA",
                        "name": "Alpha",
                    },
                    "metrics": {
                        "price": "1.23",
                        "volume24h": "9876.5",
                        "priceChange24hPercent": "11.5",
                    },
                },
                {
                    "tokenAddress": "Mint2222222222222222222222222222222222222",
                    "ticker": "BBB",
                    "metrics": {
                        "priceUsd": 0.42,
                        "volumeUSD": "1234",
                        "marketCapUsd": "654321",
                    },
                },
            ]
        }
    }

    session = _DummySession(payload)
    tokens = asyncio.run(token_scanner._helius_trending(session, limit=3))

    assert [item["address"] for item in tokens] == [
        "Mint111111111111111111111111111111111111111",
        "Mint2222222222222222222222222222222222222",
    ]
    assert tokens[0]["symbol"] == "AAA"
    assert pytest.approx(tokens[0]["price"]) == 1.23
    assert pytest.approx(tokens[0]["volume"]) == 9876.5
    assert pytest.approx(tokens[0]["price_change"]) == 11.5
    assert tokens[0]["sources"] == ["helius"]

    assert tokens[1]["symbol"] == "BBB"
    assert pytest.approx(tokens[1]["price"]) == 0.42
    assert pytest.approx(tokens[1]["volume"]) == 1234
    assert pytest.approx(tokens[1]["market_cap"]) == 654321

    assert session.calls, "Expected POST request to be recorded"
    body = session.calls[0]["json"]
    assert body is not None
    assert body["timeframe"] == "24h"
    assert body["limit"] == 3
    assert body.get("offset") == 0
