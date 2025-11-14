from __future__ import annotations

import asyncio
from typing import Any, Dict, List

import aiohttp
import pytest

from solhunter_zero import token_scanner


class _DummyResponse:
    def __init__(self, payload: List[Dict[str, Any]], status: int = 200):
        self._payload = payload
        self.status = status

    async def __aenter__(self) -> "_DummyResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # noqa: D401 - signature match
        return None

    async def json(self, content_type: str | None = None) -> List[Dict[str, Any]]:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientError(f"status={self.status}")


class _DummySession:
    def __init__(self, payload: List[Dict[str, Any]]):
        self._payload = payload
        self.calls: List[Dict[str, Any]] = []

    def get(
        self,
        url: str,
        *,
        params: Dict[str, Any] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
    ) -> _DummyResponse:
        self.calls.append(
            {
                "method": "GET",
                "url": url,
                "params": params,
                "timeout": timeout,
            }
        )
        return _DummyResponse(self._payload)


def test_pump_trending_uses_upstream_rank(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://example.com/pump")

    payload: List[Dict[str, Any]] = [
        {
            "mint": "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
            "rank": "5",
            "name": "Alpha",
        },
        {
            "tokenMint": "9GyVZQvqtD9JzqDZFJCBxJ63HgNbUsVTNff7kwh28Fdg",
            "position": " 7 ",
            "symbol": "BBB",
        },
        {
            "tokenMint": "2zKhYki71RJKVQUNBM8DT6vKrrfEigdFe9rvuiEa5xE4",
            "name": "Gamma",
        },
    ]

    session = _DummySession(payload)
    tokens = asyncio.run(token_scanner._pump_trending(session, limit=5))

    assert [item["address"] for item in tokens] == [
        "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq",
        "9GyVZQvqtD9JzqDZFJCBxJ63HgNbUsVTNff7kwh28Fdg",
        "2zKhYki71RJKVQUNBM8DT6vKrrfEigdFe9rvuiEa5xE4",
    ]
    assert [item["rank"] for item in tokens] == [5, 7, 2]
    assert all(isinstance(item["rank"], int) for item in tokens)
    assert session.calls, "Expected GET request to be recorded"
