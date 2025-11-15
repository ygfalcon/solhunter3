from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List

import aiohttp
import pytest

from solhunter_zero import token_scanner


class _DummyResponse:
    def __init__(
        self,
        payload: Dict[str, Any],
        *,
        status: int = 200,
        reason: str = "OK",
        headers: Dict[str, str] | None = None,
    ):
        self._payload = payload
        self.status = status
        self.reason = reason
        self.headers = headers or {}

    async def __aenter__(self) -> "_DummyResponse":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def json(self, content_type: str | None = None) -> Dict[str, Any]:
        return self._payload

    async def text(self) -> str:
        return ""

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientError(f"status={self.status}")


class _DummyErrorResponse(_DummyResponse):
    def __init__(self, *, status: int = 400, message: str = "bad request"):
        payload = {"success": False, "message": message}
        super().__init__(payload, status=status, reason="Bad Request")
        self._message = message

    async def json(self, content_type: str | None = None) -> Dict[str, Any]:  # pragma: no cover - defensive
        raise AssertionError("json() should not be called for error responses")

    async def text(self) -> str:
        return self._message


class _DummySession:
    def __init__(self, responses: List[_DummyResponse], calls: List[Dict[str, Any]]):
        self._responses = responses
        self._calls = calls

    async def __aenter__(self) -> "_DummySession":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    def get(
        self,
        url: str,
        *,
        headers: Dict[str, str] | None = None,
        params: Dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> _DummyResponse:
        self._calls.append(
            {
                "url": url,
                "headers": headers or {},
                "params": params or {},
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise AssertionError("No more responses queued")
        return self._responses.pop(0)


class _StubClientTimeout:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        return None


class _StubConnector:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        return None


aiohttp.ClientTimeout = _StubClientTimeout  # type: ignore[attr-defined]
setattr(token_scanner.aiohttp, "ClientTimeout", _StubClientTimeout)
aiohttp.TCPConnector = _StubConnector  # type: ignore[attr-defined]
setattr(token_scanner.aiohttp, "TCPConnector", _StubConnector)
aiohttp.ClientError = Exception  # type: ignore[attr-defined]
setattr(token_scanner.aiohttp, "ClientError", Exception)


def test_birdeye_trending_uses_new_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIRDEYE_TRENDING_TIMEFRAME", "6h")
    monkeypatch.setenv("BIRDEYE_TRENDING_TYPE", "trending")

    payload = {
        "success": True,
        "data": {
            "items": [
                {"address": "MintAddress00000000000000000000000000000001"},
                {"token": {"mintAddress": "MintAddress00000000000000000000000000000002"}},
                {"tokenInfo": {"tokenAddress": "MintAddress00000000000000000000000000000003"}},
            ],
            "total": 3,
        },
    }

    calls: List[Dict[str, Any]] = []
    session = _DummySession([_DummyResponse(payload)], calls)

    async def _run() -> List[str]:
        return await token_scanner._birdeye_trending(
            session,
            "api-key",
            limit=3,
            offset=5,
        )

    result = asyncio.run(_run())

    assert result == [
        "MintAddress00000000000000000000000000000001",
        "MintAddress00000000000000000000000000000002",
        "MintAddress00000000000000000000000000000003",
    ]

    assert calls, "Expected Birdeye GET request"
    call = calls[0]
    assert call["url"].endswith("/defi/trending")
    assert call["params"]["offset"] == 5
    assert call["params"]["limit"] == 3
    assert call["params"]["timeframe"] == "6h"
    assert call["params"]["type"] == "trending"
    assert call["headers"]["X-API-KEY"] == "api-key"


def test_birdeye_trending_detects_compute_units_throttle() -> None:
    calls: List[Dict[str, Any]] = []
    session = _DummySession(
        [
            _DummyErrorResponse(
                status=400,
                message="Compute units usage limit exceeded for plan Free",
            )
        ],
        calls,
    )

    async def _run() -> List[str]:
        return await token_scanner._birdeye_trending(session, "api-key", limit=5)

    with pytest.raises(token_scanner.BirdeyeThrottleError) as excinfo:
        asyncio.run(_run())

    assert len(calls) == 1
    assert excinfo.value.status == 400
    assert excinfo.value.throttle is True
    assert "compute units usage limit exceeded" in (excinfo.value.body or "").lower()


def test_scan_tokens_enters_cooldown_after_400(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setattr(token_scanner, "_LAST_TRENDING_RESULT", {"mints": [], "metadata": {}, "timestamp": 0.0})
    monkeypatch.setattr(token_scanner, "_FAILURE_COUNT", 0)
    monkeypatch.setattr(token_scanner, "_COOLDOWN_UNTIL", 0.0)

    monkeypatch.setattr(token_scanner, "_FAILURE_THRESHOLD", 2)
    monkeypatch.setattr(token_scanner, "_FAILURE_COOLDOWN", 120.0)
    monkeypatch.setattr(token_scanner, "_FATAL_FAILURE_COOLDOWN", 120.0)

    async def _helius_empty(session: aiohttp.ClientSession, *, limit: int) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(token_scanner, "_helius_trending", _helius_empty)

    calls: List[Dict[str, Any]] = []

    def _session_factory(*args: Any, **kwargs: Any) -> _DummySession:
        response = _DummyErrorResponse(status=400, message="invalid api key")
        return _DummySession([response], calls)

    monkeypatch.setattr(token_scanner.aiohttp, "ClientSession", _session_factory)

    with caplog.at_level("WARNING"):
        first = asyncio.run(token_scanner.scan_tokens_async(limit=2, api_key="invalid"))

    assert len(calls) == 1, "Expected a single Birdeye call"
    assert token_scanner._COOLDOWN_UNTIL > time.time()
    assert token_scanner._FAILURE_COUNT == token_scanner._FAILURE_THRESHOLD
    assert any("Birdeye trending fatal error" in rec.message for rec in caplog.records)

    calls.clear()

    second = asyncio.run(token_scanner.scan_tokens_async(limit=1, api_key="invalid"))

    assert not calls, "Cooldown should prevent additional Birdeye calls"
    assert first[0] == second[0]


def test_scan_tokens_resets_cooldown_after_recovery(monkeypatch: pytest.MonkeyPatch) -> None:
    base_state = {"mints": [], "metadata": {}, "timestamp": 0.0}
    monkeypatch.setattr(token_scanner, "_LAST_TRENDING_RESULT", base_state)
    monkeypatch.setattr(token_scanner, "_FAILURE_COUNT", 0)
    monkeypatch.setattr(token_scanner, "_COOLDOWN_UNTIL", 0.0)

    monkeypatch.setattr(token_scanner, "_FAILURE_THRESHOLD", 1)
    monkeypatch.setattr(token_scanner, "_FAILURE_COOLDOWN", 60.0)
    monkeypatch.setattr(token_scanner, "_FATAL_FAILURE_COOLDOWN", 60.0)
    monkeypatch.setattr(token_scanner, "_THROTTLE_COOLDOWN", 60.0)
    monkeypatch.setattr(token_scanner, "_MIN_SCAN_INTERVAL", 0.0)
    monkeypatch.setattr(token_scanner, "_PARTIAL_THRESHOLD", 2)

    class _DummyTimeout:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

    class _DummyConnector:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            return None

    monkeypatch.setattr(token_scanner.aiohttp, "ClientTimeout", _DummyTimeout)
    monkeypatch.setattr(token_scanner.aiohttp, "TCPConnector", _DummyConnector)
    monkeypatch.setattr(token_scanner.aiohttp, "ClientError", Exception, raising=False)

    async def _collect_empty(
        session: aiohttp.ClientSession,
        *,
        limit: int,
        birdeye_api_key: str | None = None,
        rpc_url: str | None = None,
    ) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(token_scanner, "_collect_trending_seeds", _collect_empty)

    async def _helius_empty(session: aiohttp.ClientSession, *, limit: int) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(token_scanner, "_helius_trending", _helius_empty)

    async def _pump_empty(session: aiohttp.ClientSession, limit: int) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(token_scanner, "_pump_trending", _pump_empty)

    def _failure_session_factory(*args: Any, **kwargs: Any) -> _DummySession:
        response = _DummyErrorResponse(status=400, message="invalid api key")
        return _DummySession([response], [])

    monkeypatch.setattr(token_scanner.aiohttp, "ClientSession", _failure_session_factory)

    asyncio.run(token_scanner.scan_tokens_async(limit=2, api_key="invalid"))

    assert token_scanner._FAILURE_COUNT == token_scanner._FAILURE_THRESHOLD
    initial_reason = token_scanner._LAST_TRENDING_RESULT.get("cooldown_reason")
    assert initial_reason

    monkeypatch.setattr(token_scanner, "_COOLDOWN_UNTIL", 0.0)

    async def _collect_failure(
        session: aiohttp.ClientSession,
        *,
        limit: int,
        birdeye_api_key: str | None = None,
        rpc_url: str | None = None,
    ) -> List[Dict[str, Any]]:
        raise token_scanner.BirdeyeFatalError(500, "upstream failure")

    monkeypatch.setattr(token_scanner, "_collect_trending_seeds", _collect_failure)

    helius_entries = [
        {"address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "source": "helius"},
        {"address": "So11111111111111111111111111111111111111112", "source": "helius"},
        {"address": "Es9vMFrzaCERo4PwniDPYZ6BtMqADdJe1na43EwxrsT", "source": "helius"},
    ]

    async def _helius_success(
        session: aiohttp.ClientSession,
        *,
        limit: int,
    ) -> List[Dict[str, Any]]:
        return helius_entries[:limit]

    monkeypatch.setattr(token_scanner, "_helius_trending", _helius_success)
    monkeypatch.setattr(token_scanner, "ENABLE_HELIUS_TRENDING", True)
    monkeypatch.setattr(token_scanner, "_birdeye_enabled", lambda *_: False)
    monkeypatch.setattr(token_scanner, "_birdeye_trending_allowed", lambda: False)

    def _success_session_factory(*args: Any, **kwargs: Any) -> _DummySession:
        return _DummySession([], [])

    monkeypatch.setattr(token_scanner.aiohttp, "ClientSession", _success_session_factory)

    recovered = asyncio.run(token_scanner.scan_tokens_async(limit=3, api_key="valid"))

    assert recovered == [entry["address"] for entry in helius_entries]
    assert token_scanner._FAILURE_COUNT == 0
    assert "cooldown_reason" not in token_scanner._LAST_TRENDING_RESULT
    assert "cooldown_until" not in token_scanner._LAST_TRENDING_RESULT


def test_scan_tokens_uses_cache_after_compute_units_throttle(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cached_mint = "CachedMint00000000000000000000000000000001"
    monkeypatch.setattr(
        token_scanner,
        "_LAST_TRENDING_RESULT",
        {
            "mints": [cached_mint],
            "metadata": {
                cached_mint: {
                    "address": cached_mint,
                    "source": "cache",
                    "sources": ["cache"],
                }
            },
            "timestamp": 0.0,
        },
    )
    monkeypatch.setattr(token_scanner, "_FAILURE_COUNT", 0)
    monkeypatch.setattr(token_scanner, "_COOLDOWN_UNTIL", 0.0)

    monkeypatch.setattr(token_scanner, "_FAILURE_THRESHOLD", 1)
    monkeypatch.setattr(token_scanner, "_FAILURE_COOLDOWN", 30.0)
    monkeypatch.setattr(token_scanner, "_FATAL_FAILURE_COOLDOWN", 300.0)
    monkeypatch.setattr(token_scanner, "_THROTTLE_COOLDOWN", 600.0)

    async def _helius_empty(session: aiohttp.ClientSession, *, limit: int) -> List[Dict[str, Any]]:
        return []

    monkeypatch.setattr(token_scanner, "_helius_trending", _helius_empty)

    calls: List[Dict[str, Any]] = []

    def _session_factory(*args: Any, **kwargs: Any) -> _DummySession:
        responses = [
            _DummyErrorResponse(
                status=400,
                message="Compute units usage limit exceeded for plan Free",
            ),
            _DummyErrorResponse(
                status=400,
                message="Compute units usage limit exceeded for plan Free",
            ),
        ]
        return _DummySession(responses, calls)

    monkeypatch.setattr(token_scanner.aiohttp, "ClientSession", _session_factory)

    with caplog.at_level("WARNING"):
        first = asyncio.run(token_scanner.scan_tokens_async(limit=1, api_key="exhausted"))

    assert len(calls) == 1, "Expected exactly one Birdeye request"
    assert first == [cached_mint]
    assert token_scanner._FAILURE_COUNT == token_scanner._FAILURE_THRESHOLD
    assert token_scanner._COOLDOWN_UNTIL > time.time()
    assert (
        token_scanner._COOLDOWN_UNTIL - time.time()
        >= token_scanner._THROTTLE_COOLDOWN - 1
    )
    assert any("Birdeye trending throttle" in rec.message for rec in caplog.records)

    calls.clear()

    second = asyncio.run(token_scanner.scan_tokens_async(limit=1, api_key="exhausted"))

    assert not calls, "Cooldown should prevent additional Birdeye calls"
    assert second == first
