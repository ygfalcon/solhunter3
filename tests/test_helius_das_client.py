"""Tests for the helius_das client helpers."""

from __future__ import annotations

import asyncio
import importlib
import json
import sys
from typing import Any, Dict, Optional

import pytest


def _reload_module(
    monkeypatch: pytest.MonkeyPatch,
    *,
    base_url: Optional[str] = None,
    extra_env: Optional[Dict[str, str]] = None,
):
    for key in ("HELIUS_API_KEYS", "HELIUS_API_KEY", "HELIUS_API_TOKEN", "DAS_BASE_URL"):
        monkeypatch.delenv(key, raising=False)
    if base_url is not None:
        monkeypatch.setenv("DAS_BASE_URL", base_url)
    if extra_env:
        for key, value in extra_env.items():
            monkeypatch.setenv(key, value)
    sys.modules.pop("solhunter_zero.clients.helius_das", None)
    return importlib.import_module("solhunter_zero.clients.helius_das")


def test_inline_key_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_module(
        monkeypatch,
        base_url="https://example.com/some-path?api-key=inline-token&foo=bar",
    )
    assert module._API_KEYS.keys == ("inline-token",)


def test_post_rpc_uses_base_url_when_pool_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_module(
        monkeypatch,
        base_url="https://example.com/rpc?api-key=inline",
    )

    original_keys = module._API_KEYS.keys
    module._API_KEYS.keys = tuple()
    try:
        calls = []

        class DummyResponse:
            status = 200
            headers: Dict[str, str] = {}

            def __init__(self) -> None:
                self._text = json.dumps({"result": {}})

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc_info):
                return False

            def raise_for_status(self) -> None:
                return None

            async def text(self) -> str:
                return self._text

        class DummySession:
            def post(self, url: str, *, json: Dict[str, Any], timeout: Any, headers: Dict[str, str]):
                calls.append({"url": url, "json": json, "headers": headers})
                return DummyResponse()

        session = DummySession()
        result = asyncio.run(module._post_rpc(session, "testMethod", {"foo": "bar"}))
        assert result == {"result": {}}
        assert calls and calls[0]["url"] == module.DAS_BASE
        assert calls[0]["headers"]["Content-Type"] == "application/json"
    finally:
        module._API_KEYS.keys = original_keys


def test_post_rpc_error_message_includes_context(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_module(
        monkeypatch,
        base_url="https://example.com/rpc",
        extra_env={"HELIUS_API_KEY": "env-key"},
    )

    class ErrorResponse:
        status = 200
        headers: Dict[str, str] = {}

        def __init__(self) -> None:
            self._text = json.dumps({"error": {"code": -32602, "message": "bad"}})

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc_info):
            return False

        def raise_for_status(self) -> None:
            return None

        async def text(self) -> str:
            return self._text

    class DummySession:
        def post(self, url: str, *, json: Dict[str, Any], timeout: Any, headers: Dict[str, str]):
            return ErrorResponse()

    session = DummySession()
    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(module._post_rpc(session, "searchAssets", {"page": 1, "limit": 2}))
    message = str(excinfo.value)
    assert "searchAssets" in message
    assert '"page":1' in message
    assert '"limit":2' in message


def test_get_asset_batch_filters_invalid_mints(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_module(
        monkeypatch,
        base_url="https://example.com/rpc?api-key=inline",
    )

    captured = {}

    async def fake_post(session, method, params, **kwargs):
        captured["params"] = params
        return {"result": [{"address": "11111111111111111111111111111111"}]}

    monkeypatch.setattr(module, "_post_rpc", fake_post)

    class DummySession:
        pass

    valid = "11111111111111111111111111111111"
    also_valid = "So11111111111111111111111111111111111111112"
    invalid = "this-is-not-valid"

    result = asyncio.run(module.get_asset_batch(DummySession(), [invalid, valid, also_valid, None]))
    assert result == [{"address": valid}]
    assert captured["params"] == {"ids": [valid, also_valid]}


def test_clean_payload_drops_empty_mint_lists(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _reload_module(monkeypatch)
    payload = module._clean_payload({"ids": ["not-a-mint"], "tokenType": "fungible"})
    assert payload == {"tokenType": "fungible"}
    payload = module._clean_payload({"tokenAddresses": ["11111111111111111111111111111111", "bad"]})
    assert payload == {"tokenAddresses": ["11111111111111111111111111111111"]}
