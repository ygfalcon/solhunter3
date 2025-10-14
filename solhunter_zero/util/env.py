"""Helpers for resolving environment-provided Solana / Helius endpoints."""

from __future__ import annotations

import os
from typing import Iterable

_PLACEHOLDER_MARKERS = {"YOUR_KEY", "demo-helius-key", "CHANGE_ME", "EXAMPLE"}


def _is_placeholder(value: str) -> bool:
    lowered = value.lower()
    return any(marker.lower() in lowered for marker in _PLACEHOLDER_MARKERS)


def _has_api_key(value: str) -> bool:
    value = value.lower()
    return "api-key=" in value or "api_key=" in value


def _resolve_env(names: Iterable[str]) -> str:
    for name in names:
        candidate = (os.environ.get(name) or "").strip()
        if not candidate or _is_placeholder(candidate):
            continue
        return candidate
    return ""


def require_helius_rpc_url() -> str:
    url = _resolve_env(("HELIUS_RPC_URL", "SOLANA_RPC_URL"))
    if not url:
        raise RuntimeError("HELIUS_RPC_URL (or SOLANA_RPC_URL) must be set to a Helius RPC URL")
    if not _has_api_key(url):
        raise RuntimeError("Helius RPC URL must include ?api-key=…")
    return url


def optional_helius_rpc_url(default: str | None = None) -> str:
    url = _resolve_env(("HELIUS_RPC_URL", "SOLANA_RPC_URL"))
    if url:
        return url
    return default or ""


def optional_helius_ws_url(default: str | None = None) -> str:
    url = _resolve_env(("HELIUS_WS_URL", "SOLANA_WS_URL"))
    if url:
        return url
    return default or ""


def optional_testnet_rpc_url(default: str | None = None) -> str:
    url = _resolve_env(("SOLANA_TESTNET_RPC_URL",))
    if url:
        if not _has_api_key(url):
            raise RuntimeError("Testnet RPC URL must include ?api-key=…")
        return url
    return default or ""

