"""Utility helpers for normalising token mint addresses."""

from __future__ import annotations

from functools import lru_cache
import json
import os
from typing import Dict

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_MAX_BASE58_LENGTH = 64
_ZERO_WIDTH_CHARS = {
    "\u200b",  # zero-width space
    "\u200c",  # zero-width non-joiner
    "\u200d",  # zero-width joiner
    "\u200e",  # left-to-right mark
    "\u200f",  # right-to-left mark
    "\u2060",  # word joiner
    "\ufeff",  # byte order mark
}
_ZERO_WIDTH_TRANSLATION = str.maketrans({ord(ch): None for ch in _ZERO_WIDTH_CHARS})
_ALIAS_HOP_LIMIT = 16


def _normalize_text(value: object | None) -> str:
    """Return ``value`` stripped of whitespace and zero-width characters."""

    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    cleaned = value.translate(_ZERO_WIDTH_TRANSLATION).strip()
    return cleaned


_STATIC_ALIASES_RAW: Dict[str, str] = {
    # Jupiter router -> JUP token mint
    "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
}

_STATIC_ALIASES: Dict[str, str] = {
    src: dst
    for src, dst in (
        (_normalize_text(key), _normalize_text(value))
        for key, value in _STATIC_ALIASES_RAW.items()
    )
    if src and dst
}


def _load_runtime_aliases() -> Dict[str, str]:
    """Parse runtime alias overrides from ``TOKEN_ALIAS_JSON`` if set."""

    blob = os.environ.get("TOKEN_ALIAS_JSON")
    if not blob:
        return {}
    try:
        mapping = json.loads(blob)
    except json.JSONDecodeError:
        return {}
    if not isinstance(mapping, dict):
        return {}
    aliases: Dict[str, str] = {}
    for key, value in mapping.items():
        normalized_key = _normalize_text(key)
        normalized_value = _normalize_text(value)
        if normalized_key and normalized_value:
            aliases[normalized_key] = normalized_value
    return aliases


_RUNTIME_ALIASES: Dict[str, str] = _load_runtime_aliases()
_ALIASES: Dict[str, str] = {**_STATIC_ALIASES, **_RUNTIME_ALIASES}


def _b58_decode(value: str) -> bytes | None:
    """Return the raw bytes for ``value`` when it is valid base58."""

    if not value or len(value) > _MAX_BASE58_LENGTH:
        return None
    total = 0
    for ch in value:
        idx = _B58_ALPHABET.find(ch)
        if idx == -1:
            return None
        total = total * 58 + idx
    # account for leading zeroes encoded as ``1``
    prefix_len = len(value) - len(value.lstrip("1"))
    if total == 0:
        decoded = b""
    else:
        size = (total.bit_length() + 7) // 8
        decoded = total.to_bytes(size, "big")
    return b"\x00" * prefix_len + decoded


@lru_cache(maxsize=1024)
def _resolve_alias_chain(address: str) -> str:
    """Follow alias links until a stable mint is found or a cycle is detected."""

    current = address
    visited = {current}
    for _ in range(_ALIAS_HOP_LIMIT):
        target = _ALIASES.get(current)
        if not target:
            break
        normalized = _normalize_text(target)
        if not normalized or normalized in visited:
            break
        current = normalized
        visited.add(current)
    return current


def canonical_mint(address: str | None) -> str:
    """Return the canonical mint address for ``address`` after normalisation."""

    normalized = _normalize_text(address)
    if not normalized:
        return ""
    return _resolve_alias_chain(normalized)


def _is_valid_base58_public_key(candidate: str) -> bool:
    if len(candidate) < 32 or len(candidate) > 44:
        return False
    raw = _b58_decode(candidate)
    if raw is None or len(raw) != 32:
        return False
    return True


def validate_mint(address: str | None) -> bool:
    """Return ``True`` when ``address`` is a 32-byte base58 public key."""

    if not isinstance(address, str):
        return False
    candidate = canonical_mint(address)
    if not candidate:
        return False
    return _is_valid_base58_public_key(candidate)


def normalize_mint_or_none(address: str | None) -> str | None:
    """Return a canonical, validated mint or ``None`` when invalid."""

    candidate = canonical_mint(address)
    if not candidate:
        return None
    return candidate if _is_valid_base58_public_key(candidate) else None


__all__ = ["canonical_mint", "normalize_mint_or_none", "validate_mint"]
