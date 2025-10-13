"""Utility helpers for normalising token mint addresses."""

from __future__ import annotations

from typing import Dict

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _b58_decode(value: str) -> bytes | None:
    """Return the raw bytes for ``value`` when it is valid base58."""

    if not value:
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

# Map well-known program/router addresses to their underlying token mints.
_ALIASES: Dict[str, str] = {
    # Jupiter router -> JUP token mint
    "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
}


def canonical_mint(address: str) -> str:
    """Return the canonical mint address for ``address``."""

    return _ALIASES.get(address, address)


def validate_mint(address: str | None) -> bool:
    """Return ``True`` when ``address`` is a 32-byte base58 public key."""

    if not isinstance(address, str):
        return False
    candidate = canonical_mint(address.strip())
    if not candidate:
        return False
    if len(candidate) < 32 or len(candidate) > 44:
        return False
    raw = _b58_decode(candidate)
    if raw is None or len(raw) != 32:
        return False
    return True


__all__ = ["canonical_mint", "validate_mint"]
