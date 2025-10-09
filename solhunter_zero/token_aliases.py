"""Utility helpers for normalising token mint addresses."""

from __future__ import annotations

from typing import Dict

# Map well-known program/router addresses to their underlying token mints.
_ALIASES: Dict[str, str] = {
    # Jupiter router -> JUP token mint
    "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
}


def canonical_mint(address: str) -> str:
    """Return the canonical mint address for ``address``."""

    return _ALIASES.get(address, address)


__all__ = ["canonical_mint"]
