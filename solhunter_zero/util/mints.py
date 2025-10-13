"""Utilities for validating and cleaning Solana mint addresses."""

from __future__ import annotations

import base58
from typing import List, Sequence, Tuple


def is_valid_solana_mint(mint: str) -> bool:
    """Return ``True`` when ``mint`` decodes to a 32-byte base58 string."""

    if not isinstance(mint, str):
        return False
    candidate = mint.strip()
    if not (32 <= len(candidate) <= 44):
        return False
    try:
        raw = base58.b58decode(candidate)
    except Exception:
        return False
    return len(raw) == 32


def _coerce_candidate(value: object) -> str | None:
    if isinstance(value, str):
        return value
    try:
        return str(value)
    except Exception:
        return None


def _strip_artifacts(text: str) -> str:
    stripped = text.strip()
    for sep in (" ", ",", ";", "\t", "\n"):
        stripped = stripped.split(sep, 1)[0]
    return stripped.strip()


def clean_candidate_mints(mints: Sequence[object]) -> Tuple[List[str], List[object]]:
    """Split and validate potential mint addresses.

    Returns ``(valid, dropped)`` where ``valid`` contains cleaned base58 mints
    suitable for downstream processing and ``dropped`` lists the rejected
    entries for diagnostics.
    """

    cleaned: List[str] = []
    dropped: List[object] = []
    for raw in mints:
        candidate = _coerce_candidate(raw)
        if not candidate:
            dropped.append(raw)
            continue
        stripped = _strip_artifacts(candidate)
        if is_valid_solana_mint(stripped):
            cleaned.append(stripped)
        else:
            dropped.append(raw)
    return cleaned, dropped


__all__ = ["clean_candidate_mints", "is_valid_solana_mint"]
