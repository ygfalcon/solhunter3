"""Utilities for validating and cleaning discovered mint addresses."""

from __future__ import annotations

import base58
from typing import Iterable, List, Tuple

__all__ = ["is_valid_solana_mint", "clean_candidate_mints"]


def is_valid_solana_mint(candidate: str) -> bool:
    """Return ``True`` when ``candidate`` decodes to a 32-byte base58 string."""

    try:
        raw = base58.b58decode(candidate.strip())
    except Exception:  # pragma: no cover - invalid base58
        return False
    return len(raw) == 32


def _sanitize(raw: object) -> str:
    return str(raw).split()[0].split(",")[0].split(";")[0].strip()


def clean_candidate_mints(mints: Iterable[object]) -> Tuple[List[str], List[Tuple[object, str]]]:
    """Split ``mints`` into valid Solana mint addresses and dropped entries."""

    valid: List[str] = []
    dropped: List[Tuple[object, str]] = []
    for item in mints:
        candidate = _sanitize(item)
        if candidate and is_valid_solana_mint(candidate):
            valid.append(candidate)
        else:
            dropped.append((item, candidate))
    return valid, dropped

