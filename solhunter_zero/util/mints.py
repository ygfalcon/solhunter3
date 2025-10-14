"""Helpers for validating and normalising Solana mint addresses."""

from __future__ import annotations

from typing import Iterable, List, Tuple

try:  # pragma: no cover - optional dependency
    import base58  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - graceful fallback
    base58 = None  # type: ignore[assignment]

from ..token_aliases import validate_mint


def is_valid_solana_mint(s: str) -> bool:
    """Return ``True`` when ``s`` decodes to a 32-byte base58 value."""

    text = str(s).strip()
    if not text:
        return False
    if base58 is not None:
        try:
            raw = base58.b58decode(text)
            return len(raw) == 32
        except Exception:
            return False
    return validate_mint(text)


def clean_candidate_mints(mints: Iterable[str]) -> Tuple[List[str], List[str]]:
    """Split and validate mint candidates, returning ``(valid, dropped)`` lists."""

    valid: List[str] = []
    dropped: List[str] = []
    for m in mints:
        s = str(m).split()[0].split(",")[0].split(";")[0]
        if is_valid_solana_mint(s):
            valid.append(s)
        else:
            dropped.append(m)
    return valid, dropped


__all__ = ["is_valid_solana_mint", "clean_candidate_mints"]
