"""Utilities for validating and cleaning Solana mint addresses."""
from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any, Dict, Iterable, List, Sequence, Tuple

import base58

# Base58 (Bitcoin) alphabet used by Solana
_BASE58_ALPHABET = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

# Heuristic patterns to pull mints out of free text/URLs quickly
_MINT_RE = re.compile(r"([1-9A-HJ-NP-Za-km-z]{32,44})")
_SOLSCAN_TOKEN_RE = re.compile(r"solscan\\.io/(?:token|account)/([1-9A-HJ-NP-Za-km-z]{32,44})", re.I)
_QUERY_PARAM_RE = re.compile(r"[?&](?:mint|address|token)=([1-9A-HJ-NP-Za-km-z]{32,44})", re.I)


@dataclass(frozen=True)
class MintCleanResult:
    valid: List[str]
    dropped: List[Any]
    reasons: Dict[Any, str]


def is_valid_solana_mint(mint: str) -> bool:
    """Return True when `mint` decodes to a 32-byte base58 string."""
    if not isinstance(mint, str):
        return False
    s = mint.strip()
    # quick length & alphabet gate
    if not (32 <= len(s) <= 44) or any(ch not in _BASE58_ALPHABET for ch in s):
        return False
    try:
        raw = base58.b58decode(s)
    except Exception:
        return False
    return len(raw) == 32


def _coerce_iter(mints: Iterable[Any] | Any) -> Iterable[Any]:
    # Accept a single string of comma/space/newline separated tokens as well
    if isinstance(mints, str):
        # split on commas/semicolons/whitespace
        parts = re.split(r"[,\s;]+", mints.strip())
        return (p for p in parts if p)
    return mints


def _extract_candidate(text: str) -> str:
    """Strip obvious artifacts: URLs, query params, trailing punctuation."""
    t = text.strip()
    # Try explicit URL/query param captures
    for rex in (_QUERY_PARAM_RE, _SOLSCAN_TOKEN_RE):
        m = rex.search(t)
        if m:
            return m.group(1)
    # Fallback: take the first plausible base58 chunk
    m = _MINT_RE.search(t)
    return m.group(1) if m else t


_LOGGER = logging.getLogger(__name__)
_LOGGED_INVALID_SOURCES: set[tuple[str, str]] = set()


def _summarise_value(value: Any, *, limit: int = 80) -> str:
    try:
        rendered = str(value)
    except Exception:
        return "<unrenderable>"
    if len(rendered) > limit:
        return f"{rendered[: limit - 3]}..."
    return rendered


def _log_invalid_source(source: str | None, value: Any, reason: str) -> None:
    if not source:
        return
    key = (source, reason)
    if key in _LOGGED_INVALID_SOURCES:
        return
    _LOGGED_INVALID_SOURCES.add(key)
    summary = _summarise_value(value)
    _LOGGER.warning(
        "Invalid mint filtered from %s (%s): %s",
        source,
        reason,
        summary,
    )


def clean_candidate_mints(
    mints: Sequence[object] | str,
    source: str | None = None,
) -> Tuple[List[str], List[object]]:
    """Backwards-compatible API: returns (valid, dropped)."""
    res = clean_mints(mints, source=source)
    return res.valid, res.dropped


def clean_mints(
    mints: Sequence[object] | str, *, source: str | None = None
) -> MintCleanResult:
    """Split, extract, validate, and dedupe potential mint addresses."""
    seen: set[str] = set()
    valid: List[str] = []
    dropped: List[Any] = []
    reasons: Dict[Any, str] = {}

    for raw in _coerce_iter(mints):
        # Keep original object for diagnostics
        orig = raw
        try:
            s = str(raw)
        except Exception:
            dropped.append(orig)
            reasons[orig] = "unstringifiable"
            continue

        cand = _extract_candidate(s)
        # common trailing punctuation in chats/CSV
        cand = cand.strip().strip(")]},.;:'\"")

        if not cand:
            dropped.append(orig)
            reasons[orig] = "empty"
            _log_invalid_source(source, orig, "empty")
            continue
        if not (32 <= len(cand) <= 44):
            dropped.append(orig)
            reasons[orig] = "bad_length"
            _log_invalid_source(source, orig, "bad_length")
            continue
        if any(ch not in _BASE58_ALPHABET for ch in cand):
            dropped.append(orig)
            reasons[orig] = "invalid_chars"
            _log_invalid_source(source, orig, "invalid_chars")
            continue
        try:
            raw_bytes = base58.b58decode(cand)
        except Exception:
            dropped.append(orig)
            reasons[orig] = "b58decode_error"
            _log_invalid_source(source, orig, "b58decode_error")
            continue
        if len(raw_bytes) != 32:
            dropped.append(orig)
            reasons[orig] = "decoded_len!=32"
            _log_invalid_source(source, orig, "decoded_len!=32")
            continue
        if cand in seen:
            # silently dedupe
            continue
        seen.add(cand)
        valid.append(cand)

    return MintCleanResult(valid=valid, dropped=dropped, reasons=reasons)


__all__ = [
    "is_valid_solana_mint",
    "clean_candidate_mints",
    "clean_mints",
    "MintCleanResult",
]
