"""Helpers for normalizing Pump.fun API payloads."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple, TypedDict

from .discovery.mint_resolver import normalize_candidate
from .token_aliases import canonical_mint, validate_mint


class PumpFunEntry(TypedDict, total=False):
    """Canonical representation of a Pump.fun leaderboard entry."""

    mint: str
    name: str
    symbol: str
    icon: str
    rank: Any
    liquidity: Any
    volume_24h: Any
    volume_1h: Any
    volume_5m: Any
    market_cap: Any
    price: Any
    score: Any
    buyers_last_hour: Any
    tweets_last_hour: Any
    sentiment: Any


_PAYLOAD_KEYS: Tuple[str, ...] = ("tokens", "items", "results", "data")

_MINT_KEYS: Tuple[str, ...] = ("mint", "tokenMint", "tokenAddress", "address")

_FIELD_ALIASES: Mapping[str, Tuple[str, ...]] = {
    "name": ("name",),
    "symbol": ("symbol",),
    "icon": ("image_url", "imageUrl", "image"),
    "rank": ("rank", "leaderboardRank"),
    "liquidity": ("liquidity", "liquidityUsd", "liquidity_usd"),
    "volume_24h": ("volume_24h", "volume24h"),
    "volume_1h": ("volume_1h", "volume1h"),
    "volume_5m": ("volume_5m", "volume5m"),
    "market_cap": ("market_cap", "marketCap"),
    "price": ("price", "lastPrice"),
    "score": ("score", "pumpScore"),
    "buyers_last_hour": ("buyersLastHour", "buyers_last_hour"),
    "tweets_last_hour": ("tweetsLastHour", "tweets_last_hour"),
    "sentiment": ("sentiment",),
}


def normalize_pumpfun_payload(payload: Any) -> List[PumpFunEntry]:
    """Return Pump.fun entries with canonical keys and mints."""

    entries = _extract_entries(payload)
    normalized: List[PumpFunEntry] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        mint = _normalize_mint(entry)
        if not mint or mint in seen:
            continue
        seen.add(mint)
        normalized_entry: PumpFunEntry = {"mint": mint}
        for field, aliases in _FIELD_ALIASES.items():
            value = _first_present(entry, aliases)
            if value is None:
                continue
            if field in {"name", "symbol", "icon"} and not isinstance(value, str):
                continue
            normalized_entry[field] = value  # type: ignore[assignment]
        normalized.append(normalized_entry)
    return normalized


def pumpfun_entries_by_mint(payload: Any) -> Dict[str, PumpFunEntry]:
    """Return a mapping from canonical mint to normalized Pump.fun entry."""

    return {entry["mint"]: entry for entry in normalize_pumpfun_payload(payload)}


def _extract_entries(payload: Any) -> Sequence[Any]:
    if isinstance(payload, Mapping):
        for key in _PAYLOAD_KEYS:
            data = payload.get(key)
            if isinstance(data, list):
                return data
            if isinstance(data, tuple):
                return list(data)
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, tuple):
        return list(payload)
    return []


def _normalize_mint(entry: Mapping[str, Any]) -> str | None:
    for key in _MINT_KEYS:
        candidate = entry.get(key)
        if not isinstance(candidate, str):
            continue
        normalized = normalize_candidate(candidate)
        if not normalized:
            continue
        canonical = canonical_mint(normalized)
        if canonical and validate_mint(canonical):
            return canonical
    return None


def _first_present(entry: Mapping[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in entry:
            value = entry[key]
            if value is not None:
                return value
    return None


__all__ = [
    "PumpFunEntry",
    "normalize_pumpfun_payload",
    "pumpfun_entries_by_mint",
]

