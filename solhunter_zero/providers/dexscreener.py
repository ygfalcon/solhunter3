"""Dexscreener REST adapter with defensive parsing and rate limiting."""

from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Sequence

import aiohttp

from solhunter_zero.http import get_session, host_request, host_retry_config

_DEFAULT_BASE_URL = "https://api.dexscreener.com"
_BASE_URL = (os.getenv("DEXSCREENER_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")

try:
    _DEFAULT_TIMEOUT = float(os.getenv("DEXSCREENER_TIMEOUT", "2.0") or 2.0)
except Exception:  # pragma: no cover - defensive
    _DEFAULT_TIMEOUT = 2.0

try:
    _RPS = float(os.getenv("DEXSCREENER_RPS", "1.0") or 1.0)
except Exception:  # pragma: no cover - defensive
    _RPS = 1.0

_RATE_LIMITER_LOCK = asyncio.Lock()
_NEXT_ALLOWED: float = 0.0


async def _acquire_session(
    session: aiohttp.ClientSession | None,
) -> tuple[aiohttp.ClientSession, bool]:
    if session is not None:
        return session, False
    shared = await get_session()
    return shared, False


async def _rate_limit() -> None:
    if _RPS <= 0:
        return
    minimum_interval = 1.0 / max(_RPS, 0.001)
    async with _RATE_LIMITER_LOCK:
        global _NEXT_ALLOWED
        now = time.monotonic()
        wait = _NEXT_ALLOWED - now
        if wait > 0:
            await asyncio.sleep(wait)
            now = time.monotonic()
        _NEXT_ALLOWED = max(now, _NEXT_ALLOWED) + minimum_interval


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(float(value)):
            return None
        return float(value)
    if isinstance(value, str):
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if math.isnan(numeric):
            return None
        return numeric
    if isinstance(value, Mapping):
        for key in ("price", "value", "usd", "amount"):
            if key in value:
                return _coerce_float(value.get(key))
    return None


def _parse_timestamp(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts <= 0:
            return None
        if ts < 1e12:
            ts *= 1000.0
        return int(ts)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.isdigit():
            return _parse_timestamp(float(raw))
        try:
            numeric = float(raw)
        except (TypeError, ValueError):
            numeric = None
        if numeric is not None:
            return _parse_timestamp(numeric)
    return None


def _extract_pairs(payload: Any) -> Sequence[MutableMapping[str, Any]]:
    if isinstance(payload, Mapping):
        for key in ("pairs", "data", "results"):
            pairs = payload.get(key)
            if isinstance(pairs, Sequence):
                return [pair for pair in pairs if isinstance(pair, MutableMapping)]
        return []
    if isinstance(payload, Sequence):
        return [pair for pair in payload if isinstance(pair, MutableMapping)]
    return []


def _token_from_pair(pair: Mapping[str, Any], role: str) -> str:
    token = pair.get(f"{role}Token") or pair.get(f"{role}_token")
    if isinstance(token, Mapping):
        value = token.get("address") or token.get("id") or token.get("mint")
        if isinstance(value, str):
            return value
    if isinstance(token, str):
        return token
    return ""


def _pair_liquidity(pair: Mapping[str, Any]) -> float:
    liquidity = pair.get("liquidity")
    if isinstance(liquidity, Mapping):
        return _coerce_float(liquidity.get("usd")) or 0.0
    return _coerce_float(liquidity) or 0.0


def _pair_timestamp(pair: Mapping[str, Any]) -> int:
    for field in ("updatedAt", "lastTradeUnixTime", "lastUpdatedAt", "pairCreatedAt"):
        ts = _parse_timestamp(pair.get(field))
        if ts is not None:
            return ts
    return int(time.time() * 1000)


def _select_pairs(
    pairs: Iterable[MutableMapping[str, Any]],
    token: str,
) -> list[MutableMapping[str, Any]]:
    token_lower = token.lower()
    filtered: list[MutableMapping[str, Any]] = []
    for pair in pairs:
        base = _token_from_pair(pair, "base").lower()
        quote = _token_from_pair(pair, "quote").lower()
        if token_lower and token_lower not in {base, quote}:
            continue
        filtered.append(pair)
    ranked = sorted(
        filtered,
        key=lambda item: (
            -_pair_liquidity(item),
            -_pair_timestamp(item),
        ),
    )
    if ranked:
        return ranked
    return sorted(
        list(filtered or list(pairs)),
        key=lambda item: (
            -_pair_liquidity(item),
            -_pair_timestamp(item),
        ),
    )


def _compute_spread_bps(pair: Mapping[str, Any]) -> float | None:
    change = pair.get("priceChange") or pair.get("price_change")
    if isinstance(change, Mapping):
        snap = change.get("m5") or change.get("m1")
        spread = _coerce_float(snap)
        if spread is not None:
            return abs(spread) * 100.0
    native = _coerce_float(pair.get("priceNative") or pair.get("price_native"))
    usd = _coerce_float(pair.get("priceUsd") or pair.get("price_usd"))
    if native is not None and usd is not None and usd > 0:
        return abs(native - usd) / usd * 10000.0
    return None


def _build_venues(
    pairs: Sequence[MutableMapping[str, Any]],
) -> list[Dict[str, Any]]:
    venues: list[Dict[str, Any]] = []
    for pair in pairs[:3]:
        liquidity = _pair_liquidity(pair)
        dex_name = str(pair.get("dexId") or pair.get("dex_id") or "").strip()
        pair_addr = (
            pair.get("pairAddress")
            or pair.get("pair_address")
            or pair.get("poolAddress")
            or pair.get("id")
        )
        venue: Dict[str, Any] = {
            "name": dex_name or "dex",
            "pair": str(pair_addr or ""),
            "liquidity_usd": float(liquidity),
        }
        venues.append(venue)
    return venues


async def fetch(
    token_or_mint: str,
    *,
    timeout: float | None = None,
    session: aiohttp.ClientSession | None = None,
) -> Dict[str, Any]:
    """Fetch best-effort pricing details for ``token_or_mint`` from Dexscreener."""

    token = (token_or_mint or "").strip()
    if not token:
        raise ValueError("token_or_mint must be a non-empty string")

    url = f"{_BASE_URL}/latest/dex/tokens/{token}"
    timeout_value = float(timeout or _DEFAULT_TIMEOUT)
    client_timeout = aiohttp.ClientTimeout(total=timeout_value)

    session_obj, _ = await _acquire_session(session)
    attempts, backoff = host_retry_config(url)
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        await _rate_limit()
        try:
            async with host_request(url):
                async with session_obj.get(
                    url,
                    headers={"Accept": "application/json"},
                    timeout=client_timeout,
                ) as resp:
                    resp.raise_for_status()
                    payload = await resp.json()
        except Exception as exc:  # pragma: no cover - retried path
            last_error = exc
            if attempt + 1 >= max(1, attempts):
                raise
            await asyncio.sleep(backoff * (2**attempt))
            continue

        pairs = _extract_pairs(payload)
        ranked = _select_pairs(pairs, token)
        venues = _build_venues(ranked)
        best_pair = ranked[0] if ranked else None

        if best_pair is not None:
            price = (
                _coerce_float(best_pair.get("priceUsd"))
                or _coerce_float(best_pair.get("price"))
                or _coerce_float(payload.get("priceUsd"))
            )
            as_of = _pair_timestamp(best_pair)
            spread_bps = _compute_spread_bps(best_pair)
            liquidity = float(_pair_liquidity(best_pair))
        else:
            price = _coerce_float(payload.get("priceUsd"))
            as_of = _parse_timestamp(payload.get("timestamp")) or int(time.time() * 1000)
            spread_bps = None
            liquidity = 0.0

        return {
            "price": float(price) if price is not None else None,
            "liquidity_usd": liquidity,
            "spread_bps": spread_bps,
            "venues": venues,
            "as_of": as_of,
            "source": "dexscreener",
            "pairs": [dict(pair) for pair in ranked],
        }

    if last_error is not None:
        raise last_error
    
    # If we somehow exit the retry loop without returning or raising, fallback.
    now_ms = int(time.time() * 1000)
    return {
        "price": None,
        "liquidity_usd": 0.0,
        "spread_bps": None,
        "venues": [],
        "as_of": now_ms,
        "source": "dexscreener",
        "pairs": [],
    }


__all__ = ["fetch"]

