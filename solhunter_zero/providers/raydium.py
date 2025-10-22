"""Raydium public API adapter used for discovery metadata."""

from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Sequence

import aiohttp

from solhunter_zero.http import get_session, host_request, host_retry_config

_DEFAULT_URL = "https://api.raydium.io/v2/amm/new-pairs"
_BASE_URL = (os.getenv("RAYDIUM_POOLS_URL") or _DEFAULT_URL).strip() or _DEFAULT_URL

try:
    _DEFAULT_TIMEOUT = float(os.getenv("RAYDIUM_TIMEOUT", "2.0") or 2.0)
except Exception:  # pragma: no cover - defensive
    _DEFAULT_TIMEOUT = 2.0


async def _acquire_session(
    session: aiohttp.ClientSession | None,
) -> tuple[aiohttp.ClientSession, bool]:
    if session is not None:
        return session, False
    owned = await get_session()
    return owned, True


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
        for key in ("usd", "value", "amount", "price"):
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
        for key in ("data", "pairs", "result", "poolList", "items"):
            pairs = payload.get(key)
            if isinstance(pairs, Sequence):
                return [pair for pair in pairs if isinstance(pair, MutableMapping)]
        return []
    if isinstance(payload, Sequence):
        return [pair for pair in payload if isinstance(pair, MutableMapping)]
    return []


def _token_value(entry: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        raw = entry.get(key)
        if isinstance(raw, str) and raw:
            return raw
    return ""


def _normalise_pairs(
    pairs: Iterable[MutableMapping[str, Any]],
    token: str | None,
) -> list[Dict[str, Any]]:
    token_lower = (token or "").lower()
    normalised: list[Dict[str, Any]] = []
    now_ms = int(time.time() * 1000)
    for pair in pairs:
        base = _token_value(pair, "baseMint", "base_mint", "mint")
        quote = _token_value(pair, "quoteMint", "quote_mint", "quote")
        if token_lower:
            matches = {base.lower(), quote.lower()}
            if token_lower not in matches:
                continue
        liquidity = (
            _coerce_float(pair.get("liquidity"))
            or _coerce_float(pair.get("liquidityUsd"))
            or _coerce_float(pair.get("tvl"))
            or 0.0
        )
        price = _coerce_float(pair.get("priceUsd") or pair.get("price"))
        created = (
            _parse_timestamp(pair.get("createdAt") or pair.get("created_at"))
            or _parse_timestamp(pair.get("timestamp"))
            or now_ms
        )
        name = _token_value(pair, "name", "baseName", "base_name")
        symbol = _token_value(pair, "symbol", "baseSymbol", "base_symbol")
        pool = _token_value(pair, "ammId", "id", "poolId", "pool")
        record = {
            "mint_base": base,
            "mint_quote": quote,
            "liquidity_usd": float(liquidity or 0.0),
            "price_usd": float(price) if price is not None else None,
            "as_of": created,
            "name": name,
            "symbol": symbol,
            "pool": pool,
        }
        normalised.append(record)
    normalised.sort(key=lambda item: (-item["liquidity_usd"], -item["as_of"]))
    return normalised


async def fetch(
    token_or_mint: str | None,
    *,
    timeout: float | None = None,
    session: aiohttp.ClientSession | None = None,
) -> Dict[str, Any]:
    """Fetch Raydium new-pair metadata, optionally filtered by ``token_or_mint``."""

    url = _BASE_URL
    timeout_value = float(timeout or _DEFAULT_TIMEOUT)
    client_timeout = aiohttp.ClientTimeout(total=timeout_value)

    owned_session: aiohttp.ClientSession | None = None
    try:
        session_obj, owned = await _acquire_session(session)
        if owned:
            owned_session = session_obj
        attempts, backoff = host_retry_config(url)
        last_error: Exception | None = None
        for attempt in range(max(1, attempts)):
            try:
                async with host_request(url):
                    async with session_obj.get(
                        url,
                        headers={"accept": "application/json"},
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
            normalised = _normalise_pairs(pairs, token_or_mint)
            top = normalised[0] if normalised else None
            as_of = top["as_of"] if top else int(time.time() * 1000)
            price = top["price_usd"] if top else None
            liquidity = top["liquidity_usd"] if top else 0.0
            venues = []
            for entry in normalised[:3]:
                venues.append(
                    {
                        "name": "raydium",
                        "pair": entry.get("pool", ""),
                        "liquidity_usd": entry.get("liquidity_usd", 0.0),
                    }
                )
            return {
                "price": price,
                "liquidity_usd": liquidity,
                "spread_bps": None,
                "venues": venues,
                "as_of": as_of,
                "source": "raydium",
                "pairs": normalised,
            }

        if last_error is not None:
            raise last_error
    finally:
        if owned_session is not None:
            await owned_session.close()

    now_ms = int(time.time() * 1000)
    return {
        "price": None,
        "liquidity_usd": 0.0,
        "spread_bps": None,
        "venues": [],
        "as_of": now_ms,
        "source": "raydium",
        "pairs": [],
    }


__all__ = ["fetch"]

