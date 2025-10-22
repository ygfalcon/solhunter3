"""Orca Whirlpool catalog adapter."""

from __future__ import annotations

import asyncio
import math
import os
import time
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Sequence

import aiohttp

from solhunter_zero.http import get_session, host_request, host_retry_config

_DEFAULT_URL = "https://api.mainnet.orca.so/v1/whirlpool/list"
_BASE_URL = (os.getenv("ORCA_POOLS_URL") or _DEFAULT_URL).strip() or _DEFAULT_URL

try:
    _DEFAULT_TIMEOUT = float(os.getenv("ORCA_TIMEOUT", "2.0") or 2.0)
except Exception:  # pragma: no cover - defensive
    _DEFAULT_TIMEOUT = 2.0


async def _acquire_session(
    session: aiohttp.ClientSession | None,
) -> tuple[aiohttp.ClientSession, bool]:
    if session is not None:
        return session, False
    shared = await get_session()
    return shared, False


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
            numeric = None
        if numeric is None or math.isnan(numeric):
            return None
        return numeric
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
        if raw.isdigit():
            return _parse_timestamp(float(raw))
        try:
            numeric = float(raw)
        except (TypeError, ValueError):
            return None
        return _parse_timestamp(numeric)
    return None


def _extract_catalog(pools: Iterable[MutableMapping[str, Any]]) -> Dict[str, list[Dict[str, Any]]]:
    catalog: Dict[str, list[Dict[str, Any]]] = {}
    for pool in pools:
        address = str(pool.get("address") or pool.get("id") or "")
        token_a = _mint_from(pool.get("tokenA"))
        token_b = _mint_from(pool.get("tokenB"))
        if not token_a and not token_b:
            continue
        liquidity = (
            _coerce_float(pool.get("tvl"))
            or _coerce_float(pool.get("liquidity"))
            or 0.0
        )
        price = _coerce_float(pool.get("price"))
        timestamp = (
            _parse_timestamp(pool.get("modifiedTimeMs"))
            or _parse_timestamp(pool.get("modified_at"))
            or int(time.time() * 1000)
        )
        entry = {
            "pool": address,
            "token_a": token_a,
            "token_b": token_b,
            "price": price,
            "liquidity_usd": float(liquidity or 0.0),
            "as_of": timestamp,
            "tick_spacing": pool.get("tickSpacing") or pool.get("tick_spacing"),
            "whitelisted": bool(pool.get("whitelisted", False)),
        }
        for mint in (token_a, token_b):
            if not mint:
                continue
            catalog.setdefault(mint, []).append(entry)
    for entries in catalog.values():
        entries.sort(key=lambda item: (-item["liquidity_usd"], -item["as_of"]))
    return catalog


def _mint_from(token_block: Any) -> str:
    if isinstance(token_block, Mapping):
        raw = token_block.get("mint") or token_block.get("address")
        if isinstance(raw, str):
            return raw
    if isinstance(token_block, str):
        return token_block
    return ""


async def fetch(
    token_or_mint: str | None = None,
    *,
    timeout: float | None = None,
    session: aiohttp.ClientSession | None = None,
) -> Dict[str, Any]:
    """Fetch the Orca Whirlpool list and return a mint → pool catalog."""

    url = _BASE_URL
    timeout_value = float(timeout or _DEFAULT_TIMEOUT)
    client_timeout = aiohttp.ClientTimeout(total=timeout_value)

    session_obj, _ = await _acquire_session(session)
    attempts, backoff = host_retry_config(url)
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
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

        pools = []
        if isinstance(payload, Mapping):
            pools_raw = payload.get("whirlpools") or payload.get("data")
        else:
            pools_raw = payload
        if isinstance(pools_raw, Sequence):
            pools = [pool for pool in pools_raw if isinstance(pool, MutableMapping)]
        catalog = _extract_catalog(pools)
        now_ms = int(time.time() * 1000)
        venues = []
        liquidity = 0.0
        if token_or_mint:
            entries = catalog.get(token_or_mint, [])
            if entries:
                top = entries[0]
                liquidity = top.get("liquidity_usd", 0.0)
                venues.append(
                    {
                        "name": "orca",
                        "pair": top.get("pool", ""),
                        "liquidity_usd": top.get("liquidity_usd", 0.0),
                    }
                )
        return {
            "price": None,
            "liquidity_usd": float(liquidity),
            "spread_bps": None,
            "venues": venues,
            "as_of": now_ms,
            "source": "orca",
            "catalog": catalog,
        }

    if last_error is not None:
        raise last_error

    now_ms = int(time.time() * 1000)
    return {
        "price": None,
        "liquidity_usd": 0.0,
        "spread_bps": None,
        "venues": [],
        "as_of": now_ms,
        "source": "orca",
        "catalog": {},
    }


__all__ = ["fetch"]

