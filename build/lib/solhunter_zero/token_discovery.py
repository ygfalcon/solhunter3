# solhunter_zero/token_discovery.py
from __future__ import annotations

import asyncio
import contextlib
import math
import os
import logging
import threading
from typing import Dict, List

from aiohttp import ClientTimeout
import aiohttp

from .scanner_common import (
    BIRDEYE_API,
    BIRDEYE_API_KEY,
    HEADERS,
)
from .lru import TTLCache
from .mempool_scanner import stream_ranked_mempool_tokens_with_depth

logger = logging.getLogger(__name__)

_FAST_MODE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: str, *, fast_default: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        if _FAST_MODE and fast_default is not None:
            return float(fast_default)
        raw = default
    try:
        value = float(raw)
    except Exception:
        value = float(default)
    return value


_MIN_VOLUME = _env_float("DISCOVERY_MIN_VOLUME_USD", "50000", fast_default=0.0)
_MIN_LIQUIDITY = _env_float("DISCOVERY_MIN_LIQUIDITY_USD", "75000", fast_default=0.0)
_MAX_TOKENS = int(os.getenv("DISCOVERY_MAX_TOKENS", "50") or 50)
_PAGE_LIMIT = max(1, min(int(os.getenv("DISCOVERY_PAGE_SIZE", "50") or 50), 50))
_OVERFETCH_FACTOR = float(os.getenv("DISCOVERY_OVERFETCH_FACTOR", "1.0") or 1.0)
_CACHE_TTL = float(os.getenv("DISCOVERY_CACHE_TTL", "45") or 45)
_MAX_OFFSET = int(os.getenv("DISCOVERY_MAX_OFFSET", "4000") or 4000)
_MEMPOOL_LIMIT = int(os.getenv("DISCOVERY_MEMPOOL_LIMIT", "12") or 12)
_VOLUME_WEIGHT = float(os.getenv("DISCOVERY_VOLUME_WEIGHT", "0.45") or 0.45)
_LIQUIDITY_WEIGHT = float(os.getenv("DISCOVERY_LIQUIDITY_WEIGHT", "0.55") or 0.55)
_MEMPOOL_BONUS = float(os.getenv("DISCOVERY_MEMPOOL_BONUS", "5.0") or 5.0)
_ENABLE_MEMPOOL = os.getenv("DISCOVERY_ENABLE_MEMPOOL", "1").lower() in {"1", "true", "yes"}
_WARM_TIMEOUT = float(os.getenv("DISCOVERY_WARM_TIMEOUT", "5") or 5)
_BIRDEYE_RETRIES = int(os.getenv("DISCOVERY_BIRDEYE_RETRIES", "3") or 3)
_BIRDEYE_BACKOFF = float(os.getenv("DISCOVERY_BIRDEYE_BACKOFF", "1.0") or 1.0)
_BIRDEYE_BACKOFF_MAX = float(os.getenv("DISCOVERY_BIRDEYE_BACKOFF_MAX", "8.0") or 8.0)

_BIRDEYE_CACHE: TTLCache[str, List[Dict[str, float]]] = TTLCache(maxsize=1, ttl=_CACHE_TTL)


def _score_component(value: float) -> float:
    if value <= 0:
        return 0.0
    return math.log1p(value)


async def _fetch_birdeye_tokens() -> List[Dict[str, float]]:
    """
    Pull BirdEye token list (paginated) for Solana with correct headers & params.
    Numeric filters only; no name/suffix heuristics.
    """
    api_key = os.getenv("BIRDEYE_API_KEY") or BIRDEYE_API_KEY
    if not api_key:
        logger.debug("BirdEye API key missing; skipping BirdEye discovery")
        return []

    # Keep HEADERS in sync for other modules
    if HEADERS.get("X-API-KEY") != api_key:
        HEADERS["X-API-KEY"] = api_key
    HEADERS.setdefault("Accept", "application/json")
    HEADERS["x-chain"] = "solana"  # BirdEye accepts chain via header

    cached = _BIRDEYE_CACHE.get("tokens")
    if cached is not None:
        return cached

    tokens: Dict[str, Dict[str, float]] = {}
    offset = 0
    target_count = max(int(_MAX_TOKENS * _OVERFETCH_FACTOR), _PAGE_LIMIT)
    backoff = _BIRDEYE_BACKOFF

    # Build a per-request headers copy to avoid accidental mutation
    def _headers() -> dict:
        return {
            "X-API-KEY": api_key,
            "x-chain": "solana",
            "Accept": "application/json",
        }

    async with aiohttp.ClientSession(timeout=ClientTimeout(total=12)) as session:
        while offset < _MAX_OFFSET and len(tokens) < target_count:
            params = {
                "offset": offset,
                "limit": _PAGE_LIMIT,
                "sortBy": "v24hUSD",
                "chain": "solana",  # also pass chain in query to satisfy stricter backends
            }
            try:
                async with session.get(BIRDEYE_API, params=params, headers=_headers()) as resp:
                    if resp.status in (429, 503):
                        if tokens:
                            logger.info("BirdEye %s after %s tokens; using partial batch", resp.status, len(tokens))
                            break
                        text = await resp.text()
                        logger.warning("BirdEye %s at offset %s: %s; backoff %.1fs", resp.status, offset, text[:200], backoff)
                        await asyncio.sleep(backoff)
                        backoff = min(backoff * 2, _BIRDEYE_BACKOFF_MAX)
                        continue

                    if resp.status == 400:
                        text = await resp.text()
                        logger.warning("BirdEye 400 at offset %s (params=%r): %s", offset, params, text[:200])
                        # Do not loop infinitely on 400; bail out.
                        break

                    resp.raise_for_status()
                    payload = await resp.json()
                    backoff = _BIRDEYE_BACKOFF  # reset backoff on success

            except aiohttp.ClientResponseError as exc:
                logger.warning("BirdEye token fetch failed at offset %s: %s", offset, exc)
                if not tokens:
                    _BIRDEYE_CACHE.set("tokens", [])
                break
            except Exception as exc:
                logger.warning("BirdEye token fetch failed at offset %s: %s", offset, exc)
                if not tokens:
                    _BIRDEYE_CACHE.set("tokens", [])
                break

            data = payload.get("data", {})
            items = data.get("tokens") or data.get("list") or []
            if not items:
                break

            for item in items:
                address = item.get("address") or item.get("mint")
                if not address:
                    continue
                try:
                    volume = float(item.get("v24hUSD") or item.get("volume24hUSD") or item.get("volume") or 0.0)
                except Exception:
                    volume = 0.0
                try:
                    liquidity = float(item.get("liquidity") or 0.0)
                except Exception:
                    liquidity = 0.0
                try:
                    price = float(item.get("price") or 0.0)
                except Exception:
                    price = 0.0
                try:
                    change = float(item.get("v24hChangePercent") or 0.0)
                except Exception:
                    change = 0.0

                if _MIN_VOLUME and volume < _MIN_VOLUME:
                    continue
                if _MIN_LIQUIDITY and liquidity < _MIN_LIQUIDITY:
                    continue

                entry = tokens.setdefault(
                    address,
                    {
                        "address": address,
                        "symbol": item.get("symbol") or "",
                        "name": item.get("name") or item.get("symbol") or address,
                        "liquidity": liquidity,
                        "volume": volume,
                        "price": price,
                        "price_change": change,
                        "sources": {"birdeye"},
                    },
                )
                # Aggregate max across pages
                entry["liquidity"] = max(entry["liquidity"], liquidity)
                entry["volume"] = max(entry["volume"], volume)
                entry["price"] = price or entry.get("price", 0.0)
                entry["price_change"] = change

            offset += _PAGE_LIMIT
            total = data.get("total")
            try:
                total_int = int(total) if total is not None else None
            except (TypeError, ValueError):
                total_int = None
            if total_int is not None and offset >= total_int:
                break

    result = list(tokens.values())
    _BIRDEYE_CACHE.set("tokens", result)
    if not result:
        logger.warning("Token discovery: BirdEye returned no items after filtering.")
    return result


async def _collect_mempool_signals(rpc_url: str, threshold: float) -> Dict[str, Dict[str, float]]:
    """Collect a small batch of ranked mempool candidates (with depth)."""
    scores: Dict[str, Dict[str, float]] = {}
    gen = None
    try:
        gen = stream_ranked_mempool_tokens_with_depth(rpc_url, threshold=threshold)
        async for item in gen:
            addr = item.get("address")
            if not addr:
                continue
            scores[addr] = item
            if len(scores) >= _MEMPOOL_LIMIT:
                break
    except Exception as exc:
        logger.debug("Mempool stream unavailable: %s", exc)
    finally:
        if gen is not None:
            with contextlib.suppress(Exception):
                await gen.aclose()
    return scores


async def discover_candidates(
    rpc_url: str,
    *,
    limit: int | None = None,
    mempool_threshold: float | None = None,
) -> List[Dict[str, float]]:
    """Combine BirdEye numeric candidates with mempool signals and rank."""
    if limit is None or limit <= 0:
        limit = _MAX_TOKENS
    if mempool_threshold is None:
        mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)

    bird_task = asyncio.create_task(_fetch_birdeye_tokens())
    mempool_task = (
        asyncio.create_task(_collect_mempool_signals(rpc_url, mempool_threshold))
        if _ENABLE_MEMPOOL and rpc_url
        else None
    )

    results = await asyncio.gather(*(t for t in (bird_task, mempool_task) if t), return_exceptions=True)
    bird_tokens: List[Dict[str, float]] = []
    mempool: Dict[str, Dict[str, float]] = {}

    if results:
        bird_res = results[0]
        if isinstance(bird_res, Exception):
            logger.warning("BirdEye discovery failed: %s", bird_res)
        else:
            bird_tokens = bird_res or []

        if len(results) > 1:
            mp_res = results[1]
            if isinstance(mp_res, Exception):
                logger.debug("Mempool signals unavailable: %s", mp_res)
            else:
                mempool = mp_res or {}

    candidates: Dict[str, Dict[str, float]] = {}
    bird_addresses = set()
    for token in bird_tokens:
        addr = token.get("address")
        if not addr:
            continue
        entry = dict(token)
        entry["sources"] = set(token.get("sources", {"birdeye"}))
        candidates[addr] = entry
        bird_addresses.add(addr)

    for addr, mp in mempool.items():
        entry = candidates.setdefault(
            addr,
            {
                "address": addr,
                "symbol": mp.get("symbol", ""),
                "name": mp.get("name", addr),
                "liquidity": float(mp.get("liquidity", 0.0) or 0.0),
                "volume": float(mp.get("volume", 0.0) or 0.0),
                "price": float(mp.get("price", 0.0) or 0.0),
                "price_change": 0.0,
                "sources": set(),
            },
        )
        entry.setdefault("sources", set()).add("mempool")
        for key in ("score", "momentum", "anomaly", "wallet_concentration", "avg_swap_size"):
            val = mp.get(key)
            if val is not None:
                try:
                    entry[key] = float(val)
                except Exception:
                    pass

    for addr, entry in candidates.items():
        entry_sources = entry.setdefault("sources", set())
        if addr in bird_addresses:
            entry_sources.add("birdeye")
        if addr in mempool:
            entry_sources.add("mempool")

        liquidity = float(entry.get("liquidity", 0.0) or 0.0)
        volume = float(entry.get("volume", 0.0) or 0.0)

        base = (
            _LIQUIDITY_WEIGHT * _score_component(liquidity)
            + _VOLUME_WEIGHT * _score_component(volume)
        )

        mp = mempool.get(addr)
        if mp:
            try:
                base += _MEMPOOL_BONUS * float(mp.get("score", 0.0) or 0.0)
            except Exception:
                pass

        try:
            change = float(entry.get("price_change", 0.0) or 0.0)
        except Exception:
            change = 0.0
        base *= max(0.5, 1.0 + (change / 100.0) * 0.1)

        entry["score"] = base

    ordered = sorted(candidates.values(), key=lambda c: c.get("score", 0.0), reverse=True)

    final: List[Dict[str, float]] = []
    for entry in ordered[:limit]:
        entry["sources"] = sorted(entry.get("sources", []))
        final.append(entry)

    return final


def warm_cache(rpc_url: str, *, limit: int | None = None) -> None:
    """Prime the discovery cache synchronously (best-effort)."""
    if not (rpc_url or BIRDEYE_API_KEY):
        return

    limit = limit or min(_MAX_TOKENS, 10)
    mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)
    coro = discover_candidates(rpc_url, limit=limit, mempool_threshold=mempool_threshold)

    def _worker() -> None:
        try:
            asyncio.run(asyncio.wait_for(coro, timeout=_WARM_TIMEOUT))
        except Exception:
            pass

    thread = threading.Thread(target=_worker, name="discovery-warm", daemon=True)
    thread.start()
