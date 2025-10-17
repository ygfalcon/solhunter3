# solhunter_zero/token_discovery.py
from __future__ import annotations

import asyncio
import contextlib
import math
import os
import logging
import threading
from threading import Lock
from typing import Dict, List, Any, Union

from aiohttp import ClientTimeout
import aiohttp

from .scanner_common import (
    BIRDEYE_API,
    BIRDEYE_API_KEY,
)
from .lru import TTLCache
from .mempool_scanner import stream_ranked_mempool_tokens_with_depth
from .util.mints import is_valid_solana_mint

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

TokenEntry = Dict[str, Union[float, str, List[str]]]

_BIRDEYE_CACHE: TTLCache[str, List[TokenEntry]] = TTLCache(maxsize=1, ttl=_CACHE_TTL)
_CACHE_LOCK = Lock()
_BIRDEYE_DISABLED_INFO = False


def _cache_get(key: str) -> List[TokenEntry] | None:
    with _CACHE_LOCK:
        return _BIRDEYE_CACHE.get(key)


def _cache_set(key: str, value: List[TokenEntry]) -> None:
    with _CACHE_LOCK:
        _BIRDEYE_CACHE.set(key, value)


def _cache_clear() -> None:
    with _CACHE_LOCK:
        _BIRDEYE_CACHE.clear()


def _current_cache_key() -> str:
    return f"tokens:{int(_MIN_VOLUME)}:{int(_MIN_LIQUIDITY)}:{_PAGE_LIMIT}"


async def get_session(*, timeout: ClientTimeout | None = None) -> aiohttp.ClientSession:
    """Factory for aiohttp sessions; patched in tests."""
    return aiohttp.ClientSession(timeout=timeout)


async def fetch_trending_tokens_async() -> List[str]:  # pragma: no cover - legacy hook
    """Compatibility shim for older tests; returns no extra tokens."""
    return []


def _score_component(value: float) -> float:
    try:
        numeric = float(value)
    except Exception:
        return 0.0
    if numeric <= 0 or math.isnan(numeric):
        return 0.0
    return math.log1p(numeric)


def _clear_birdeye_cache_for_tests() -> None:
    """Testing helper: clear the BirdEye TTL cache."""
    try:
        _cache_clear()
    except Exception:
        pass


async def _fetch_birdeye_tokens() -> List[TokenEntry]:
    """
    Pull BirdEye token list (paginated) for Solana with correct headers & params.
    Numeric filters only; no name/suffix heuristics.
    """
    api_key = os.getenv("BIRDEYE_API_KEY") or BIRDEYE_API_KEY
    global _BIRDEYE_DISABLED_INFO
    if not api_key:
        if not _BIRDEYE_DISABLED_INFO and not _ENABLE_MEMPOOL:
            logger.info(
                "Discovery sources disabled: set BIRDEYE_API_KEY or enable DISCOVERY_ENABLE_MEMPOOL to restore BirdEye/mempool inputs.",
            )
            _BIRDEYE_DISABLED_INFO = True
        logger.debug("BirdEye API key missing; skipping BirdEye discovery")
        return []

    cache_key = _current_cache_key()
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    tokens: Dict[str, TokenEntry] = {}
    offset = 0
    target_count = max(int(_MAX_TOKENS * _OVERFETCH_FACTOR), _PAGE_LIMIT)
    backoff = _BIRDEYE_BACKOFF

    logger.debug(
        "BirdEye fetch start offset=%s limit=%s target=%s", offset, _PAGE_LIMIT, target_count
    )

    def _headers() -> dict:
        return {
            "X-API-KEY": api_key,
            "x-chain": "solana",
            "Accept": "application/json",
        }

    session_timeout = ClientTimeout(total=12, connect=4, sock_read=8)
    try:
        session_cm = await get_session(timeout=session_timeout)
    except TypeError:
        session_cm = await get_session()
    async with session_cm as session:
        while offset < _MAX_OFFSET and len(tokens) < target_count:
            logger.debug(
                "BirdEye fetch page offset=%s limit=%s accumulated=%s",
                offset,
                _PAGE_LIMIT,
                len(tokens),
            )
            params = {
                "offset": offset,
                "limit": _PAGE_LIMIT,
                "sortBy": "v24hUSD",
                "chain": "solana",  # also pass chain in query to satisfy stricter backends
            }
            payload: Dict[str, Any] | None = None
            for attempt in range(1, _BIRDEYE_RETRIES + 1):
                try:
                    try:
                        request_cm = session.get(
                            BIRDEYE_API, params=params, headers=_headers()
                        )
                    except TypeError:
                        request_cm = session.get(BIRDEYE_API, headers=_headers())
                    async with request_cm as resp:
                        if resp.status in (429, 503) or 500 <= resp.status < 600:
                            logger.warning(
                                "BirdEye %s attempt=%s offset=%s backoff=%.2fs",
                                resp.status,
                                attempt,
                                offset,
                                backoff,
                            )
                            delay = backoff
                            retry_after = resp.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    ra_val = min(float(retry_after), _BIRDEYE_BACKOFF_MAX)
                                    delay = max(delay, ra_val)
                                except ValueError:
                                    pass
                            if attempt >= _BIRDEYE_RETRIES:
                                if tokens:
                                    logger.warning(
                                        "BirdEye %s after %s tokens; returning partial results",
                                        resp.status,
                                        len(tokens),
                                    )
                                    payload = None
                                    break
                                _cache_set(cache_key, [])
                                return []
                            await asyncio.sleep(delay)
                            backoff = min(max(backoff * 2, delay), _BIRDEYE_BACKOFF_MAX)
                            continue

                        if resp.status == 400:
                            text = await resp.text()
                            logger.warning(
                                "BirdEye 400 at offset %s (params=%r): %s",
                                offset,
                                params,
                                text[:200],
                            )
                            payload = None
                            offset = _MAX_OFFSET
                            break

                        resp.raise_for_status()
                        payload = await resp.json()
                        backoff = _BIRDEYE_BACKOFF
                        break
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    logger.warning(
                        "BirdEye request error attempt=%s offset=%s: %s",
                        attempt,
                        offset,
                        exc,
                    )
                    if attempt >= _BIRDEYE_RETRIES:
                        if tokens:
                            logger.warning(
                                "BirdEye retries exhausted after %s tokens; returning partial",
                                len(tokens),
                            )
                            payload = None
                            break
                        _cache_set(cache_key, [])
                        return []
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _BIRDEYE_BACKOFF_MAX)
                    continue
                except Exception as exc:
                    logger.warning("BirdEye unexpected error offset=%s: %s", offset, exc)
                    if not tokens:
                        _cache_set(cache_key, [])
                        return []
                    payload = None
                    break
                else:
                    # Success path already breaks out of loop
                    pass

                if payload is not None:
                    break

            if payload is None:
                break

            data = payload.get("data", {})
            items = data.get("tokens") or data.get("list") or []
            if not items:
                break

            for item in items:
                raw_address = item.get("address") or item.get("mint")
                if not raw_address:
                    continue
                address = str(raw_address)
                if not is_valid_solana_mint(address):
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
                        "address": str(address),
                        "symbol": str(item.get("symbol") or ""),
                        "name": str(item.get("name") or item.get("symbol") or address),
                        "liquidity": liquidity,
                        "volume": volume,
                        "price": price,
                        "price_change": change,
                        "sources": ["birdeye"],
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
    _cache_set(cache_key, result)
    if not result:
        logger.warning("Token discovery: BirdEye returned no items after filtering.")
    logger.debug(
        "BirdEye fetch complete total=%s cached=%s", len(result), bool(result)
    )
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
) -> List[TokenEntry]:
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
    if _ENABLE_MEMPOOL and rpc_url:
        logger.debug("Discovery mempool threshold=%.3f", mempool_threshold)

    tasks = [bird_task]
    task_labels = ["bird"]
    if mempool_task is not None:
        tasks.append(mempool_task)
        task_labels.append("mempool")

    overall_timeout_raw = os.getenv("DISCOVERY_OVERALL_TIMEOUT", "0")
    try:
        overall_timeout = float(overall_timeout_raw or 0.0)
    except Exception:
        overall_timeout = 0.0

    results: List[Any] = []
    if overall_timeout > 0:
        done, pending = await asyncio.wait(tasks, timeout=overall_timeout)
        if pending:
            logger.warning(
                "Discovery overall timeout after %.2fs; pending_tasks=%s",
                overall_timeout,
                len(pending),
            )
        for task in tasks:
            if task in done:
                try:
                    results.append(task.result())
                except Exception as exc:  # pragma: no cover - defensive
                    results.append(exc)
            else:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                results.append(asyncio.TimeoutError(f"Discovery timed out after {overall_timeout}s"))
    else:
        results = await asyncio.gather(*tasks, return_exceptions=True)

    bird_tokens: List[TokenEntry] = []
    mempool: Dict[str, Dict[str, float]] = {}

    for label, res in zip(task_labels, results):
        if label == "bird":
            if isinstance(res, Exception):
                logger.warning("BirdEye discovery failed: %s", res)
            else:
                bird_tokens = list(res or [])
        elif label == "mempool":
            if isinstance(res, Exception):
                logger.debug("Mempool signals unavailable: %s", res)
            else:
                mempool = dict(res or {})

    candidates: Dict[str, Dict[str, Any]] = {}
    bird_addresses = set()
    for token in bird_tokens:
        addr = token.get("address")
        if not addr:
            continue
        entry = dict(token)
        entry["sources"] = set(token.get("sources") or ["birdeye"])
        candidates[addr] = entry
        bird_addresses.add(addr)

    for addr, mp in mempool.items():
        entry = candidates.setdefault(
            addr,
            {
                "address": str(addr),
                "symbol": str(mp.get("symbol") or ""),
                "name": str(mp.get("name") or addr),
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

        liq_comp = _LIQUIDITY_WEIGHT * _score_component(liquidity)
        vol_comp = _VOLUME_WEIGHT * _score_component(volume)
        mp_comp = 0.0

        mp = mempool.get(addr)
        if mp:
            raw_score = mp.get("score", 0.0)
            try:
                raw = float(raw_score or 0.0)
            except Exception:
                raw = 0.0
            mp_score = max(0.0, min(raw, 1.0))
            mp_comp = _MEMPOOL_BONUS * mp_score

        try:
            change = float(entry.get("price_change", 0.0) or 0.0)
        except Exception:
            change = 0.0
        # price_change soft multiplier = clamp between 0.5 and 1.25
        mult = max(0.5, min(1.25, 1.0 + (change / 100.0) * 0.1))

        base = (liq_comp + vol_comp + mp_comp) * mult

        entry.update(
            {
                "score": base,
                "score_liq": liq_comp,
                "score_vol": vol_comp,
                "score_mp": mp_comp,
                "score_mult": mult,
            }
        )

    ordered = sorted(
        candidates.values(),
        key=lambda c: (c.get("score", 0.0), c.get("address", "")),
        reverse=True,
    )

    final: List[TokenEntry] = []
    for entry in ordered[:limit]:
        entry["sources"] = sorted(entry.get("sources", []))
        final.append(entry)

    top_score = final[0]["score"] if final and "score" in final[0] else None
    logger.debug(
        "Discovery combine summary bird=%s mempool=%s final=%s top_score=%s",
        len(bird_tokens),
        len(mempool),
        len(final),
        f"{top_score:.4f}" if isinstance(top_score, (int, float)) else "n/a",
    )

    return final


def warm_cache(rpc_url: str, *, limit: int | None = None) -> None:
    """Prime the discovery cache synchronously (best-effort)."""
    if not (rpc_url or BIRDEYE_API_KEY):
        return

    limit = limit or min(_MAX_TOKENS, 10)
    mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)

    def _worker() -> None:
        try:
            coro = discover_candidates(
                rpc_url, limit=limit, mempool_threshold=mempool_threshold
            )
            asyncio.run(asyncio.wait_for(coro, timeout=_WARM_TIMEOUT))
        except Exception as exc:
            logger.debug("Discovery warm cache failed: %s", exc)

    thread = threading.Thread(target=_worker, name="discovery-warm", daemon=True)
    thread.start()
