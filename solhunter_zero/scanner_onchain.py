# solhunter_zero/scanner_onchain.py
from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Sequence, Tuple

import aiohttp

import requests
from urllib.parse import urlparse

from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import RPCException
from solders.pubkey import Pubkey

# Backwards-compatibility alias for tests that patch ``PublicKey`` directly.
PublicKey = Pubkey

from solhunter_zero.lru import TTLCache
from .http import STATIC_DNS_MAP, get_session
from .clients.helius_das import get_asset_batch, search_fungible_recent
from .util.mints import clean_candidate_mints

try:  # optional dependency shared with the event bus
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - redis optional
    aioredis = None  # type: ignore

try:  # pragma: no cover - optional observability dependency
    from prometheus_client import Counter, Histogram  # type: ignore
    from prometheus_client.registry import REGISTRY  # type: ignore
except Exception:  # pragma: no cover - prometheus optional
    Counter = None  # type: ignore
    Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore

try:  # optional dependency shared with the event bus
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - redis optional
    aioredis = None  # type: ignore

from .rpc_helpers import (
    extract_signature_entries,
    extract_token_accounts,
    extract_program_accounts,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & caches
# ---------------------------------------------------------------------------

# SPL Token Program (SPL-Token v2)
TOKEN_PROGRAM_ID: Pubkey = Pubkey.from_string(
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

# Module-level caches for frequently polled metrics
_METRIC_TTL = float(os.getenv("ONCHAIN_METRIC_TTL", "30") or 30.0)
METRIC_CACHE_TTL = _METRIC_TTL  # Backwards compatibility for tests/utilities
MEMPOOL_RATE_CACHE: TTLCache[Tuple[str, str], float] = TTLCache(maxsize=512, ttl=_METRIC_TTL)
WHALE_ACTIVITY_CACHE: TTLCache[Tuple[str, str], float] = TTLCache(maxsize=512, ttl=_METRIC_TTL)
AVG_SWAP_SIZE_CACHE: TTLCache[Tuple[str, str], float] = TTLCache(maxsize=512, ttl=_METRIC_TTL)
_METRIC_FAILURE_CACHE: TTLCache[Tuple[str, str], bool] = TTLCache(maxsize=2048, ttl=120.0)

_SKIP_METRICS = os.getenv("ONCHAIN_METRIC_SKIP", "").lower() in {"1", "true", "yes", "on"}

# Backoff window when a provider (currently Helius) rejects heavy GPA scans. Using a TTL cache
# keeps the behaviour stateless between runs but prevents hammering the RPC on every discovery
# cycle when we already know the request will be refused.
_HEAVY_SCAN_BACKOFF_TTL = float(os.getenv("PROGRAM_SCAN_BACKOFF", "600") or 600.0)
_HEAVY_SCAN_BACKOFF: TTLCache[str, bool] = TTLCache(
    maxsize=16,
    ttl=_HEAVY_SCAN_BACKOFF_TTL,
)

# History buffer used for optional forecasting in fetch_mempool_tx_rate
MEMPOOL_FEATURE_HISTORY: Dict[Tuple[str, str], list[list[float]]] = {}

# Cap for safety when doing a raw program scan (can be heavy on public RPC)
MAX_PROGRAM_SCAN_ACCOUNTS = int(os.getenv("MAX_PROGRAM_SCAN_ACCOUNTS", "2000") or 2000)

# Token accounts (SPL Token program) are 165 bytes. We perform a secondary scan using this
# size to retain the historical "token account" discovery path while still filtering the
# dataset aggressively enough to keep the RPC payload bounded.
TOKEN_ACCOUNT_DATA_SIZE = int(os.getenv("TOKEN_ACCOUNT_DATA_SIZE", "165") or 165)

# Mint accounts in the SPL Token program have a fixed data size of 82 bytes. Fetching only
# accounts of this size dramatically reduces the volume of data returned by
# ``getProgramAccounts`` and makes the results relevant for discovery (previously the scan was
# dominated by regular token accounts, most of which were long-lived and unrelated to recent
# launches).
MINT_ACCOUNT_DATA_SIZE = int(os.getenv("MINT_ACCOUNT_DATA_SIZE", "82") or 82)
_MINT_ACCOUNT_FILTERS: list[dict[str, int]] = [
    {"dataSize": MINT_ACCOUNT_DATA_SIZE},
]
_TOKEN_ACCOUNT_FILTERS: list[dict[str, int]] = [
    {"dataSize": TOKEN_ACCOUNT_DATA_SIZE},
]

# Helius-specific pagination settings. Requests default to 1k accounts per page which keeps
# memory bounded while avoiding the "Too many accounts requested" RPC errors.
_HELIUS_GPA_PAGE_LIMIT = int(os.getenv("HELIUS_GPA_PAGE_LIMIT", "1000") or 1000)
_HELIUS_GPA_TIMEOUT = float(os.getenv("HELIUS_GPA_TIMEOUT", "20") or 20.0)

_FAST_PIPELINE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
_ONCHAIN_SCAN_OVERFETCH = float(
    os.getenv(
        "ONCHAIN_SCAN_OVERFETCH",
        "3.0" if _FAST_PIPELINE else "5.0",
    )
    or ("3.0" if _FAST_PIPELINE else "5.0")
)
_ONCHAIN_SCAN_MIN_ACCOUNTS = int(
    os.getenv(
        "ONCHAIN_SCAN_MIN_ACCOUNTS",
        "80" if _FAST_PIPELINE else "160",
    )
    or ("80" if _FAST_PIPELINE else "160")
)

# DAS discovery knobs (with defensive defaults)
_DISCOVERY_OVERFETCH_MULT = float(os.getenv("DISCOVERY_OVERFETCH_MULT", "1.5") or 1.5)
_DISCOVERY_RECENT_TTL = int(os.getenv("DISCOVERY_RECENT_TTL_SEC", "86400") or 86400)
_DISCOVERY_REDIS_URL = (
    os.getenv("DISCOVERY_STATE_URL")
    or os.getenv("DISCOVERY_REDIS_URL")
    or os.getenv("DISCOVERY_STATE_REDIS")
    or ""
)
_DISCOVERY_REQUESTS_PER_MIN = int(os.getenv("DISCOVERY_REQUESTS_PER_MIN", "120") or 120)
_DAS_SHADOW_ONLY = (os.getenv("DAS_SHADOW_ONLY") or "").strip().lower() in {"1", "true", "yes", "on"}
_DAS_MUST = (os.getenv("DAS_MUST") or "").strip().lower() in {"1", "true", "yes", "on"}
_DAS_META_ENABLED = (os.getenv("DAS_INCLUDE_META") or "1").strip().lower() in {"1", "true", "yes", "on"}
_DISCOVERY_CIRCUIT_FAILS = int(os.getenv("DISCOVERY_CIRCUIT_FAILS", "5") or 5)
_DISCOVERY_CIRCUIT_WINDOW = float(os.getenv("DISCOVERY_CIRCUIT_WINDOW_SEC", "30") or 30.0)
_DISCOVERY_CIRCUIT_COOLDOWN = float(os.getenv("DISCOVERY_CIRCUIT_COOLDOWN_SEC", "60") or 60.0)

_DISCOVERY_BANNED_DEFAULT = {
    "11111111111111111111111111111111",  # System program
    "ComputeBudget111111111111111111111111111111",  # Compute budget
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token program
    "JUP2jxtut9t7d1xJzJ9YGVtNrXDp8e1koXnRrAbP5pG",  # Jupiter router (v4/v5)
    "PhoeNiXzAbp3n5DPNoqzG7pKqYG1ChZ9EDZb2FPZdG2",  # Phoenix
    "whirLbMiZJfTZNFJ6h9Eeq1Cdm42Hcps225y7sY9Mu1",  # Orca whirlpools
}

_DISCOVERY_BANNED_EXTRA = {
    mint.strip(): True
    for mint in (os.getenv("DISCOVERY_BANNED_MINTS") or "").split(",")
    if mint.strip()
}


def _get_metric(factory, name: str, documentation: str, labelnames: Sequence[str]):
    if factory is None:
        return None
    try:
        return factory(name, documentation, labelnames=tuple(labelnames))
    except ValueError:
        if REGISTRY is not None:
            return REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
    except Exception:
        return None
    return None


_METRIC_DAS_REQUESTS = _get_metric(
    Counter,
    "discovery_das_requests_total",
    "Number of DAS requests issued",
    ("op",),
)
_METRIC_DAS_ERRORS = _get_metric(
    Counter,
    "discovery_das_errors_total",
    "Number of DAS request errors",
    ("op", "code"),
)
_METRIC_DAS_LATENCY = _get_metric(
    Histogram,
    "discovery_das_latency_ms",
    "Latency of DAS operations in milliseconds",
    ("op",),
)
_METRIC_CANDIDATES = _get_metric(
    Counter,
    "discovery_candidates_total",
    "Candidates produced by discovery",
    ("verified",),
)
_METRIC_DEDUPE = _get_metric(
    Counter,
    "discovery_dedupe_skips_total",
    "Candidates skipped because of deduper",
    tuple(),
)
_METRIC_CURSOR_RESUME = _get_metric(
    Counter,
    "discovery_cursor_resumes_total",
    "Number of DAS cursor resumes",
    tuple(),
)


def _metric_counter_inc(counter, labels: Optional[Dict[str, Any]] = None, amount: float = 1.0) -> None:
    if counter is None:
        return
    try:
        if labels:
            counter.labels(**labels).inc(amount)  # type: ignore[call-arg]
        else:
            counter.inc(amount)  # type: ignore[call-arg]
    except Exception:  # pragma: no cover - metrics backend optional
        return


def _metric_hist_observe(hist, value: float, labels: Optional[Dict[str, Any]] = None) -> None:
    if hist is None:
        return
    try:
        if labels:
            hist.labels(**labels).observe(value)  # type: ignore[call-arg]
        else:
            hist.observe(value)  # type: ignore[call-arg]
    except Exception:  # pragma: no cover - metrics backend optional
        return


_SEEN_LOCAL_CACHE: TTLCache[str, float] = TTLCache(maxsize=8192, ttl=float(_DISCOVERY_RECENT_TTL))
_DISCOVERY_STATE_MEMORY: MutableMapping[str, Tuple[str, float | None]] = {}
_DISCOVERY_STATE_LOCK = asyncio.Lock()
_DISCOVERY_REDIS: Any | None = None
_DISCOVERY_REDIS_ATTEMPTED = False


async def _get_redis_client() -> Any | None:
    global _DISCOVERY_REDIS, _DISCOVERY_REDIS_ATTEMPTED
    if _DISCOVERY_REDIS is not None:
        return _DISCOVERY_REDIS
    if _DISCOVERY_REDIS_ATTEMPTED:
        return None
    _DISCOVERY_REDIS_ATTEMPTED = True
    if not _DISCOVERY_REDIS_URL or aioredis is None:
        return None
    try:
        _DISCOVERY_REDIS = aioredis.from_url(_DISCOVERY_REDIS_URL)
    except Exception as exc:  # pragma: no cover - connection failure
        logger.warning("Discovery Redis connection failed: %s", exc)
        _DISCOVERY_REDIS = None
    return _DISCOVERY_REDIS


async def _state_get(key: str) -> Optional[str]:
    redis = await _get_redis_client()
    if redis is not None:
        try:
            data = await redis.get(key)
        except Exception as exc:  # pragma: no cover - redis failure
            logger.debug("Redis get failed for %s: %s", key, exc)
        else:
            if data is None:
                return None
            if isinstance(data, bytes):
                return data.decode()
            return str(data)
    async with _DISCOVERY_STATE_LOCK:
        stored = _DISCOVERY_STATE_MEMORY.get(key)
        if not stored:
            return None
        value, expires = stored
        if expires is not None and expires < time.monotonic():
            _DISCOVERY_STATE_MEMORY.pop(key, None)
            return None
        return value


async def _state_set(key: str, value: Any, *, ttl: int | float | None = None) -> None:
    redis = await _get_redis_client()
    str_value = "" if value is None else str(value)
    if redis is not None:
        try:
            await redis.set(key, str_value, ex=float(ttl) if ttl else None)
            return
        except Exception as exc:  # pragma: no cover - redis failure
            logger.debug("Redis set failed for %s: %s", key, exc)
    async with _DISCOVERY_STATE_LOCK:
        expiry = (time.monotonic() + float(ttl)) if ttl else None
        _DISCOVERY_STATE_MEMORY[key] = (str_value, expiry)


async def _state_setnx(key: str, value: Any, *, ttl: int | float) -> bool:
    redis = await _get_redis_client()
    str_value = "" if value is None else str(value)
    if redis is not None:
        try:
            return bool(await redis.set(key, str_value, ex=float(ttl), nx=True))
        except Exception as exc:  # pragma: no cover - redis failure
            logger.debug("Redis setnx failed for %s: %s", key, exc)
    async with _DISCOVERY_STATE_LOCK:
        stored = _DISCOVERY_STATE_MEMORY.get(key)
        now = time.monotonic()
        if stored:
            _, expires = stored
            if expires is None or expires > now:
                return False
        _DISCOVERY_STATE_MEMORY[key] = (str_value, now + float(ttl))
        return True


async def _state_incr(key: str) -> int:
    redis = await _get_redis_client()
    if redis is not None:
        try:
            return int(await redis.incr(key))
        except Exception as exc:  # pragma: no cover - redis failure
            logger.debug("Redis incr failed for %s: %s", key, exc)
    async with _DISCOVERY_STATE_LOCK:
        value, expires = _DISCOVERY_STATE_MEMORY.get(key, ("0", None))
        try:
            current = int(value)
        except Exception:
            current = 0
        current += 1
        _DISCOVERY_STATE_MEMORY[key] = (str(current), expires)
        return current


async def _state_delete(key: str) -> None:
    redis = await _get_redis_client()
    if redis is not None:
        try:
            await redis.delete(key)
        except Exception as exc:  # pragma: no cover - redis failure
            logger.debug("Redis delete failed for %s: %s", key, exc)
    async with _DISCOVERY_STATE_LOCK:
        _DISCOVERY_STATE_MEMORY.pop(key, None)


@dataclass
class _DiscoveryHealth:
    last_success_ts: Optional[float] = None
    last_error: Optional[str] = None
    last_error_ts: Optional[float] = None
    last_cursor: Optional[str] = None
    breaker_open_until: float = 0.0
    consecutive_failures: int = 0

    def as_dict(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "last_success_ts": self.last_success_ts,
            "last_error": self.last_error,
            "last_error_ts": self.last_error_ts,
            "cursor": self.last_cursor,
            "breaker_open": bool(self.breaker_open_until and self.breaker_open_until > now),
            "breaker_open_until": self.breaker_open_until if self.breaker_open_until else None,
            "consecutive_failures": self.consecutive_failures,
        }


_DISCOVERY_HEALTH = _DiscoveryHealth()


class _DiscoveryBudget:
    def __init__(self, per_minute: int) -> None:
        self.per_minute = max(1, per_minute)
        self.capacity = float(self.per_minute)
        self.tokens = float(self.per_minute)
        self.rate = self.capacity / 60.0
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self, cost: float = 1.0) -> None:
        async with self.lock:
            while True:
                now = time.monotonic()
                elapsed = max(0.0, now - self.last)
                if elapsed > 0:
                    self.tokens = min(
                        self.capacity,
                        self.tokens + elapsed * self.rate,
                    )
                    self.last = now
                if self.tokens >= cost:
                    self.tokens -= cost
                    return
                needed = (cost - self.tokens) / self.rate if self.rate > 0 else 1.0
                await asyncio.sleep(min(max(needed, 0.05), 2.0))


_DISCOVERY_BUDGET = _DiscoveryBudget(_DISCOVERY_REQUESTS_PER_MIN)


async def _discovery_mark_seen(mint: str) -> bool:
    if _SEEN_LOCAL_CACHE.get(mint) is not None:
        _metric_counter_inc(_METRIC_DEDUPE)
        return False
    ttl = max(60, _DISCOVERY_RECENT_TTL)
    fresh = await _state_setnx(f"discovery:seen:{mint}", "1", ttl=ttl)
    _SEEN_LOCAL_CACHE.set(mint, time.monotonic())
    if not fresh:
        _metric_counter_inc(_METRIC_DEDUPE)
    return fresh


async def _load_discovery_cursor() -> Optional[str]:
    cursor = await _state_get("discovery:cursor")
    if cursor:
        _metric_counter_inc(_METRIC_CURSOR_RESUME)
    if cursor:
        cursor = cursor.strip()
    return cursor or None


async def _persist_discovery_cursor(cursor: Optional[str]) -> None:
    if cursor:
        await _state_set("discovery:cursor", cursor, ttl=_DISCOVERY_RECENT_TTL)
    else:
        await _state_delete("discovery:cursor")


async def _load_breaker_until() -> float:
    now = time.time()
    if _DISCOVERY_HEALTH.breaker_open_until and _DISCOVERY_HEALTH.breaker_open_until > now:
        return _DISCOVERY_HEALTH.breaker_open_until
    raw = await _state_get("discovery:breaker_until")
    if not raw:
        return 0.0
    try:
        until = float(raw)
    except Exception:
        until = 0.0
    _DISCOVERY_HEALTH.breaker_open_until = until
    return until


async def _reset_discovery_failures() -> None:
    _DISCOVERY_HEALTH.consecutive_failures = 0
    await _state_set("discovery:fails", 0, ttl=_DISCOVERY_CIRCUIT_WINDOW)
    await _state_delete("discovery:fail_start")


async def _record_discovery_failure(message: str, status_code: str | None = None) -> None:
    now = time.time()
    _DISCOVERY_HEALTH.last_error = message
    _DISCOVERY_HEALTH.last_error_ts = now
    try:
        start_raw = await _state_get("discovery:fail_start")
        if not start_raw:
            await _state_set("discovery:fail_start", now, ttl=_DISCOVERY_CIRCUIT_WINDOW)
            failures = 1
        else:
            try:
                start_ts = float(start_raw)
            except Exception:
                start_ts = now
            if now - start_ts > _DISCOVERY_CIRCUIT_WINDOW:
                await _state_set("discovery:fail_start", now, ttl=_DISCOVERY_CIRCUIT_WINDOW)
                failures = 1
            else:
                raw_fails = await _state_get("discovery:fails") or "0"
                try:
                    failures = int(raw_fails) + 1
                except Exception:
                    failures = 1
        await _state_set("discovery:fails", failures, ttl=_DISCOVERY_CIRCUIT_WINDOW)
        _DISCOVERY_HEALTH.consecutive_failures = failures
        if failures >= _DISCOVERY_CIRCUIT_FAILS:
            until = now + _DISCOVERY_CIRCUIT_COOLDOWN
            _DISCOVERY_HEALTH.breaker_open_until = until
            await _state_set("discovery:breaker_until", until, ttl=_DISCOVERY_CIRCUIT_COOLDOWN + 5)
            await _state_set("discovery:fails", 0, ttl=_DISCOVERY_CIRCUIT_COOLDOWN)
            await _state_delete("discovery:fail_start")
            _DISCOVERY_HEALTH.consecutive_failures = 0
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Failed to record discovery failure: %s", exc)
    labels = {"op": "search"}
    if status_code:
        labels["code"] = status_code
    _metric_counter_inc(_METRIC_DAS_ERRORS, labels=labels)


async def _record_discovery_success(cursor: Optional[str]) -> None:
    now = time.time()
    _DISCOVERY_HEALTH.last_success_ts = now
    _DISCOVERY_HEALTH.last_cursor = cursor
    _DISCOVERY_HEALTH.last_error = None
    _DISCOVERY_HEALTH.last_error_ts = None
    await _reset_discovery_failures()


# ---------------------------------------------------------------------------
# Small helpers: safe numeric coercions (squelch “an integer is required”)
# ---------------------------------------------------------------------------

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            s = x.strip()
            if s.lower() in {"", "nan", "none", "null"}:
                return default
            return float(s)
    except Exception:
        pass
    return default


def _is_rate_limited(exc: Exception) -> bool:
    message = str(exc).lower()
    rate_markers = (
        "too many requests",
        "rate limit",
        "usage limit exceeded",
        "429",
        "compute units usage limit exceeded",
    )
    return any(marker in message for marker in rate_markers)


def _log_metric_failure(kind: str, token: str, exc: Exception) -> None:
    cache_key = (kind, token)
    message = str(exc) or repr(exc)
    already_logged = cache_key in _METRIC_FAILURE_CACHE
    _METRIC_FAILURE_CACHE.set(cache_key, True)
    if _is_rate_limited(exc) or already_logged:
        logger.debug("Metric fetch (%s) failed for %s: %s", kind, token, message)
    else:
        logger.warning("Metric fetch (%s) failed for %s: %s", kind, token, message)


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return int(x)
        if isinstance(x, str):
            s = x.strip()
            if s.lower() in {"", "nan", "none", "null"}:
                return default
            return int(float(s))
    except Exception:
        pass
    return default


def _parse_int_sequence(raw: str | None, default: Sequence[int]) -> tuple[int, ...]:
    """Parse a comma-delimited list of ints from ``raw``.

    Keeps ordering, ignores invalid/empty entries, and falls back to ``default``
    when nothing valid is provided.
    """

    values: list[int] = []
    for part in (raw or "").split(","):
        chunk = part.strip()
        if not chunk:
            continue
        try:
            num = int(chunk)
        except Exception:
            continue
        if num > 0 and num not in values:
            values.append(num)
    if values:
        return tuple(values)
    return tuple(default)


_DEFAULT_SIGNATURE_LIMITS: tuple[int, ...] = (250, 120, 60, 30, 15, 8)
_SIGNATURE_SCAN_LIMITS: tuple[int, ...] = _parse_int_sequence(
    os.getenv("SIGNATURE_SCAN_LIMITS"), _DEFAULT_SIGNATURE_LIMITS
)

_RETRYABLE_SIGNATURE_ERROR_SNIPPETS: tuple[str, ...] = (
    "429",
    "rate limit",
    "too many",
    "too many requests",
    "heavy",
    "improve",
    "still failing",
    "timeout",
    "timed out",
    "deadline",
    "gateway",
    "overloaded",
    "busy",
    "temporarily unavailable",
)


def _signature_attempt_limits(initial: int | None = None) -> tuple[int, ...]:
    """Return the list of limits to try when fetching signatures."""

    limits = list(_SIGNATURE_SCAN_LIMITS)
    if initial is not None:
        try:
            init = int(initial)
        except Exception:
            init = 0
        if init > 0:
            if init in limits:
                limits.remove(init)
            limits.insert(0, init)
    return tuple(limits)


def _extract_error_message(exc: Exception) -> str:
    """Human-friendly error message from ``exc`` for logging/retry decisions."""

    if isinstance(exc, RPCException):
        payload = exc.args[0] if exc.args else None
        if isinstance(payload, dict):
            message = payload.get("message")
            if not message:
                data = payload.get("data")
                if isinstance(data, dict):
                    message = data.get("message") or data.get("error")
            if message:
                return str(message)
        return str(exc)
    return str(exc)


def _is_retryable_signature_error(exc: Exception) -> bool:
    if isinstance(exc, requests.RequestException):
        return True
    message = _extract_error_message(exc).lower()
    return any(snippet in message for snippet in _RETRYABLE_SIGNATURE_ERROR_SNIPPETS)


async def _get_signatures_for_address_async(
    client: AsyncClient,
    pubkey: Pubkey,
    *,
    limits: Sequence[int] | None = None,
):
    """Fetch signatures with graceful backoff on provider errors."""

    attempt_limits = tuple(limits or _SIGNATURE_SCAN_LIMITS)
    last_exc: Exception | None = None
    for idx, limit in enumerate(attempt_limits):
        try:
            return await client.get_signatures_for_address(pubkey, limit=limit)
        except Exception as exc:  # pragma: no cover - network variability
            last_exc = exc
            should_retry = _is_retryable_signature_error(exc)
            if should_retry and idx < len(attempt_limits) - 1:
                next_limit = attempt_limits[idx + 1]
                logger.debug(
                    "getSignaturesForAddress(%s) failed with limit=%s: %s; retrying with %s",
                    pubkey,
                    limit,
                    _extract_error_message(exc),
                    next_limit,
                )
                await asyncio.sleep(0)
                continue
            raise
    if last_exc is not None:
        raise last_exc


def _get_signatures_for_address(
    client: Client,
    pubkey: Pubkey,
    *,
    limits: Sequence[int] | None = None,
):
    """Sync variant of ``_get_signatures_for_address_async``."""

    attempt_limits = tuple(limits or _SIGNATURE_SCAN_LIMITS)
    last_exc: Exception | None = None
    for idx, limit in enumerate(attempt_limits):
        try:
            return client.get_signatures_for_address(pubkey, limit=limit)
        except Exception as exc:  # pragma: no cover - network variability
            last_exc = exc
            should_retry = _is_retryable_signature_error(exc)
            if should_retry and idx < len(attempt_limits) - 1:
                next_limit = attempt_limits[idx + 1]
                logger.debug(
                    "getSignaturesForAddress(%s) failed with limit=%s: %s; retrying with %s",
                    pubkey,
                    limit,
                    _extract_error_message(exc),
                    next_limit,
                )
                continue
            raise
    if last_exc is not None:
        raise last_exc
def _to_pubkey(token: str | Pubkey) -> Pubkey:
    if isinstance(token, Pubkey):
        return token
    if isinstance(token, bytes):
        token = token.decode("utf-8", "ignore")
    text = str(token).strip()
    if not text:
        raise ValueError("token is required")
    result = PublicKey(text)
    if isinstance(result, Pubkey):
        return result
    return result  # type: ignore[return-value]


def _is_helius_url(rpc_url: str) -> bool:
    return "helius" in (rpc_url or "").lower()


def _should_backoff_heavy_scan(rpc_url: str) -> bool:
    if not _is_helius_url(rpc_url):
        return False
    if _HEAVY_SCAN_BACKOFF.get(rpc_url):
        logger.debug(
            "Skipping getProgramAccounts on %s due to recent provider backoff", rpc_url
        )
        return True
    return False


def _is_provider_plan_error(exc: Exception) -> bool:
    message = str(exc).lower()
    plan_markers = (
        "compute units usage limit exceeded",
        "compute units limit exceeded",
        "plan requires upgrade",
        "access denied",
        "forbidden",
        "not permitted",
        "429",
        "too many requests",
        "heavy request",
    )
    return any(marker in message for marker in plan_markers)


async def _perform_program_scan(
    rpc_url: str,
    *,
    filters: Iterable[Dict[str, Any]] | None,
    data_size: int | None,
    limit: int,
) -> tuple[Dict[str, Any] | None, bool]:
    """Run a capped ``getProgramAccounts`` request with defensive error handling."""

    delays = [0.0, 1.0, 2.0]
    last_exc: Exception | None = None

    for attempt in range(len(delays)):
        try:
            if _is_helius_url(rpc_url):
                resp = await asyncio.to_thread(
                    _fetch_program_accounts_v2_helius,
                    rpc_url,
                    TOKEN_PROGRAM_ID,
                    limit,
                    filters=filters,
                )
                _HEAVY_SCAN_BACKOFF.pop(rpc_url, None)
            else:
                client = Client(rpc_url)
                kwargs: Dict[str, Any] = {"encoding": "jsonParsed"}
                if data_size is not None:
                    kwargs["data_size"] = data_size
                if filters:
                    kwargs["filters"] = list(filters)
                resp = await asyncio.to_thread(
                    client.get_program_accounts,
                    TOKEN_PROGRAM_ID,
                    **kwargs,
                )
            return resp, False
        except Exception as exc:
            if _is_helius_url(rpc_url) and _is_provider_plan_error(exc):
                _HEAVY_SCAN_BACKOFF.set(rpc_url, True)
                logger.warning(
                    "Helius rejected getProgramAccounts (%s); backing off for %.0fs",
                    exc,
                    _HEAVY_SCAN_BACKOFF_TTL,
                )
                return None, True
            last_exc = exc
            if attempt < len(delays) - 1:
                await asyncio.sleep(delays[attempt + 1])

    if last_exc is not None:
        logger.warning("Program scan failed: %s", last_exc)
    return None, False


def _collect_mint_candidates(resp: Dict[str, Any] | None, limit: int) -> list[str]:
    if not resp or limit <= 0:
        return []

    mints: list[str] = []
    for acc in extract_program_accounts(resp):
        if len(mints) >= limit:
            break
        account = acc.get("account", {})
        data = account.get("data", {}) if isinstance(account, dict) else {}
        parsed = data.get("parsed", {}) if isinstance(data, dict) else {}
        parsed_type = parsed.get("type") if isinstance(parsed, dict) else None
        candidate: str | None = None
        info = parsed.get("info", {}) if isinstance(parsed, dict) else {}
        is_initialized = bool(info.get("isInitialized", True))
        supply_ok = True
        supply = info.get("supply")
        if supply is not None:
            try:
                supply_ok = int(str(supply)) > 0
            except (ValueError, TypeError):
                supply_ok = False

        if (
            isinstance(parsed_type, str)
            and parsed_type.lower() == "mint"
            and isinstance(acc.get("pubkey"), str)
            and is_initialized
            and supply_ok
        ):
            candidate = acc.get("pubkey")
        else:
            info = parsed.get("info", {}) if isinstance(parsed, dict) else {}
            mint = info.get("mint") if isinstance(info, dict) else None
            if isinstance(mint, str):
                candidate = mint

        if isinstance(candidate, str) and len(candidate) >= 32:
            mints.append(candidate)
            if len(mints) >= limit:
                break

    return mints


def _account_cap_for(target_tokens: int | None) -> int:
    """Return a bounded account cap based on requested token count."""

    if target_tokens is None or target_tokens <= 0:
        return MAX_PROGRAM_SCAN_ACCOUNTS

    try:
        overfetch = max(1.0, float(_ONCHAIN_SCAN_OVERFETCH))
    except Exception:
        overfetch = 3.0 if _FAST_PIPELINE else 5.0

    try:
        minimum = max(1, int(_ONCHAIN_SCAN_MIN_ACCOUNTS))
    except Exception:
        minimum = 80 if _FAST_PIPELINE else 160

    desired = max(minimum, int(target_tokens * overfetch))
    return min(MAX_PROGRAM_SCAN_ACCOUNTS, desired)


def _ingest_candidates(
    existing: list[str],
    candidates: Iterable[str],
    *,
    max_tokens: int | None,
) -> bool:
    """Extend ``existing`` with new unique candidates until ``max_tokens`` is reached."""

    seen = set(existing)
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        existing.append(candidate)
        if max_tokens is not None and max_tokens > 0 and len(existing) >= max_tokens:
            return True
    return False


# ---------------------------------------------------------------------------
# Helius DAS adapter (cursor-driven fungible discovery)
# ---------------------------------------------------------------------------

def _das_enabled() -> bool:
    provider = (os.getenv("ONCHAIN_DISCOVERY_PROVIDER") or "").strip().lower()
    if provider in {"das", "helius_das"}:
        return True
    if provider in {"gpa", "legacy", "rpc"}:
        return False
    flag = (os.getenv("ONCHAIN_USE_DAS") or "").strip().lower()
    if flag in {"1", "true", "yes", "on"}:
        return True
    if flag in {"0", "false", "no", "off"}:
        return False
    return True


def _das_page_limit(target: int) -> int:
    raw = os.getenv("ONCHAIN_DAS_PAGE_LIMIT") or os.getenv("DAS_DISCOVERY_LIMIT")
    try:
        limit = int(raw) if raw not in {None, ""} else target
    except Exception:
        limit = target
    limit = max(1, limit)
    if target > 0:
        limit = min(limit, target)
    return limit


def _das_metadata_limit() -> int:
    raw = os.getenv("ONCHAIN_DAS_METADATA_LIMIT") or os.getenv("DAS_METADATA_LIMIT")
    if not raw:
        return 128
    try:
        limit = int(raw)
    except Exception:
        return 128
    return max(0, limit)


def _is_banned_mint(value: str) -> bool:
    if value in _DISCOVERY_BANNED_DEFAULT:
        return True
    if value in _DISCOVERY_BANNED_EXTRA:
        return True
    return False


def _normalise_mint(mint: Any) -> str | None:
    if not isinstance(mint, str):
        return None
    candidate = mint.strip()
    if not candidate or len(candidate) < 32 or len(candidate) > 44:
        return None
    if _is_banned_mint(candidate):
        return None
    base58_chars = {
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "J",
        "K",
        "L",
        "M",
        "N",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
        "a",
        "b",
        "c",
        "d",
        "e",
        "f",
        "g",
        "h",
        "i",
        "j",
        "k",
        "m",
        "n",
        "o",
        "p",
        "q",
        "r",
        "s",
        "t",
        "u",
        "v",
        "w",
        "x",
        "y",
        "z",
    }
    if any(ch not in base58_chars for ch in candidate):
        return None
    return candidate


def _maybe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        chunk = value.strip()
        if not chunk:
            return None
        try:
            return int(float(chunk))
        except Exception:
            return None
    return None


def _collect_venue_hints(asset: Dict[str, Any]) -> List[str]:
    venues: set[str] = set()
    grouping = asset.get("grouping") or asset.get("groups") or asset.get("collection")
    if isinstance(grouping, (list, tuple)):
        for entry in grouping:
            if not isinstance(entry, dict):
                continue
            key = (
                str(
                    entry.get("groupKey")
                    or entry.get("group_key")
                    or entry.get("collection")
                    or entry.get("name")
                    or ""
                )
            ).lower()
            if "raydium" in key:
                venues.add("raydium")
            if "orca" in key:
                venues.add("orca")
            if "phoenix" in key:
                venues.add("phoenix")
    creators = asset.get("creators")
    if isinstance(creators, list):
        for entry in creators:
            if isinstance(entry, dict):
                addr = str(entry.get("address") or "").lower()
                if "raydium" in addr:
                    venues.add("raydium")
                if "orca" in addr:
                    venues.add("orca")
                if "phoenix" in addr:
                    venues.add("phoenix")
    pool_addr = asset.get("pool") or asset.get("poolAddress")
    if isinstance(pool_addr, str):
        lower = pool_addr.lower()
        if "orca" in lower:
            venues.add("orca")
        if "raydium" in lower:
            venues.add("raydium")
        if "phoenix" in lower:
            venues.add("phoenix")
    if not venues:
        return []
    return sorted(venues)


def _extract_asset_details(asset: Dict[str, Any]) -> Dict[str, Any]:
    details: Dict[str, Any] = {}
    token_info = asset.get("token_info") or asset.get("tokenInfo") or {}
    fungible_info = asset.get("fungible_token_info") or asset.get("fungibleTokenInfo") or {}
    symbol = token_info.get("symbol") or fungible_info.get("symbol")
    if isinstance(symbol, str) and symbol.strip():
        details["symbol"] = symbol.strip()
    decimals = _maybe_int(token_info.get("decimals"))
    if decimals is None:
        decimals = _maybe_int(fungible_info.get("decimals"))
    if decimals is not None:
        details["decimals"] = decimals
    token_program = (
        token_info.get("tokenProgram")
        or fungible_info.get("tokenProgram")
        or asset.get("token_program")
        or asset.get("tokenProgram")
    )
    if isinstance(token_program, str) and token_program.strip():
        details["token_program"] = token_program.strip()
        details["is_token_2022"] = token_program.strip().lower().startswith("tokenz")
    verified = token_info.get("verified")
    if isinstance(verified, bool):
        details["verified"] = verified
    venues = _collect_venue_hints(asset)
    if venues:
        details["venues"] = venues
    return details


def _extract_asset_mint(asset: Dict[str, Any]) -> str | None:
    if not isinstance(asset, dict):
        return None

    compression = asset.get("compression")
    if isinstance(compression, dict) and compression.get("compressed"):
        # Skip compressed assets; fungible tokens should be uncompressed.
        return None

    for key in ("id", "mint", "tokenAddress", "address"):
        value = asset.get(key)
        mint = _normalise_mint(value)
        if mint:
            return mint

    token_info = asset.get("token_info") or asset.get("tokenInfo")
    if isinstance(token_info, dict):
        for key in ("mint", "address", "tokenAddress"):
            value = token_info.get(key)
            mint = _normalise_mint(value)
            if mint:
                return mint

    content = asset.get("content")
    if isinstance(content, dict):
        metadata = content.get("metadata")
        if isinstance(metadata, dict):
            value = metadata.get("mint") or metadata.get("address")
            mint = _normalise_mint(value)
            if mint:
                return mint

    return None


def _merge_metadata(entry: Dict[str, Any], metadata: Dict[str, Any] | None) -> Dict[str, Any]:
    if not metadata:
        return entry

    meta_payload: Dict[str, Any] | None = None
    if _DAS_META_ENABLED:
        current_meta = entry.get("meta")
        if isinstance(current_meta, dict):
            meta_payload = dict(current_meta)
        else:
            meta_payload = {}

    for key, value in metadata.items():
        if key == "sources":
            base_sources = set(entry.get("sources") or set())
            if isinstance(value, (set, list, tuple)):
                base_sources.update(value)
            elif isinstance(value, str):
                base_sources.add(value)
            entry["sources"] = base_sources
            continue
        if key in {"address", "liquidity", "volume"}:
            continue
        if key in {"symbol", "decimals", "token_program", "is_token_2022", "venues", "verified"}:
            entry[key] = value
        if meta_payload is not None:
            meta_payload[key] = value
    if meta_payload:
        entry["meta"] = meta_payload
    return entry


async def _fetch_das_metadata(
    session: aiohttp.ClientSession,
    mints: Sequence[str],
) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    limit = _das_metadata_limit()
    if limit <= 0:
        return {}, False

    subset = list(dict.fromkeys(mints[:limit]))
    subset, dropped = clean_candidate_mints(subset)
    if dropped:
        logger.warning(
            "Dropped %d invalid mint(s) before DAS metadata fetch", len(dropped)
        )
    if not subset:
        return {}, False

    try:
        await _DISCOVERY_BUDGET.acquire(cost=len(subset))
        _metric_counter_inc(_METRIC_DAS_REQUESTS, labels={"op": "batch"})
        start = time.monotonic()
        batch = await get_asset_batch(session, subset)
        latency = (time.monotonic() - start) * 1000.0
        _metric_hist_observe(_METRIC_DAS_LATENCY, latency, labels={"op": "batch"})
    except Exception as exc:
        logger.debug("Helius DAS metadata fetch failed: %s", exc)
        _metric_counter_inc(
            _METRIC_DAS_ERRORS,
            labels={"op": "batch", "code": getattr(exc, "status", "error")},
        )
        return {}, False

    metadata: Dict[str, Dict[str, Any]] = {}
    for asset in batch:
        mint = _extract_asset_mint(asset)
        if not mint:
            continue
        details = _extract_asset_details(asset)
        if "verified" not in details:
            details["verified"] = True
        metadata[mint] = details
    return metadata, True


async def _collect_onchain_metrics(
    mints: Sequence[str],
    rpc_url: str,
    *,
    sources: Iterable[str] | None = None,
    metadata: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    base_sources = set(sources or {"onchain"})
    if _SKIP_METRICS:
        results: List[Dict[str, Any]] = []
        for mint in mints:
            entry = {
                "address": mint,
                "liquidity": 0.0,
                "volume": 0.0,
                "sources": set(base_sources),
            }
            entry = _merge_metadata(entry, (metadata or {}).get(mint))
            results.append(entry)
        return results

    try:
        concurrency_env = int(os.getenv("ONCHAIN_METRIC_CONCURRENCY", "0") or 0)
    except ValueError:
        concurrency_env = 0
    default_concurrency = 2 if _FAST_PIPELINE else 3
    concurrency = max(1, concurrency_env or default_concurrency)
    semaphore = asyncio.Semaphore(concurrency)

    async def _task(mint: str) -> Tuple[str, float, float]:
        async with semaphore:
            try:
                liq = await fetch_liquidity_onchain_async(mint, rpc_url)
            except Exception as exc:
                logger.debug("Async liquidity fetch failed for %s: %s", mint, exc)
                liq = await asyncio.to_thread(fetch_liquidity_onchain, mint, rpc_url)
            try:
                vol = await fetch_volume_onchain_async(mint, rpc_url)
            except Exception as exc:
                logger.debug("Async volume fetch failed for %s: %s", mint, exc)
                vol = await asyncio.to_thread(fetch_volume_onchain, mint, rpc_url)
        return mint, _safe_float(liq), _safe_float(vol)

    tasks = [asyncio.create_task(_task(mint)) for mint in mints]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

    results: List[Dict[str, Any]] = []
    for item in raw_results:
        if isinstance(item, Exception):
            logger.debug("On-chain metric task failed: %s", item)
            continue
        mint, liq, vol = item
        entry = {
            "address": mint,
            "liquidity": liq,
            "volume": vol,
            "sources": set(base_sources),
        }
        entry = _merge_metadata(entry, (metadata or {}).get(mint))
        results.append(entry)
    return results


async def _scan_tokens_via_das(
    rpc_url: str,
    *,
    return_metrics: bool,
    max_tokens: int | None,
) -> List[str] | List[Dict[str, Any]]:
    try:
        session = await get_session()
    except Exception as exc:
        logger.warning("Unable to acquire shared HTTP session for DAS discovery: %s", exc)
        return []

    now = time.time()
    breaker_until = await _load_breaker_until()
    if breaker_until and breaker_until > now:
        sleep_for = max(0.0, breaker_until - now)
        logger.warning("DAS discovery breaker open; sleeping %.2fs", sleep_for)
        await asyncio.sleep(min(sleep_for, _DISCOVERY_CIRCUIT_COOLDOWN))
        if not _DAS_MUST:
            return []

    account_cap = _account_cap_for(max_tokens)
    if account_cap <= 0:
        return []
    target_total = max(account_cap, int(math.ceil(account_cap * _DISCOVERY_OVERFETCH_MULT)))
    page_limit = _das_page_limit(target_total)
    cursor = await _load_discovery_cursor()
    previous_cursor: str | None = None
    uniq_mints: list[str] = []
    mint_details: Dict[str, Dict[str, Any]] = {}

    while len(uniq_mints) < target_total:
        remaining = target_total - len(uniq_mints)
        limit = page_limit if page_limit > 0 else remaining
        limit = max(1, min(limit, remaining))
        await _DISCOVERY_BUDGET.acquire()
        _metric_counter_inc(_METRIC_DAS_REQUESTS, labels={"op": "search"})
        start = time.monotonic()
        try:
            items, next_cursor = await search_fungible_recent(
                session,
                cursor=cursor,
                limit=limit,
            )
            latency = (time.monotonic() - start) * 1000.0
            _metric_hist_observe(_METRIC_DAS_LATENCY, latency, labels={"op": "search"})
        except Exception as exc:
            await _record_discovery_failure(str(exc), getattr(exc, "status", None))
            logger.warning("Helius DAS searchAssets failed: %s", exc)
            return []

        if not items:
            cursor = next_cursor
            break

        added = 0
        skipped = 0
        for asset in items:
            mint = _extract_asset_mint(asset)
            if not mint:
                skipped += 1
                continue
            details = _extract_asset_details(asset)
            decimals = details.get("decimals")
            if decimals is not None and decimals > 12:
                skipped += 1
                continue
            fresh = await _discovery_mark_seen(mint)
            if not fresh:
                skipped += 1
                continue
            if mint in mint_details:
                continue
            uniq_mints.append(mint)
            mint_details[mint] = details
            added += 1
            if len(uniq_mints) >= target_total:
                break

        logger.info(
            "DAS discovery page",
            extra={
                "op": "search",
                "cursor": cursor,
                "page_size": len(items),
                "added": added,
                "skipped": skipped,
                "target": target_total,
            },
        )

        cursor = next_cursor
        if not cursor or cursor == previous_cursor:
            break
        previous_cursor = cursor

    await _persist_discovery_cursor(cursor)

    if not uniq_mints:
        return []

    await _record_discovery_success(cursor)

    final_mints = uniq_mints[:account_cap]

    metadata: Dict[str, Dict[str, Any]] = {}
    metadata_ok = False
    if return_metrics or _DAS_META_ENABLED:
        metadata, metadata_ok = await _fetch_das_metadata(session, final_mints)
        if metadata:
            mint_details.update(metadata)
        if not metadata_ok:
            logger.info("DAS metadata unavailable; marking candidates as unverified")

    filtered_mints: list[str] = []
    for mint in final_mints:
        details = mint_details.get(mint, {})
        decimals = details.get("decimals")
        if decimals is not None and decimals > 12:
            continue
        filtered_mints.append(mint)
    final_mints = filtered_mints

    if not return_metrics:
        return final_mints

    verified_count = 0
    unverified_count = 0
    metadata_map: Dict[str, Dict[str, Any]] = {}
    for mint in final_mints:
        base_meta = dict(mint_details.get(mint, {}))
        if metadata_ok and mint in metadata:
            base_meta["verified"] = True
        else:
            base_meta.setdefault("verified", False)
            if not metadata_ok:
                logger.debug("Mint %s marked unverified due to metadata miss", mint)
        if base_meta.get("verified"):
            verified_count += 1
        else:
            unverified_count += 1
        metadata_map[mint] = base_meta

    _metric_counter_inc(_METRIC_CANDIDATES, labels={"verified": "true"}, amount=verified_count)
    _metric_counter_inc(_METRIC_CANDIDATES, labels={"verified": "false"}, amount=unverified_count)

    return await _collect_onchain_metrics(
        final_mints,
        rpc_url,
        sources={"onchain", "helius-das"},
        metadata=metadata_map,
    )


def get_discovery_health() -> Dict[str, Any]:
    """Expose the current DAS discovery health snapshot."""

    return _DISCOVERY_HEALTH.as_dict()


# ---------------------------------------------------------------------------
# Optional raw on-chain *discovery* (conservative, numeric-only, capped)
# ---------------------------------------------------------------------------

async def scan_tokens_onchain(
    rpc_url: str,
    *,
    return_metrics: bool = False,
    max_tokens: int | None = None,
) -> List[str] | List[Dict[str, Any]]:
    """
    Light-weight, *best-effort* discovery of SPL-Token mints using `getProgramAccounts`.
    This is capped and should not be your primary discovery source (BirdEye/WS/mempool
    are preferred). We DO NOT use any name/suffix heuristics here.

    When `return_metrics=True`, basic per-mint liquidity/volume are fetched (still safe).
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    if _das_enabled():
        das_results = await _scan_tokens_via_das(
            rpc_url,
            return_metrics=return_metrics,
            max_tokens=max_tokens,
        )
        if _DAS_SHADOW_ONLY:
            count = len(das_results) if isinstance(das_results, list) else 0
            logger.info("DAS shadow-only run completed", extra={"count": count})
        else:
            if das_results:
                return das_results
            if _DAS_MUST:
                return das_results
            logger.info("DAS discovery returned no tokens; falling back to RPC scan")

    if _should_backoff_heavy_scan(rpc_url):
        return []

    account_cap = _account_cap_for(max_tokens)

    resp_mints, backoff = await _perform_program_scan(
        rpc_url,
        filters=_MINT_ACCOUNT_FILTERS,
        data_size=MINT_ACCOUNT_DATA_SIZE,
        limit=account_cap,
    )
    if backoff:
        return []

    mint_candidates = _collect_mint_candidates(resp_mints, account_cap)

    uniq_mints: list[str] = []
    limit_reached = _ingest_candidates(uniq_mints, mint_candidates, max_tokens=max_tokens)

    if not limit_reached:
        if max_tokens is not None and max_tokens > 0:
            remaining_tokens = max_tokens - len(uniq_mints)
            additional_cap = _account_cap_for(remaining_tokens) if remaining_tokens > 0 else 0
        else:
            additional_cap = account_cap - len(mint_candidates)

        if additional_cap > 0:
            resp_tokens, backoff_tokens = await _perform_program_scan(
                rpc_url,
                filters=_TOKEN_ACCOUNT_FILTERS,
                data_size=TOKEN_ACCOUNT_DATA_SIZE,
                limit=additional_cap,
            )
            if not backoff_tokens:
                token_candidates = _collect_mint_candidates(resp_tokens, additional_cap)
                _ingest_candidates(
                    uniq_mints,
                    token_candidates,
                    max_tokens=max_tokens,
                )

    if max_tokens is not None and max_tokens > 0 and len(uniq_mints) > max_tokens:
        uniq_mints = uniq_mints[:max_tokens]

    if not return_metrics:
        return uniq_mints

    return await _collect_onchain_metrics(uniq_mints, rpc_url)


def _fetch_program_accounts_v2_helius(
    rpc_url: str,
    program_id: Pubkey,
    max_accounts: int,
    *,
    encoding: str = "jsonParsed",
    filters: Iterable[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Fetch program accounts from Helius using getProgramAccountsV2 pagination."""

    per_page = max(1, min(_HELIUS_GPA_PAGE_LIMIT, max_accounts))
    aggregated: list[Dict[str, Any]] = []
    pagination_key: str | None = None
    previous_key: str | None = None

    parsed = urlparse(rpc_url)
    host = (parsed.hostname or "").lower()
    static_ips = STATIC_DNS_MAP.get(host, []) if host else []
    target_url = rpc_url
    request_headers: Dict[str, str] = {}
    if static_ips:
        ip = static_ips[0]
        netloc = ip
        if parsed.port:
            netloc = f"{ip}:{parsed.port}"
        rebuilt = parsed._replace(netloc=netloc)
        target_url = rebuilt.geturl()
        if parsed.hostname:
            request_headers["Host"] = parsed.hostname

    with requests.Session() as session:
        while len(aggregated) < max_accounts:
            remaining = max_accounts - len(aggregated)
            limit = max(1, min(per_page, remaining))
            config: Dict[str, Any] = {"encoding": encoding, "limit": limit}
            if filters:
                config["filters"] = list(filters)
            if pagination_key:
                config["paginationKey"] = pagination_key

            payload = {
                "jsonrpc": "2.0",
                "id": "solhunter-program-scan",
                "method": "getProgramAccountsV2",
                "params": [str(program_id), config],
            }

            try:
                response = session.post(
                    target_url,
                    json=payload,
                    timeout=_HELIUS_GPA_TIMEOUT,
                    headers=request_headers or None,
                )
                response.raise_for_status()
            except requests.RequestException as exc:  # pragma: no cover - network issues
                raise RuntimeError(f"Helius getProgramAccountsV2 failed: {exc}") from exc

            try:
                data = response.json()
            except ValueError as exc:  # pragma: no cover - unexpected provider payload
                raise RuntimeError("Helius getProgramAccountsV2 returned non-JSON response") from exc

            if not isinstance(data, dict):
                raise RuntimeError("Helius getProgramAccountsV2 returned unexpected payload")

            if "error" in data:
                error = data["error"]
                message = None
                if isinstance(error, dict):
                    message = error.get("message") or error.get("data")
                raise RuntimeError(f"Helius getProgramAccountsV2 error: {message or error}")

            result = data.get("result")
            if not isinstance(result, dict):
                break

            accounts = result.get("accounts")
            if not isinstance(accounts, list):
                accounts = []

            for entry in accounts:
                if isinstance(entry, dict):
                    aggregated.append(entry)
                    if len(aggregated) >= max_accounts:
                        break

            next_key = result.get("paginationKey")
            if not isinstance(next_key, str) or not accounts:
                break

            if next_key == previous_key:
                break

            previous_key = pagination_key = next_key

    return {"result": {"value": aggregated}}


def scan_tokens_onchain_sync(
    rpc_url: str,
    *,
    return_metrics: bool = False,
    max_tokens: int | None = None,
) -> List[str] | List[Dict[str, Any]]:
    """Synchronous wrapper."""
    return asyncio.run(
        scan_tokens_onchain(
            rpc_url,
            return_metrics=return_metrics,
            max_tokens=max_tokens,
        )
    )


# ---------------------------------------------------------------------------
# Liquidity / volume metrics (defensive parsing)
# ---------------------------------------------------------------------------

async def fetch_liquidity_onchain_async(token: str, rpc_url: str) -> float:
    """Sum balances from `getTokenLargestAccounts` as a proxy for liquidity."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    async with AsyncClient(rpc_url) as client:
        try:
            resp = await client.get_token_largest_accounts(_to_pubkey(token))
            accounts = extract_token_accounts(resp)
            total = 0.0
            for acc in accounts:
                # Prefer uiAmount, fall back to amount
                val = acc.get("uiAmount", acc.get("amount", 0))
                total += _safe_float(val, 0.0)
            return total
        except Exception as exc:  # pragma: no cover
            _log_metric_failure("liquidity", token, exc)
            return 0.0


def fetch_liquidity_onchain(token: str, rpc_url: str) -> float:
    """Sync wrapper for liquidity."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    client = Client(rpc_url)
    try:
        resp = client.get_token_largest_accounts(_to_pubkey(token))
        accounts = extract_token_accounts(resp)
        total = 0.0
        for acc in accounts:
            val = acc.get("uiAmount", acc.get("amount", 0))
            total += _safe_float(val, 0.0)
        return total
    except Exception as exc:  # pragma: no cover
        _log_metric_failure("liquidity", token, exc)
        return 0.0


async def fetch_volume_onchain_async(token: str, rpc_url: str) -> float:
    """Approximate recent tx volume from signature entries (best-effort)."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    async with AsyncClient(rpc_url) as client:
        try:
            pubkey = _to_pubkey(token)
            resp = await _get_signatures_for_address_async(client, pubkey)
            entries = extract_signature_entries(resp)
            return _tx_volume(entries)
        except Exception as exc:  # pragma: no cover
            _log_metric_failure("volume", token, exc)
            return 0.0


def fetch_volume_onchain(token: str, rpc_url: str) -> float:
    """Sync wrapper for recent tx volume."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    client = Client(rpc_url)
    try:
        pubkey = _to_pubkey(token)
        resp = _get_signatures_for_address(client, pubkey)
        entries = extract_signature_entries(resp)
        return _tx_volume(entries)
    except Exception as exc:  # pragma: no cover
        _log_metric_failure("volume", token, exc)
        return 0.0


def _tx_volume(entries: Iterable[dict]) -> float:
    """Sum any available 'amount' fields in signature entries safely."""
    total = 0.0
    for e in entries:
        total += _safe_float(e.get("amount"), 0.0)
    return total


# ---------------------------------------------------------------------------
# Mempool / whale / swap metrics (defensive, cached)
# ---------------------------------------------------------------------------

def fetch_mempool_tx_rate(token: str, rpc_url: str, limit: int = 20) -> float:
    """
    Approximate mempool tx *rate* (tx/sec) for `token` from signature timestamps.
    Safe against malformed timestamps; cached to avoid hammering the RPC.
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    cache_key = (token, rpc_url)
    cached = MEMPOOL_RATE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = Client(rpc_url)
    try:
        pubkey = _to_pubkey(token)
        limits = _signature_attempt_limits(limit)
        resp = _get_signatures_for_address(client, pubkey, limits=limits)
        entries = extract_signature_entries(resp)
        times = [_safe_int(e.get("blockTime")) for e in entries if e.get("blockTime") is not None]
        times = [t for t in times if t > 0]
        if len(times) >= 2:
            duration = max(times) - min(times)
            rate = (len(times) / float(duration)) if duration > 0 else float(len(times))
        else:
            rate = float(len(times))
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch mempool rate for %s: %s", token, exc)
        rate = 0.0

    # Optional feature collection for forecasting
    features = []
    try:
        depth_change = 0.0
        try:
            from . import onchain_metrics  # late import to avoid cycles
            depth_change = onchain_metrics.order_book_depth_change(token)
        except Exception:
            depth_change = 0.0

        whale = fetch_whale_wallet_activity(token, rpc_url)
        avg_swap = fetch_average_swap_size(token, rpc_url)
        features = [_safe_float(depth_change), _safe_float(rate), _safe_float(whale), _safe_float(avg_swap)]
    except Exception:
        features = [0.0, _safe_float(rate), 0.0, 0.0]

    hist = MEMPOOL_FEATURE_HISTORY.setdefault(cache_key, [])
    hist.append(features)

    # Optional tiny forecaster hook
    model_path = os.getenv("ONCHAIN_MODEL_PATH")
    if model_path:
        try:
            from .models.onchain_forecaster import get_model  # type: ignore
            model = get_model(model_path)
            if model is not None:
                seq_len = getattr(model, "seq_len", 30)
                if len(hist) >= seq_len:
                    seq = hist[-seq_len:]
                    try:
                        rate = _safe_float(model.predict(seq), _safe_float(rate))
                    except Exception as exc:  # pragma: no cover
                        logger.warning("Forecast failed: %s", exc)
        except Exception:
            # forecasting is optional; ignore entirely if anything is off
            pass

    # Keep history bounded
    try:
        hist[:] = hist[-30:]
    except Exception:
        pass

    MEMPOOL_RATE_CACHE.set(cache_key, _safe_float(rate))
    return _safe_float(rate)


def fetch_whale_wallet_activity(
    token: str,
    rpc_url: str,
    threshold: float = 1_000_000.0,
) -> float:
    """
    Fraction of supply held by large accounts (>= threshold).
    Uses `getTokenLargestAccounts`; fully defensive/coerced.
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    cache_key = (token, rpc_url)
    cached = WHALE_ACTIVITY_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = Client(rpc_url)
    try:
        resp = client.get_token_largest_accounts(_to_pubkey(token))
        accounts = extract_token_accounts(resp)
        total = 0.0
        whales = 0.0
        thr = _safe_float(threshold, 0.0)
        for acc in accounts:
            bal = _safe_float(acc.get("uiAmount", acc.get("amount", 0.0)), 0.0)
            total += bal
            if bal >= thr:
                whales += bal
        activity = (whales / total) if total > 0 else 0.0
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch whale activity for %s: %s", token, exc)
        activity = 0.0

    WHALE_ACTIVITY_CACHE.set(cache_key, _safe_float(activity))
    return _safe_float(activity)


def fetch_average_swap_size(token: str, rpc_url: str, limit: int = 20) -> float:
    """
    Average swap 'amount' inferred from recent signatures (best-effort).
    Graceful on missing/dirty fields; cached.
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    cache_key = (token, rpc_url)
    cached = AVG_SWAP_SIZE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = Client(rpc_url)
    try:
        pubkey = _to_pubkey(token)
        limits = _signature_attempt_limits(limit)
        resp = _get_signatures_for_address(client, pubkey, limits=limits)
        entries = extract_signature_entries(resp)
        total = 0.0
        count = 0
        for e in entries:
            amt = _safe_float(e.get("amount"), 0.0)
            if amt > 0:
                total += amt
                count += 1
        size = (total / float(count)) if count else 0.0
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch swap size for %s: %s", token, exc)
        size = 0.0

    AVG_SWAP_SIZE_CACHE.set(cache_key, _safe_float(size))
    return _safe_float(size)
