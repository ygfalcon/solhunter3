"""Helpers for interacting with Helius DAS search APIs."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

import aiohttp

log = logging.getLogger(__name__)


def _env_float(name: str, default: str) -> float:
    raw = os.getenv(name, default)
    try:
        return float(raw)
    except Exception:
        try:
            return float(default)
        except Exception:
            return 0.0


def _env_int(name: str, default: str, *, minimum: int = 1) -> int:
    raw = os.getenv(name, default)
    try:
        return max(minimum, int(raw))
    except Exception:
        try:
            return max(minimum, int(default))
        except Exception:
            return minimum


class RateLimiter:
    """Token bucket limiter that works with asyncio coroutines."""

    def __init__(self, rps: float, burst: int = 2):
        self.tokens = burst
        self.rps = max(0.0, rps)
        self.burst = max(burst, 1)
        self.last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while self.tokens <= 0:
                now = time.monotonic()
                refill = (now - self.last) * self.rps
                if refill >= 1:
                    self.tokens = min(
                        self.tokens + int(refill),
                        max(self.burst, int(self.rps) + self.burst),
                    )
                    self.last = now
                else:
                    await asyncio.sleep(0.05)
            self.tokens -= 1


@dataclass(slots=True)
class _APIKeyPool:
    """Round-robin API key pool supporting comma-separated keys."""

    keys: Tuple[str, ...]
    index: int = 0

    def next(self) -> str:
        if not self.keys:
            raise RuntimeError("HELIUS_API_KEY(S) not configured")
        key = self.keys[self.index]
        self.index = (self.index + 1) % len(self.keys)
        return key


def _load_keys() -> Tuple[str, ...]:
    multi = os.getenv("HELIUS_API_KEYS")
    if multi:
        keys = tuple(k.strip() for k in multi.split(",") if k.strip())
        if keys:
            return keys
    single = os.getenv("HELIUS_API_KEY") or os.getenv("HELIUS_API_TOKEN")
    if single:
        return (single.strip(),)
    return tuple()


_API_KEYS = _APIKeyPool(_load_keys())
DAS_BASE = os.getenv("DAS_BASE_URL", "https://api.helius.xyz/v1").rstrip("/")
_DEFAULT_LIMIT = _env_int("DAS_DISCOVERY_LIMIT", "100")
_SESSION_TIMEOUT = _env_float("DAS_TIMEOUT_TOTAL", "5.0") or 5.0
_CONNECT_TIMEOUT = _env_float("DAS_TIMEOUT_CONNECT", "1.5") or 1.5
_MAX_RETRIES = _env_int("DAS_MAX_RETRIES", "6", minimum=1)
_BACKOFF_BASE = max(0.1, _env_float("DAS_BACKOFF_BASE", "0.25"))
_BACKOFF_CAP = max(_BACKOFF_BASE, _env_float("DAS_BACKOFF_CAP", "5.0"))

_rl = RateLimiter(
    rps=_env_float("DAS_RPS", "2"),
    burst=_env_int("DAS_BURST", os.getenv("DAS_BURST", "4")),
)

if _API_KEYS.keys:
    log.info(
        "Helius DAS client configured",
        extra={
            "module": __name__,
            "helius_keys": len(_API_KEYS.keys),
            "das_rps": _rl.rps,
            "das_burst": _rl.burst,
        },
    )
else:
    log.warning("HELIUS_API_KEY(S) missing; DAS requests will fail fast")


async def _post_json(
    session: aiohttp.ClientSession,
    path: str,
    payload: Dict[str, Any],
    *,
    timeout: float | None = None,
    headers: Optional[Dict[str, str]] = None,
    op: str = "unknown",
) -> Dict[str, Any]:
    """POST helper with retry/backoff semantics for DAS endpoints."""

    await _rl.acquire()
    url = f"{DAS_BASE}/{path.lstrip('/')}"
    key = _API_KEYS.next()
    params = {"api-key": key}
    total_timeout = timeout or _SESSION_TIMEOUT
    backoff = _BACKOFF_BASE
    last_exception: Exception | None = None
    for attempt in range(_MAX_RETRIES):
        try:
            async with session.post(
                url,
                params=params,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=total_timeout, connect=_CONNECT_TIMEOUT),
                headers=headers,
            ) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else backoff
                    jitter = random.uniform(0, delay * 0.25)
                    wait_for = min(delay + jitter, _BACKOFF_CAP)
                    log.warning(
                        "DAS request hit 429",
                        extra={"op": op, "retry_after": retry_after, "delay": wait_for},
                    )
                    await asyncio.sleep(wait_for)
                    backoff = min(backoff * 2, _BACKOFF_CAP)
                    continue
                resp.raise_for_status()
                data = await resp.json()
                if isinstance(data, dict):
                    return data
                return {}
        except Exception as exc:  # pragma: no cover - network failures mocked in tests
            last_exception = exc
            jitter = random.uniform(0, backoff * 0.25)
            wait_for = min(backoff + jitter, _BACKOFF_CAP)
            await asyncio.sleep(wait_for)
            backoff = min(backoff * 2, _BACKOFF_CAP)
    if last_exception is not None:
        raise last_exception
    raise RuntimeError("DAS request failed without exception")


def _clean_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in payload.items() if v is not None}


async def search_fungible_recent(
    session: aiohttp.ClientSession,
    *,
    cursor: Optional[str] = None,
    limit: Optional[int] = None,
    sort_direction: str = "desc",
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Fetch the most recently active fungible assets via searchAssets."""

    resolved_limit = limit if limit is not None else _DEFAULT_LIMIT
    payload: Dict[str, Any] = {
        "ownerAddress": None,
        "interface": "FungibleToken",
        "limit": resolved_limit,
        "sortBy": {"field": "recentAction", "direction": sort_direction},
    }
    if cursor:
        payload["page"] = {"cursor": cursor}
    payload = _clean_payload(payload)
    data = await _post_json(
        session,
        "nft-events/searchAssets",
        payload,
        op="searchAssets",
    )
    items = data.get("items") or data.get("result") or []
    next_cursor = data.get("cursor") or data.get("page", {}).get("cursor")
    if isinstance(items, list):
        cleaned_items = [item for item in items if isinstance(item, dict)]
    else:
        cleaned_items = []
    return cleaned_items, next_cursor if isinstance(next_cursor, str) else None


async def get_asset_batch(
    session: aiohttp.ClientSession,
    mints: Iterable[str],
) -> List[Dict[str, Any]]:
    """Validate mint addresses and return DAS asset objects."""

    batch = [mint for mint in mints if isinstance(mint, str)]
    if not batch:
        return []
    payload = {"ids": batch}
    data = await _post_json(
        session,
        "nft-events/getAssetBatch",
        payload,
        op="getAssetBatch",
    )
    result = data.get("result") or data.get("assets") or []
    return [item for item in result if isinstance(item, dict)]


__all__ = [
    "search_fungible_recent",
    "get_asset_batch",
    "RateLimiter",
]
