"""Helpers for interacting with Helius DAS search APIs."""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
_RPC_BASE_DEFAULT = "https://mainnet.helius-rpc.com"
DAS_BASE = (os.getenv("DAS_BASE_URL") or _RPC_BASE_DEFAULT).strip().rstrip("/")
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
            "client_module": __name__,
            "helius_keys": len(_API_KEYS.keys),
            "das_rps": _rl.rps,
            "das_burst": _rl.burst,
        },
    )
else:
    log.warning("HELIUS_API_KEY(S) missing; DAS requests will fail fast")


def _rpc_url_for(key: str) -> str:
    base = DAS_BASE or _RPC_BASE_DEFAULT
    if "api-key=" in base:
        try:
            parts = urlsplit(base)
            query = dict(parse_qsl(parts.query, keep_blank_values=True))
            query["api-key"] = key
            new_query = urlencode(query)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
        except Exception:  # pragma: no cover - defensive parsing
            pass
    if "?" in base:
        separator = "&"
    else:
        separator = "?"
    return f"{base}{separator}api-key={key}"


async def _post_rpc(
    session: aiohttp.ClientSession,
    method: str,
    params: Dict[str, Any],
    *,
    timeout: float | None = None,
    headers: Optional[Dict[str, str]] = None,
    op: str = "unknown",
) -> Dict[str, Any]:
    """POST helper with retry/backoff semantics for DAS JSON-RPC endpoints."""

    await _rl.acquire()
    key = _API_KEYS.next()
    url = _rpc_url_for(key)
    total_timeout = timeout or _SESSION_TIMEOUT
    backoff = _BACKOFF_BASE
    last_exception: Exception | None = None
    payload = {
        "jsonrpc": "2.0",
        "id": f"das-{int(time.time() * 1000)}-{random.randint(1, 1000)}",
        "method": method,
        "params": params,
    }
    for attempt in range(_MAX_RETRIES):
        try:
            async with session.post(
                url,
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
                if not isinstance(data, dict):
                    return {}
                if "error" in data and data.get("error"):
                    raise RuntimeError(f"DAS RPC error: {data['error']}")
                return data
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
        "tokenType": "fungible",
        "page": 1,
        "limit": resolved_limit,
        "sortBy": "created",
        "sortDirection": sort_direction,
    }
    if cursor:
        payload["paginationToken"] = cursor
    payload = _clean_payload(payload)
    data = await _post_rpc(
        session,
        "searchAssets",
        payload,
        op="searchAssets",
    )
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, list):
        items = result
        next_cursor = None
    elif isinstance(result, dict):
        items = (
            result.get("items")
            or result.get("tokens")
            or result.get("assets")
            or []
        )
        next_cursor = (
            result.get("paginationToken")
            or result.get("pagination_token")
            or result.get("cursor")
            or result.get("nextCursor")
            or (result.get("pagination") or {}).get("next")
            or (result.get("pagination") or {}).get("cursor")
        )
    else:
        items = []
        next_cursor = None
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
    data = await _post_rpc(
        session,
        "getAssetBatch",
        payload,
        op="getAssetBatch",
    )
    result = data.get("result") if isinstance(data, dict) else None
    if isinstance(result, list):
        assets = result
    elif isinstance(result, dict):
        assets = (
            result.get("items")
            or result.get("assets")
            or result.get("tokens")
            or []
        )
    else:
        assets = []
    return [item for item in assets if isinstance(item, dict)]


__all__ = [
    "search_fungible_recent",
    "get_asset_batch",
    "RateLimiter",
]
