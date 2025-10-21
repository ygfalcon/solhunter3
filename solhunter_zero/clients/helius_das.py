"""Helpers for interacting with Helius DAS search APIs."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import aiohttp

from ..token_aliases import normalize_mint_or_none

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


def _env_flag(name: str, default: str = "0") -> bool:
    raw = os.getenv(name)
    if raw is None:
        raw = default
    normalized = str(raw).strip().lower()
    return normalized in {"1", "true", "yes", "on"}


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


_RPC_BASE_DEFAULT = "https://mainnet.helius-rpc.com"


def _resolve_base_url() -> str:
    base = (os.getenv("DAS_BASE_URL") or _RPC_BASE_DEFAULT).strip()
    return base.rstrip("/") if base else _RPC_BASE_DEFAULT


def _extract_api_key_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        parts = urlsplit(url)
        if not parts.query:
            return None
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
    except Exception:  # pragma: no cover - defensive parsing
        return None
    key = query.get("api-key")
    if isinstance(key, str) and key.strip():
        return key.strip()
    return None


def _load_keys() -> Tuple[str, ...]:
    multi = os.getenv("HELIUS_API_KEYS")
    if multi:
        keys = tuple(k.strip() for k in multi.split(",") if k.strip())
        if keys:
            return keys
    single = os.getenv("HELIUS_API_KEY") or os.getenv("HELIUS_API_TOKEN")
    if single:
        return (single.strip(),)
    inline = _extract_api_key_from_url(_resolve_base_url())
    if inline:
        return (inline,)
    return tuple()


_API_KEYS = _APIKeyPool(_load_keys())
DAS_BASE = _resolve_base_url()
_DEFAULT_LIMIT = _env_int("DAS_DISCOVERY_LIMIT", "60")
_SESSION_TIMEOUT = _env_float("DAS_TIMEOUT_TOTAL", "5.0") or 5.0
_CONNECT_TIMEOUT = _env_float("DAS_TIMEOUT_CONNECT", "1.5") or 1.5
_MAX_RETRIES = _env_int("DAS_MAX_RETRIES", "2", minimum=1)
_BACKOFF_BASE = max(0.1, _env_float("DAS_BACKOFF_BASE", "0.4"))
_BACKOFF_CAP = max(_BACKOFF_BASE, _env_float("DAS_BACKOFF_CAP", "0.8"))
_DISABLE_CREATED_SORT = _env_flag("DAS_DISABLE_CREATED_SORT", "0")

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


def _summarize_params(params: Dict[str, Any], *, limit: int = 256) -> str:
    try:
        rendered = json.dumps(params, default=str, separators=(",", ":"))
    except Exception:
        rendered = str(params)
    if len(rendered) > limit:
        return f"{rendered[: limit - 3]}..."
    return rendered


_LAST_SUCCESSFUL_SEARCH_PAYLOAD: str | None = None


def _log_success_payload(payload: Dict[str, Any]) -> None:
    global _LAST_SUCCESSFUL_SEARCH_PAYLOAD
    summary = _summarize_params(payload, limit=512)
    if summary != _LAST_SUCCESSFUL_SEARCH_PAYLOAD:
        log.info("Helius DAS searchAssets succeeded with payload=%s", summary)
        _LAST_SUCCESSFUL_SEARCH_PAYLOAD = summary


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
    url: str
    if _API_KEYS.keys:
        url = _rpc_url_for(_API_KEYS.next())
    else:
        if "api-key=" not in DAS_BASE:
            raise RuntimeError("HELIUS_API_KEY(S) not configured")
        url = DAS_BASE
    total_timeout = timeout or _SESSION_TIMEOUT
    backoff = _BACKOFF_BASE
    last_exception: Exception | None = None
    payload = {
        "jsonrpc": "2.0",
        "id": f"das-{int(time.time() * 1000)}-{random.randint(1, 1000)}",
        "method": method,
        "params": params,
    }
    prepared_headers = dict(headers or {})
    prepared_headers.setdefault("Content-Type", "application/json")
    rate_limit_logged = False
    retry_logged = False
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            log.debug("DAS payload: %s", json.dumps(payload, separators=(",", ":")))
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=total_timeout, connect=_CONNECT_TIMEOUT),
                headers=prepared_headers,
            ) as resp:
                if resp.status == 429:
                    retry_after = resp.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else backoff
                    except (TypeError, ValueError):
                        delay = backoff
                    jitter = random.uniform(0, delay * 0.25)
                    wait_for = min(delay + jitter, _BACKOFF_CAP)
                    if not rate_limit_logged:
                        log.warning(
                            "DAS request hit 429",
                            extra={
                                "op": op,
                                "attempt": attempt,
                                "status": resp.status,
                                "retry_after": retry_after,
                                "delay": wait_for,
                            },
                        )
                        rate_limit_logged = True
                    if attempt >= _MAX_RETRIES:
                        raise RuntimeError("DAS RPC rate limited (429)")
                    await asyncio.sleep(wait_for)
                    backoff = min(backoff * 2, _BACKOFF_CAP)
                    continue
                resp.raise_for_status()
                text = await resp.text()
                try:
                    data = json.loads(text)
                except json.JSONDecodeError as err:
                    raise RuntimeError("Invalid JSON returned from DAS") from err
                if not isinstance(data, dict):
                    raise RuntimeError("Unexpected DAS response type")
                if data.get("error"):
                    summary = _summarize_params(params)
                    raise RuntimeError(
                        f"DAS RPC error for {method}: {data['error']} (params={summary})"
                    )
                return data
        except Exception as exc:  # pragma: no cover - network failures mocked in tests
            last_exception = exc
            if attempt >= _MAX_RETRIES:
                break
            jitter = random.uniform(0, backoff * 0.25)
            wait_for = min(backoff + jitter, _BACKOFF_CAP)
            if not retry_logged:
                log.warning(
                    "Retrying DAS request",
                    extra={
                        "op": op,
                        "attempt": attempt,
                        "delay": wait_for,
                        "error_type": type(exc).__name__,
                        "status": getattr(exc, "status", None),
                    },
                )
                retry_logged = True
            await asyncio.sleep(wait_for)
            backoff = min(backoff * 2, _BACKOFF_CAP)
    if last_exception is not None:
        log.error(
            "DAS request failed",
            extra={
                "op": op,
                "attempts": _MAX_RETRIES,
                "error_type": type(last_exception).__name__,
                "status": getattr(last_exception, "status", None),
            },
        )
        raise last_exception
    raise RuntimeError("DAS request failed without exception")


_MINT_PARAM_KEYS = {"ids", "mintIds", "mint_ids", "tokenAddresses", "mintAddresses"}


def _clean_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Drop ``None``/empty values while preserving falsy scalars like ``0``/``False``."""

    cleaned: Dict[str, Any] = {}
    for key, value in payload.items():
        if value is None:
            continue
        if key in _MINT_PARAM_KEYS:
            if isinstance(value, (list, tuple, set, frozenset)):
                normalized = _filter_valid_mints(value)
                if not normalized:
                    continue
                cleaned[key] = normalized
                continue
            normalized_single = normalize_mint_or_none(value)
            if normalized_single:
                cleaned[key] = normalized_single
            continue
        if isinstance(value, str) and value == "":
            continue
        if isinstance(value, (list, tuple, set, frozenset)) and not value:
            continue
        if isinstance(value, dict) and not value:
            continue
        cleaned[key] = value
    return cleaned


def _filter_valid_mints(mints: Iterable[str]) -> List[str]:
    filtered: List[str] = []
    for mint in mints:
        normalized = normalize_mint_or_none(mint)
        if normalized:
            filtered.append(normalized)
    return filtered


def should_disable_token_type(message: str) -> bool:
    lowered = message.lower()
    return "owner_address" in lowered and "token_type" in lowered


def _normalize_sort_direction(direction: str) -> tuple[str, str]:
    raw = (direction or "desc").strip()
    lower = raw.lower()
    if lower not in {"asc", "desc"}:
        lower = "desc"
    upper = lower.upper()
    return lower, upper


def build_sort_variants(direction: str) -> Tuple[Any, ...]:
    lower, upper = _normalize_sort_direction(direction)
    variants: List[Any] = []
    if not _DISABLE_CREATED_SORT:
        variants.extend(
            [
                {"sortBy": "created", "sortDirection": lower},
                {"sortBy": "created", "sortDirection": upper},
                {"field": "created", "sortDirection": lower},
                {"field": "created", "sortDirection": upper},
                {"field": "created", "direction": lower},
                {"field": "created", "direction": upper},
                "created",
            ]
        )
    variants.append(None)
    return tuple(variants)


_SORT_ERROR_TOKENS = ("sortby", "sortdirection", "sort field", "assetsorting")
_SORT_ERROR_HINTS = ("missing", "unknown", "invalid", "expected")


def should_try_next_sort_variant(message: str, variant: Any) -> bool:
    if variant is None:
        return False
    lowered = message.lower()
    if not any(token in lowered for token in _SORT_ERROR_TOKENS):
        return False
    if not any(hint in lowered for hint in _SORT_ERROR_HINTS):
        return False
    return True


def _maybe_sort_by(sort_direction: str) -> Dict[str, Any]:
    """Return a ``sortBy`` payload when created-sort is enabled."""

    if _DISABLE_CREATED_SORT:
        return {}
    return {"sortBy": {"sortBy": "created", "sortDirection": sort_direction}}


def build_search_param_variants(
    *,
    page: Optional[int],
    limit: Optional[int],
    sort_direction: str = "desc",
    include_interface: bool = True,
    owner_address: Optional[str] = None,
    allow_created_sort: Optional[bool] = None,
) -> Tuple[Dict[str, Any], ...]:
    """Return parameter variants compatible with DAS ``searchAssets``."""

    if allow_created_sort is None:
        allow_created_sort = not _DISABLE_CREATED_SORT

    base: Dict[str, Any] = {"page": page, "limit": limit}
    if allow_created_sort:
        base.update(_maybe_sort_by(sort_direction))

    variants: List[Dict[str, Any]] = []
    interface_variant = dict(base)
    if include_interface:
        interface_variant["interface"] = "FungibleToken"
    variants.append(interface_variant)

    if owner_address:
        owner_variant = dict(base)
        owner_variant["ownerAddress"] = owner_address
        if include_interface:
            owner_variant["interface"] = "FungibleToken"
        variants.append(owner_variant)

    return tuple(_clean_payload(variant) for variant in variants)


async def search_fungible_recent(
    session: aiohttp.ClientSession,
    *,
    page: Optional[int] = None,
    cursor: Optional[int] = None,
    limit: Optional[int] = None,
    sort_direction: str = "desc",
    owner_address: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    """Fetch the most recently active fungible assets via searchAssets."""

    if limit is not None:
        try:
            resolved_limit = max(1, int(limit))
        except (TypeError, ValueError):
            resolved_limit = _DEFAULT_LIMIT
    else:
        resolved_limit = _DEFAULT_LIMIT
    page_hint = cursor if cursor is not None else page
    page_number = 1
    if page_hint is not None:
        try:
            page_number = max(1, int(page_hint))
        except (TypeError, ValueError):
            page_number = 1
    data: Dict[str, Any] | None = None
    last_error: Exception | None = None
    include_interface = True
    while True:
        retry_without_interface = False
        for variant_params in build_search_param_variants(
            page=page_number,
            limit=resolved_limit,
            sort_direction=sort_direction,
            include_interface=include_interface,
            owner_address=owner_address,
            allow_created_sort=not _DISABLE_CREATED_SORT,
        ):
            result_data: Dict[str, Any] | None = None
            for sort_variant in build_sort_variants(sort_direction):
                params_with_sort = dict(variant_params)
                if sort_variant is not None:
                    params_with_sort["sortBy"] = sort_variant
                payload = _clean_payload(params_with_sort)
                try:
                    result_data = await _post_rpc(
                        session,
                        "searchAssets",
                        payload,
                        op="searchAssets",
                    )
                    _log_success_payload(payload)
                    break
                except RuntimeError as exc:
                    last_error = exc
                    message = str(exc)
                    lowered = message.lower()
                    if include_interface and should_disable_token_type(lowered):
                        include_interface = False
                        retry_without_interface = True
                        break
                    if "unknown field `query`" in lowered:
                        break
                    if should_try_next_sort_variant(lowered, sort_variant):
                        continue
                    raise
            if retry_without_interface:
                break
            if result_data is not None:
                data = result_data
                break
        if data is not None:
            break
        if retry_without_interface:
            continue
        if last_error is not None:
            raise last_error
        return [], None
    if not isinstance(data, dict):
        return [], None

    result = data.get("result")
    if isinstance(result, list):
        items = result
    elif isinstance(result, dict):
        items = result.get("items") or result.get("tokens") or result.get("assets") or []
    else:
        items = data.get("items") or data.get("tokens") or data.get("assets") or []

    cleaned_items = [item for item in items if isinstance(item, dict)] if isinstance(items, list) else []

    if isinstance(result, dict):
        next_page = result.get("page")
        if isinstance(next_page, int) and next_page > page_number:
            return cleaned_items, next_page
        cursor_val = result.get("cursor") or result.get("next")
        if cursor_val:
            return cleaned_items, cursor_val

    if len(cleaned_items) >= resolved_limit:
        return cleaned_items, page_number + 1

    return cleaned_items, None


async def get_asset_batch(
    session: aiohttp.ClientSession,
    mints: Iterable[str],
) -> List[Dict[str, Any]]:
    """Validate mint addresses and return DAS asset objects."""

    batch = _filter_valid_mints(mints)
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
        fallback = []
        if isinstance(data, dict):
            for key in ("items", "assets", "tokens"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    fallback = candidate
                    break
        assets = fallback
    return [item for item in assets if isinstance(item, dict)]


__all__ = [
    "search_fungible_recent",
    "get_asset_batch",
    "RateLimiter",
    "build_sort_variants",
    "should_try_next_sort_variant",
    "build_search_param_variants",
    "should_disable_token_type",
]
