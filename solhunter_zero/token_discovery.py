# solhunter_zero/token_discovery.py
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import math
import os
import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, AsyncIterator

import yaml
from aiohttp import ClientTimeout
import aiohttp

from .http import HostCircuitOpenError, host_request, host_retry_config
from .http import get_session as _shared_http_session
from .scanner_common import DEFAULT_BIRDEYE_API_KEY
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
_PAGE_LIMIT = max(1, min(int(os.getenv("DISCOVERY_PAGE_SIZE", "25") or 25), 50))
_OVERFETCH_FACTOR = float(os.getenv("DISCOVERY_OVERFETCH_FACTOR", "0.8") or 0.8)
_CACHE_TTL = float(os.getenv("DISCOVERY_CACHE_TTL", "45") or 45)
_MAX_OFFSET = int(os.getenv("DISCOVERY_MAX_OFFSET", "4000") or 4000)
_MEMPOOL_LIMIT = int(os.getenv("DISCOVERY_MEMPOOL_LIMIT", "12") or 12)
_ENABLE_MEMPOOL = os.getenv("DISCOVERY_ENABLE_MEMPOOL", "1").lower() in {"1", "true", "yes"}
_WARM_TIMEOUT = float(os.getenv("DISCOVERY_WARM_TIMEOUT", "5") or 5)
_BIRDEYE_RETRIES = int(os.getenv("DISCOVERY_BIRDEYE_RETRIES", "3") or 3)
_BIRDEYE_BACKOFF = float(os.getenv("DISCOVERY_BIRDEYE_BACKOFF", "1.0") or 1.0)
_BIRDEYE_BACKOFF_MAX = float(os.getenv("DISCOVERY_BIRDEYE_BACKOFF_MAX", "8.0") or 8.0)
_BIRDEYE_THROTTLE_MARKERS = (
    "compute units usage limit exceeded",
    "request limit exceeded",
    "rate limit exceeded",
    "too many requests",
    "throttle",
)

_ENABLE_DEXSCREENER = (
    os.getenv("DISCOVERY_ENABLE_DEXSCREENER", "1").lower() in {"1", "true", "yes", "on"}
)
_DEXSCREENER_URL = (
    os.getenv("DEXSCREENER_TOKENS_URL")
    or "https://api.dexscreener.com/latest/dex/tokens?chainId=solana"
).strip()
_DEXSCREENER_TIMEOUT = float(os.getenv("DEXSCREENER_TIMEOUT", "8.0") or 8.0)
_DEXSCREENER_MAX_AGE_SECONDS = float(
    os.getenv("DEXSCREENER_MAX_AGE_SECONDS", "3600") or 3600.0
)

_ENABLE_METEORA = (
    os.getenv("DISCOVERY_ENABLE_METEORA", "1").lower() in {"1", "true", "yes", "on"}
)
_METEORA_POOLS_URL = (
    os.getenv("METEORA_POOLS_URL")
    or os.getenv("METEORA_DISCOVERY_URL")
    or "https://dlmm-api.meteora.ag/api/pools/latest"
).strip()
_METEORA_TIMEOUT = float(os.getenv("METEORA_TIMEOUT", "8.0") or 8.0)

_ENABLE_DEXLAB = (
    os.getenv("DISCOVERY_ENABLE_DEXLAB", "1").lower() in {"1", "true", "yes", "on"}
)
_DEXLAB_LIST_URL = (
    os.getenv("DEXLAB_LIST_URL") or "https://api.dexlab.space/v1/token/list"
).strip()
_DEXLAB_TIMEOUT = float(os.getenv("DEXLAB_TIMEOUT", "8.0") or 8.0)

_ENABLE_SOLSCAN = (
    os.getenv("DISCOVERY_ENABLE_SOLSCAN", "1").lower() in {"1", "true", "yes", "on"}
)
_SOLSCAN_META_URL = (
    os.getenv("DISCOVERY_SOLSCAN_META_URL")
    or os.getenv("SOLSCAN_DISCOVERY_URL")
    or "https://public-api.solscan.io/token/meta"
).strip()
_SOLSCAN_API_KEY = (os.getenv("SOLSCAN_API_KEY") or "").strip()
_SOLSCAN_TIMEOUT = float(os.getenv("DISCOVERY_SOLSCAN_TIMEOUT", "6.0") or 6.0)
_SOLSCAN_ENRICH_LIMIT = max(0, int(os.getenv("DISCOVERY_SOLSCAN_LIMIT", "8") or 8))

TokenEntry = Dict[str, Any]

_BIRDEYE_CACHE: TTLCache[str, List[TokenEntry]] = TTLCache(maxsize=1, ttl=_CACHE_TTL)
_CACHE_LOCK = Lock()
_BIRDEYE_DISABLED_INFO = False

_BIRDEYE_TOKENLIST_URL = (
    (os.getenv("BIRDEYE_TOKENLIST_URL") or "https://api.birdeye.so/defi/tokenlist")
    .strip()
)
if not _BIRDEYE_TOKENLIST_URL:
    _BIRDEYE_TOKENLIST_URL = "https://api.birdeye.so/defi/tokenlist"


_SCORING_DEFAULT = {
    "bias": -2.3,
    "weights": {
        "liquidity_usd": 0.25,
        "vol_1h_z": 0.2,
        "pool_age_min": -0.35,
        "source_diversity": 0.35,
        "oracle_present": 0.45,
        "sellable": 0.4,
        "mempool_pressure": 2.4,
        "staleness_ms": -0.3,
    },
}


def _resolve_weights_path() -> Path:
    configured = os.getenv("DISCOVERY_SCORE_WEIGHTS")
    if configured:
        candidate = Path(configured).expanduser()
        if candidate.exists():
            return candidate
    default = Path(__file__).resolve().parents[1] / "configs" / "discovery_score_weights.yaml"
    return default


def _load_scoring_weights() -> tuple[float, Dict[str, float]]:
    bias = float(_SCORING_DEFAULT["bias"])
    weights = dict(_SCORING_DEFAULT["weights"])
    path = _resolve_weights_path()
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}
    except Exception:
        payload = {}
    try:
        bias = float(payload.get("bias", bias))
    except Exception:
        bias = float(_SCORING_DEFAULT["bias"])
    raw_weights = payload.get("weights") or {}
    if isinstance(raw_weights, dict):
        for key, value in raw_weights.items():
            try:
                weights[key] = float(value)
            except Exception:
                continue
    return bias, weights


_SCORING_BIAS, _SCORING_WEIGHTS = _load_scoring_weights()
_STAGE_B_SCORE_THRESHOLD = float(os.getenv("DISCOVERY_STAGE_B_THRESHOLD", "0.65") or 0.65)
_STAGE_B_MIN_SOURCES = int(os.getenv("DISCOVERY_STAGE_B_MIN_SOURCES", "2") or 2)
_SOLSCAN_NEGATIVE_TTL = float(os.getenv("SOLSCAN_NEGATIVE_TTL", "1800") or 1800.0)
_SOLSCAN_NEGATIVE_CACHE: TTLCache[str, bool] = TTLCache(maxsize=2048, ttl=_SOLSCAN_NEGATIVE_TTL)

_ETAG_HOSTS = {
    "api.dexscreener.com",
    "public-api.birdeye.so",
    "api.meteora.ag",
    "api.dexlab.space",
}


@dataclass(slots=True)
class _CachedJSON:
    data: Any
    etag: str | None
    timestamp: float


_JSON_CACHE: Dict[str, _CachedJSON] = {}
_JSON_CACHE_LOCK: asyncio.Lock | None = None


def _cache_key(url: str, params: Dict[str, Any] | None) -> str:
    if not params:
        return url
    try:
        items = sorted((k, json.dumps(v, sort_keys=True)) for k, v in params.items())
    except Exception:
        items = sorted((str(k), str(v)) for k, v in params.items())
    serialised = "&".join(f"{k}={v}" for k, v in items)
    return f"{url}?{serialised}"


async def _get_cache_lock() -> asyncio.Lock:
    global _JSON_CACHE_LOCK
    if _JSON_CACHE_LOCK is None:
        _JSON_CACHE_LOCK = asyncio.Lock()
    return _JSON_CACHE_LOCK


def _resolve_birdeye_api_key() -> str:
    """Return the configured BirdEye API key (env var or default)."""

    api_key = (os.getenv("BIRDEYE_API_KEY") or "").strip()
    if not api_key:
        default_key = (DEFAULT_BIRDEYE_API_KEY or "").strip()
        api_key = default_key
    return api_key


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


def _make_timeout(value: Any) -> ClientTimeout | None:
    if isinstance(value, ClientTimeout):
        return value
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return ClientTimeout(total=numeric)


async def get_session(*, timeout: ClientTimeout | None = None) -> aiohttp.ClientSession:
    """Return a shared HTTP session."""

    _ = timeout  # Per-call timeouts handled on requests; session is shared.
    return await _shared_http_session()


async def fetch_trending_tokens_async() -> List[str]:  # pragma: no cover - legacy hook
    """Compatibility shim for older tests; returns no extra tokens."""
    return []


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _source_count(entry: Dict[str, Any]) -> int:
    sources = entry.get("sources")
    if isinstance(sources, set):
        return len(sources)
    if isinstance(sources, (list, tuple)):
        return len(set(sources))
    return 0


def _compute_feature_vector(
    entry: Dict[str, Any],
    mempool: Dict[str, float] | None,
) -> Dict[str, float]:
    now = time.time()
    liquidity = max(0.0, _coerce_numeric(entry.get("liquidity")))
    volume = max(0.0, _coerce_numeric(entry.get("volume")))
    liquidity_feature = math.log1p(liquidity / 1000.0)
    vol_feature = entry.get("vol_1h_z")
    if vol_feature is None:
        vol_feature = entry.get("volume_z")
    try:
        vol_feature = float(vol_feature)
    except Exception:
        vol_feature = math.log1p(volume / 1000.0)
    discovered_at = entry.get("discovered_at")
    age_minutes = 0.0
    if discovered_at:
        try:
            age_minutes = max(0.0, (now - float(discovered_at)) / 60.0)
        except Exception:
            age_minutes = 0.0
    else:
        age_minutes = 0.0
    source_diversity = float(_source_count(entry))
    oracle_present = 0.0
    if entry.get("oracle") or entry.get("oracle_present"):
        oracle_present = 1.0
    else:
        try:
            if _coerce_numeric(entry.get("price")) > 0:
                oracle_present = 1.0
        except Exception:
            oracle_present = 0.0
    sellable = 1.0 if liquidity >= _TRENDING_MIN_LIQUIDITY else 0.0
    mempool_pressure = 0.0
    if mempool:
        try:
            mempool_pressure = float(mempool.get("score", 0.0))
        except Exception:
            mempool_pressure = 0.0
    if mempool_pressure <= 0.0:
        try:
            mempool_pressure = float(entry.get("score_mp") or entry.get("mempool_score") or 0.0)
        except Exception:
            mempool_pressure = 0.0
    asof = entry.get("asof") or entry.get("updated_at") or discovered_at
    staleness_minutes = 0.0
    if asof:
        try:
            staleness_minutes = max(0.0, (now - float(asof)) / 60.0)
        except Exception:
            staleness_minutes = 0.0
    features = {
        "liquidity_usd": float(liquidity_feature),
        "vol_1h_z": float(vol_feature),
        "pool_age_min": float(age_minutes),
        "source_diversity": float(source_diversity),
        "oracle_present": float(oracle_present),
        "sellable": float(sellable),
        "mempool_pressure": float(max(0.0, mempool_pressure)),
        "staleness_ms": float(staleness_minutes),
    }
    return features


def _clear_birdeye_cache_for_tests() -> None:
    """Testing helper: clear the BirdEye TTL cache."""
    try:
        _cache_clear()
    except Exception:
        pass


def _coerce_numeric(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, dict):
        for key in ("usd", "USD", "value", "amount"):
            if key in value:
                return _coerce_numeric(value.get(key))
        return 0.0
    try:
        numeric = float(value)
    except Exception:
        return 0.0
    if math.isnan(numeric):
        return 0.0
    return numeric


def _extract_numeric_from_item(item: Dict[str, Any], *keys: str) -> float:
    for key in keys:
        if key not in item:
            continue
        numeric = _coerce_numeric(item.get(key))
        if not math.isnan(numeric):
            return numeric
    return 0.0


def _merge_candidate_entry(
    candidates: Dict[str, Dict[str, Any]],
    token: Dict[str, Any],
    source: str,
) -> Dict[str, Any] | None:
    if not isinstance(token, dict):
        return None
    raw_address = token.get("address") or token.get("mint")
    if not raw_address:
        return None
    address = str(raw_address)
    if not is_valid_solana_mint(address):
        return None

    liquidity = _coerce_numeric(
        token.get("liquidity")
        or token.get("liquidity_usd")
        or token.get("liquidityUsd")
    )
    volume = _coerce_numeric(
        token.get("volume")
        or token.get("volume24h")
        or token.get("volume_usd")
        or token.get("volumeUsd")
    )
    price = _coerce_numeric(token.get("price") or token.get("price_usd"))
    change = _coerce_numeric(token.get("price_change") or token.get("change"))

    name = token.get("name") or token.get("tokenName")
    symbol = token.get("symbol") or token.get("tokenSymbol")

    entry = candidates.get(address)
    discovered_at = _parse_timestamp(
        token.get("discovered_at")
        or token.get("created_at")
        or token.get("createdAt")
        or token.get("pairCreatedAt")
    )

    if entry is None:
        entry = {
            "address": address,
            "symbol": str(symbol or ""),
            "name": str(name or symbol or address),
            "liquidity": liquidity,
            "volume": volume,
            "price": price,
            "price_change": change,
            "sources": set(),
        }
        if discovered_at is not None:
            entry["discovered_at"] = discovered_at
        for extra_key in (
            "verified",
            "decimals",
            "holders",
            "supply",
            "dex_pair_url",
            "pair_address",
            "pool_address",
            "quote_token",
        ):
            if extra_key in token and token[extra_key] is not None:
                entry[extra_key] = token[extra_key]
        candidates[address] = entry
    else:
        if symbol and not entry.get("symbol"):
            entry["symbol"] = str(symbol)
        if name and (not entry.get("name") or entry.get("name") == entry.get("address")):
            entry["name"] = str(name)
        entry["liquidity"] = max(
            _coerce_numeric(entry.get("liquidity")), liquidity
        )
        entry["volume"] = max(_coerce_numeric(entry.get("volume")), volume)
        if price > 0:
            entry["price"] = price
        if change != 0:
            entry["price_change"] = change
        if discovered_at is not None:
            existing = entry.get("discovered_at")
            if not isinstance(existing, (int, float)) or discovered_at < float(existing):
                entry["discovered_at"] = discovered_at
        for extra_key in ("verified", "decimals", "holders", "supply"):
            if (
                extra_key in token
                and token[extra_key] is not None
                and extra_key not in entry
            ):
                entry[extra_key] = token[extra_key]
        for extra_key in ("dex_pair_url", "pair_address", "pool_address", "quote_token"):
            if extra_key in token and extra_key not in entry:
                entry[extra_key] = token[extra_key]

    entry.setdefault("sources", set()).add(source)
    return entry


def _parse_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        if ts <= 0:
            return None
        return ts
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
        try:
            normalized = raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    return None


async def _http_get_json(
    url: str,
    *,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    timeout: Any = None,
    session: aiohttp.ClientSession | None = None,
) -> Any:
    request_timeout = _make_timeout(timeout)
    request_headers: Dict[str, str] = dict(headers or {})
    cache_key = _cache_key(url, params)
    cached_entry: _CachedJSON | None = None
    parsed_host = urlparse(url).hostname or ""
    use_cache = parsed_host in _ETAG_HOSTS
    if use_cache:
        lock = await _get_cache_lock()
        async with lock:
            cached_entry = _JSON_CACHE.get(cache_key)
        if cached_entry and cached_entry.etag:
            request_headers.setdefault("If-None-Match", cached_entry.etag)
        request_headers.setdefault("Accept-Encoding", "gzip, deflate")

    attempts, backoff = host_retry_config(url)
    last_error: Exception | None = None

    if session is not None:
        owned_session = session
    else:
        owned_session = await get_session(timeout=request_timeout)

    for attempt in range(max(1, attempts)):
        try:
            async with host_request(url):
                async with owned_session.get(
                    url,
                    params=params,
                    headers=request_headers,
                    timeout=request_timeout,
                ) as resp:
                    if resp.status == 304 and cached_entry is not None:
                        return cached_entry.data
                    resp.raise_for_status()
                    payload = await resp.json(content_type=None)
                    if use_cache:
                        etag = resp.headers.get("ETag")
                        lock = await _get_cache_lock()
                        async with lock:
                            _JSON_CACHE[cache_key] = _CachedJSON(payload, etag, time.time())
                    return payload
        except HostCircuitOpenError:
            raise
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            await asyncio.sleep(backoff * (2 ** attempt))
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"request to {url} failed without response")


async def _fetch_birdeye_tokens() -> List[TokenEntry]:
    """
    Pull BirdEye token list (paginated) for Solana with correct headers & params.
    Numeric filters only; no name/suffix heuristics.
    """
    api_key = _resolve_birdeye_api_key()
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
        session = await get_session(timeout=session_timeout)
    except TypeError:
        session = await get_session()
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
                request_cm = session.get(
                    _BIRDEYE_TOKENLIST_URL,
                    params=params,
                    headers=_headers(),
                )
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
                        lower_text = text.lower()
                        if any(marker in lower_text for marker in _BIRDEYE_THROTTLE_MARKERS):
                            logger.warning(
                                "BirdEye throttle %s attempt=%s offset=%s backoff=%.2fs: %s",
                                resp.status,
                                attempt,
                                offset,
                                backoff,
                                text[:200],
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
                                        "BirdEye throttle %s after %s tokens; returning partial results",
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
                volume = _extract_numeric_from_item(
                    item, "v24hUSD", "volume24hUSD", "volume"
                )
                liquidity = _extract_numeric_from_item(item, "liquidity")
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
            if total is None:
                total = data.get("totalCount")
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


async def _fetch_dexscreener_tokens(
    *, session: aiohttp.ClientSession | None = None
) -> List[TokenEntry]:
    if not _ENABLE_DEXSCREENER or not _DEXSCREENER_URL:
        return []

    try:
        payload = await _http_get_json(
            _DEXSCREENER_URL,
            headers={"accept": "application/json"},
            timeout=_DEXSCREENER_TIMEOUT,
            session=session,
        )
    except Exception as exc:
        logger.debug("DexScreener discovery request failed: %s", exc)
        return []

    if isinstance(payload, dict):
        candidates = payload.get("pairs") or payload.get("data") or payload.get("results")
    else:
        candidates = payload

    if not isinstance(candidates, list):
        return []

    now = time.time()
    max_age = max(0.0, float(_DEXSCREENER_MAX_AGE_SECONDS))

    tokens: Dict[str, Dict[str, Any]] = {}

    for pair in candidates:
        if not isinstance(pair, dict):
            continue
        chain_id = str(pair.get("chainId") or pair.get("chain_id") or "").lower()
        if chain_id and chain_id not in {"solana", ""}:
            continue

        base = pair.get("baseToken") or pair.get("base_token")
        if isinstance(base, dict):
            mint = base.get("address") or base.get("mint") or base.get("id")
            name = base.get("name")
            symbol = base.get("symbol")
        else:
            mint = None
            name = None
            symbol = None

        if not isinstance(mint, str):
            mint = str(mint) if mint is not None else ""

        if not mint or not is_valid_solana_mint(mint):
            continue

        created_ts = _parse_timestamp(
            pair.get("pairCreatedAt") or pair.get("createdAt") or pair.get("created_at")
        )
        if max_age and created_ts is not None and now - created_ts > max_age:
            continue

        liquidity = 0.0
        liq_raw = pair.get("liquidity")
        if isinstance(liq_raw, dict):
            liquidity = _coerce_numeric(
                liq_raw.get("usd")
                or liq_raw.get("usdValue")
                or liq_raw.get("value")
            )
        else:
            liquidity = _coerce_numeric(liq_raw)

        volume = 0.0
        vol_raw = pair.get("volume")
        if isinstance(vol_raw, dict):
            for key in ("h24", "h6", "h1", "m5", "usd"):
                if key in vol_raw:
                    volume = max(volume, _coerce_numeric(vol_raw.get(key)))
        else:
            volume = _coerce_numeric(vol_raw)

        price = _coerce_numeric(pair.get("priceUsd") or pair.get("price"))
        change_raw = pair.get("priceChange")
        if isinstance(change_raw, dict):
            change = 0.0
            for key in ("h1", "h6", "h24"):
                if key in change_raw:
                    change = _coerce_numeric(change_raw.get(key))
                    if change != 0:
                        break
        else:
            change = _coerce_numeric(change_raw)

        payload_token: Dict[str, Any] = {
            "address": mint,
            "symbol": symbol,
            "name": name,
            "liquidity": liquidity,
            "volume": volume,
            "price": price,
            "price_change": change,
            "discovered_at": created_ts,
        }

        pair_addr = pair.get("pairAddress") or pair.get("pair_address")
        if isinstance(pair_addr, str):
            payload_token["pair_address"] = pair_addr

        url = pair.get("url")
        if isinstance(url, str):
            payload_token["dex_pair_url"] = url

        quote = pair.get("quoteToken") or pair.get("quote_token")
        if isinstance(quote, dict):
            payload_token["quote_token"] = {
                "address": quote.get("address") or quote.get("mint"),
                "symbol": quote.get("symbol"),
                "name": quote.get("name"),
            }

        _merge_candidate_entry(tokens, payload_token, "dexscreener")

    return list(tokens.values())


async def _fetch_meteora_tokens(
    *, session: aiohttp.ClientSession | None = None
) -> List[TokenEntry]:
    if not _ENABLE_METEORA or not _METEORA_POOLS_URL:
        return []

    try:
        payload = await _http_get_json(
            _METEORA_POOLS_URL,
            headers={"accept": "application/json"},
            timeout=_METEORA_TIMEOUT,
            session=session,
        )
    except Exception as exc:
        logger.debug("Meteora discovery request failed: %s", exc)
        return []

    if isinstance(payload, dict):
        pools = (
            payload.get("pools")
            or payload.get("data")
            or payload.get("results")
            or payload.get("items")
            or payload.get("latestPools")
        )
    else:
        pools = payload

    if not isinstance(pools, list):
        return []

    tokens: Dict[str, Dict[str, Any]] = {}

    for pool in pools:
        if not isinstance(pool, dict):
            continue

        mint = (
            pool.get("tokenMint")
            or pool.get("token_mint")
            or pool.get("baseMint")
            or pool.get("mint")
            or pool.get("lpMint")
        )
        if isinstance(mint, dict):
            mint = mint.get("address") or mint.get("mint")

        if not isinstance(mint, str):
            mint = str(mint) if mint is not None else ""

        if not mint or not is_valid_solana_mint(mint):
            continue

        liquidity_raw = (
            pool.get("liquidity")
            or pool.get("liquidityUsd")
            or pool.get("tvl")
            or pool.get("liquidity_usd")
        )
        if isinstance(liquidity_raw, dict):
            liquidity = _coerce_numeric(
                liquidity_raw.get("usd")
                or liquidity_raw.get("usdValue")
                or liquidity_raw.get("value")
            )
        else:
            liquidity = _coerce_numeric(liquidity_raw)

        volume_raw = (
            pool.get("volume24h")
            or pool.get("volume_24h")
            or pool.get("volume")
            or pool.get("volumeUsd")
        )
        if isinstance(volume_raw, dict):
            volume = _coerce_numeric(
                volume_raw.get("usd")
                or volume_raw.get("usdValue")
                or volume_raw.get("value")
            )
        else:
            volume = _coerce_numeric(volume_raw)

        created = (
            pool.get("createdAt")
            or pool.get("created_at")
            or pool.get("created_at_ts")
            or pool.get("timestamp")
        )

        payload_token: Dict[str, Any] = {
            "address": mint,
            "symbol": pool.get("tokenSymbol") or pool.get("symbol"),
            "name": pool.get("tokenName") or pool.get("name"),
            "liquidity": liquidity,
            "volume": volume,
            "price": _coerce_numeric(
                pool.get("price")
                or pool.get("priceUsd")
                or pool.get("price_usd")
            ),
            "price_change": _coerce_numeric(
                pool.get("priceChange") or pool.get("price_change")
            ),
            "discovered_at": _parse_timestamp(created),
        }

        pool_addr = pool.get("poolAddress") or pool.get("id") or pool.get("address")
        if isinstance(pool_addr, str):
            payload_token["pool_address"] = pool_addr

        _merge_candidate_entry(tokens, payload_token, "meteora")

    return list(tokens.values())


async def _fetch_dexlab_tokens(
    *, session: aiohttp.ClientSession | None = None
) -> List[TokenEntry]:
    if not _ENABLE_DEXLAB or not _DEXLAB_LIST_URL:
        return []

    try:
        payload = await _http_get_json(
            _DEXLAB_LIST_URL,
            headers={"accept": "application/json"},
            timeout=_DEXLAB_TIMEOUT,
            session=session,
        )
    except Exception as exc:
        logger.debug("DexLab discovery request failed: %s", exc)
        return []

    if isinstance(payload, dict):
        items = (
            payload.get("data")
            or payload.get("list")
            or payload.get("tokens")
            or payload.get("results")
        )
    else:
        items = payload

    if not isinstance(items, list):
        return []

    tokens: Dict[str, Dict[str, Any]] = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        mint = (
            item.get("mint")
            or item.get("tokenMint")
            or item.get("token_address")
            or item.get("address")
        )
        if not isinstance(mint, str):
            mint = str(mint) if mint is not None else ""

        if not mint or not is_valid_solana_mint(mint):
            continue

        payload_token: Dict[str, Any] = {
            "address": mint,
            "symbol": item.get("symbol") or item.get("tokenSymbol"),
            "name": item.get("name") or item.get("tokenName"),
            "liquidity": _coerce_numeric(
                item.get("liquidity") or item.get("liquidityUsd")
            ),
            "volume": _coerce_numeric(
                item.get("volume")
                or item.get("volume24h")
                or item.get("volumeUsd")
            ),
            "discovered_at": _parse_timestamp(
                item.get("createdAt")
                or item.get("created_at")
                or item.get("launchDate")
            ),
            "verified": item.get("isVerified") or item.get("verified"),
        }

        decimals = item.get("decimals")
        if decimals is not None:
            try:
                payload_token["decimals"] = int(decimals)
            except Exception:
                pass

        _merge_candidate_entry(tokens, payload_token, "dexlab")

    return list(tokens.values())


def _apply_solscan_enrichment(
    candidates: Dict[str, Dict[str, Any]],
    address: str,
    payload: Any,
) -> None:
    data = payload
    if isinstance(payload, dict):
        data = payload.get("data") or payload.get("token") or payload
    if not isinstance(data, dict):
        return

    entry = candidates.get(address)
    if entry is None:
        return

    name = data.get("name") or data.get("symbolName")
    symbol = data.get("symbol")
    decimals = data.get("decimals")
    supply = data.get("supply") or data.get("totalSupply")
    holders = data.get("holder") or data.get("holders")
    verified = data.get("verified")

    if symbol and not entry.get("symbol"):
        entry["symbol"] = str(symbol)
    if name and (not entry.get("name") or entry.get("name") == address):
        entry["name"] = str(name)
    if decimals is not None:
        try:
            entry["decimals"] = int(decimals)
        except Exception:
            pass
    if supply is not None:
        entry["supply"] = _coerce_numeric(supply)
    if holders is not None:
        entry["holders"] = _coerce_numeric(holders)
    if isinstance(verified, bool):
        entry["verified"] = verified

    entry.setdefault("sources", set()).add("solscan")


async def _enrich_with_solscan(
    candidates: Dict[str, Dict[str, Any]],
    *,
    addresses: Iterable[str] | None = None,
) -> None:
    if (
        not _ENABLE_SOLSCAN
        or not _SOLSCAN_META_URL
        or _SOLSCAN_ENRICH_LIMIT <= 0
        or not candidates
    ):
        return

    allowed = set(addresses) if addresses is not None else None
    pending: List[str] = []
    for addr, entry in candidates.items():
        if allowed is not None and addr not in allowed:
            continue
        needs_symbol = not entry.get("symbol")
        needs_name = not entry.get("name") or entry.get("name") == addr
        needs_decimals = "decimals" not in entry
        if _SOLSCAN_NEGATIVE_CACHE.get(addr):
            continue
        if needs_symbol or needs_name or needs_decimals:
            pending.append(addr)
        if len(pending) >= _SOLSCAN_ENRICH_LIMIT:
            break

    if not pending:
        return

    headers = {"accept": "application/json"}
    if _SOLSCAN_API_KEY:
        headers["token"] = _SOLSCAN_API_KEY

    timeout = _make_timeout(_SOLSCAN_TIMEOUT)
    session = await get_session(timeout=timeout)

    concurrency = max(1, min(4, _SOLSCAN_ENRICH_LIMIT))
    semaphore = asyncio.Semaphore(concurrency)

    async def _fetch_and_apply(address: str) -> None:
        params = {"tokenAddress": address, "address": address}
        async with semaphore:
            try:
                payload = await _http_get_json(
                    _SOLSCAN_META_URL,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    session=session,
                )
            except HostCircuitOpenError:
                logger.debug("Solscan circuit open; skipping %s", address)
                return
            except aiohttp.ClientResponseError as exc:
                if exc.status == 404:
                    _SOLSCAN_NEGATIVE_CACHE.set(address, True)
                logger.debug("Solscan metadata unavailable for %s: %s", address, exc)
                return
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.debug("Solscan metadata fetch failed for %s: %s", address, exc)
                return
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Solscan metadata unexpected error for %s: %s", address, exc)
                return

        try:
            _apply_solscan_enrichment(candidates, address, payload)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "Solscan metadata processing failed for %s: %s", address, exc
            )
    tasks = [_fetch_and_apply(addr) for addr in pending]
    if tasks:
        await asyncio.gather(*tasks)

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


class _DiscoveryResult:
    """Wrapper that is both awaitable and async iterable."""

    def __init__(self, agen: AsyncIterator[List[TokenEntry]]) -> None:
        self._agen = agen
        self._final: List[TokenEntry] | None = None
        self._consumed = False

    def __aiter__(self) -> AsyncIterator[List[TokenEntry]]:
        async def _iterate() -> AsyncIterator[List[TokenEntry]]:
            if self._consumed:
                return
            self._consumed = True
            async for batch in self._agen:
                self._final = batch
                yield batch

        return _iterate()

    def __await__(self):
        return self._consume().__await__()

    async def _consume(self) -> List[TokenEntry]:
        if not self._consumed:
            self._consumed = True
            final: List[TokenEntry] | None = None
            async for batch in self._agen:
                final = batch
            self._final = final
        return list(self._final or [])


def discover_candidates(
    rpc_url: str,
    *,
    limit: int | None = None,
    mempool_threshold: float | None = None,
) -> _DiscoveryResult:
    """Combine BirdEye numeric candidates with mempool signals and rank."""

    if limit is None or limit <= 0:
        limit = _MAX_TOKENS
    if mempool_threshold is None:
        mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)

    shared_http_sources = any(
        (
            _ENABLE_DEXSCREENER and _DEXSCREENER_URL,
            _ENABLE_METEORA and _METEORA_POOLS_URL,
            _ENABLE_DEXLAB and _DEXLAB_LIST_URL,
        )
    )

    def _score_candidates(
        candidates: Dict[str, Dict[str, Any]],
        mempool: Dict[str, Dict[str, float]],
    ) -> None:
        for addr, entry in candidates.items():
            entry.setdefault("sources", set())
            mp = mempool.get(addr)
            features = _compute_feature_vector(entry, mp)
            z = float(_SCORING_BIAS)
            breakdown: List[Dict[str, float]] = []
            for name, value in features.items():
                weight = float(_SCORING_WEIGHTS.get(name, 0.0))
                contribution = weight * value
                breakdown.append(
                    {
                        "name": name,
                        "value": float(value),
                        "weight": weight,
                        "contribution": float(contribution),
                    }
                )
                z += contribution
            score = _sigmoid(z)
            entry["score"] = float(score)
            entry["score_features"] = features
            entry["score_breakdown"] = breakdown
            top = sorted(breakdown, key=lambda item: abs(item["contribution"]), reverse=True)[:3]
            entry["top_features"] = top
            for legacy in ("score_liq", "score_vol", "score_mp", "score_mult"):
                entry.pop(legacy, None)
            source_count = _source_count(entry)
            entry["_stage_b_eligible"] = bool(
                score >= _STAGE_B_SCORE_THRESHOLD or source_count >= _STAGE_B_MIN_SOURCES
            )

    def _snapshot(
        candidates: Dict[str, Dict[str, Any]],
        *,
        limit: int,
    ) -> List[TokenEntry]:
        ordered = sorted(
            candidates.values(),
            key=lambda c: (c.get("score", 0.0), c.get("address", "")),
            reverse=True,
        )

        final: List[TokenEntry] = []
        for entry in ordered[:limit]:
            sources = entry.get("sources", [])
            if isinstance(sources, set):
                src_list = sorted(sources)
            else:
                src_list = sorted(list(sources or []))
            copy = dict(entry)
            copy["sources"] = src_list
            for internal in ("_stage_b_eligible", "score_breakdown"):
                copy.pop(internal, None)
            final.append(copy)
        return final

    async def _generator() -> AsyncIterator[List[TokenEntry]]:
        shared_session_obj: aiohttp.ClientSession | None = None
        if shared_http_sources:
            try:
                shared_session_obj = await get_session()
            except TypeError:
                shared_session_obj = await get_session()

        async def _run(
            shared_session: aiohttp.ClientSession | None,
        ) -> AsyncIterator[List[TokenEntry]]:
            bird_task = asyncio.create_task(_fetch_birdeye_tokens())
            mempool_task = (
                asyncio.create_task(
                    _collect_mempool_signals(rpc_url, mempool_threshold)
                )
                if _ENABLE_MEMPOOL and rpc_url
                else None
            )
            if _ENABLE_MEMPOOL and rpc_url:
                logger.debug("Discovery mempool threshold=%.3f", mempool_threshold)

            task_map: Dict[asyncio.Task[Any], str] = {bird_task: "bird"}
            if mempool_task is not None:
                task_map[mempool_task] = "mempool"
            if _ENABLE_DEXSCREENER and _DEXSCREENER_URL:
                task_map[
                    asyncio.create_task(
                        _fetch_dexscreener_tokens(session=shared_session)
                    )
                ] = "dexscreener"
            if _ENABLE_METEORA and _METEORA_POOLS_URL:
                task_map[
                    asyncio.create_task(
                        _fetch_meteora_tokens(session=shared_session)
                    )
                ] = "meteora"
            if _ENABLE_DEXLAB and _DEXLAB_LIST_URL:
                task_map[
                    asyncio.create_task(
                        _fetch_dexlab_tokens(session=shared_session)
                    )
                ] = "dexlab"

            merge_locks: Dict[str, asyncio.Lock] = {
                label: asyncio.Lock() for label in set(task_map.values())
            }

            candidates: Dict[str, Dict[str, Any]] = {}
            mempool: Dict[str, Dict[str, float]] = {}

            bird_tokens: List[TokenEntry] = []
            dexscreener_tokens: List[TokenEntry] = []
            meteora_tokens: List[TokenEntry] = []
            dexlab_tokens: List[TokenEntry] = []

            last_snapshot: List[TokenEntry] | None = None
            emitted = False

            overall_timeout_raw = os.getenv("DISCOVERY_OVERALL_TIMEOUT", "0")
            try:
                overall_timeout = float(overall_timeout_raw or 0.0)
            except Exception:
                overall_timeout = 0.0

            completed: set[asyncio.Task[Any]] = set()

            async def _merge_result(label: str, result: Any) -> bool:
                nonlocal mempool, bird_tokens, dexscreener_tokens, meteora_tokens, dexlab_tokens
                lock = merge_locks[label]
                async with lock:
                    changed = False
                    if label == "bird":
                        if isinstance(result, Exception):
                            logger.warning("BirdEye discovery failed: %s", result)
                            return False
                        bird_tokens = list(result or [])
                        for token in bird_tokens:
                            entry = _merge_candidate_entry(candidates, dict(token), "birdeye")
                            if entry is not None:
                                changed = True
                        return changed
                    if label == "mempool":
                        if isinstance(result, Exception):
                            logger.debug("Mempool signals unavailable: %s", result)
                            return False
                        mempool = dict(result or {})
                        changed = bool(mempool)
                        for addr, mp in mempool.items():
                            mp_token: Dict[str, Any] = {
                                "address": str(addr),
                                "symbol": mp.get("symbol"),
                                "name": mp.get("name") or str(addr),
                                "liquidity": mp.get("liquidity"),
                                "volume": mp.get("volume"),
                                "price": mp.get("price"),
                            }
                            entry = _merge_candidate_entry(
                                candidates, mp_token, "mempool"
                            )
                            if entry is None:
                                continue
                            changed = True
                            for key in (
                                "score",
                                "momentum",
                                "anomaly",
                                "wallet_concentration",
                                "avg_swap_size",
                            ):
                                val = mp.get(key)
                                if val is not None:
                                    try:
                                        entry[key] = float(val)
                                    except Exception:
                                        pass
                        return changed
                    if label == "dexscreener":
                        if isinstance(result, Exception):
                            logger.debug(
                                "DexScreener discovery unavailable: %s", result
                            )
                            return False
                        dexscreener_tokens = list(result or [])
                        for token in dexscreener_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "dexscreener"
                            )
                            if entry is not None:
                                changed = True
                        return changed
                    if label == "meteora":
                        if isinstance(result, Exception):
                            logger.debug("Meteora discovery unavailable: %s", result)
                            return False
                        meteora_tokens = list(result or [])
                        for token in meteora_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "meteora"
                            )
                            if entry is not None:
                                changed = True
                        return changed
                    if label == "dexlab":
                        if isinstance(result, Exception):
                            logger.debug("DexLab discovery unavailable: %s", result)
                            return False
                        dexlab_tokens = list(result or [])
                        for token in dexlab_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "dexlab"
                            )
                            if entry is not None:
                                changed = True
                        return changed
                    return False

            task_list = list(task_map.keys())

            try:
                iterator = asyncio.as_completed(
                    task_list,
                    timeout=overall_timeout if overall_timeout > 0 else None,
                )
                for fut in iterator:
                    label = task_map[fut]
                    try:
                        result = await fut
                    except Exception as exc:
                        result = exc
                    completed.add(fut)
                    changed = await _merge_result(label, result)
                    if not candidates or not changed:
                        continue
                    _score_candidates(candidates, mempool)
                    snapshot = _snapshot(candidates, limit=limit)
                    last_snapshot = snapshot
                    emitted = True
                    yield snapshot
            except asyncio.TimeoutError:
                logger.warning(
                    "Discovery overall timeout after %.2fs; pending_tasks=%s",
                    overall_timeout,
                    len(task_list) - len(completed),
                )
            finally:
                pending = [task for task in task_list if task not in completed]
                for task in pending:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

            if candidates:
                _score_candidates(candidates, mempool)
            stage_b_candidates = [
                addr for addr, entry in candidates.items() if entry.get("_stage_b_eligible")
            ]
            try:
                if stage_b_candidates:
                    await _enrich_with_solscan(candidates, addresses=stage_b_candidates)
            except Exception as exc:
                logger.debug("Solscan enrichment unavailable: %s", exc)

            if candidates:
                _score_candidates(candidates, mempool)
            final = _snapshot(candidates, limit=limit)

            top_score = final[0]["score"] if final and "score" in final[0] else None
            logger.debug(
                "Discovery combine summary bird=%s mempool=%s dexscreener=%s meteora=%s dexlab=%s final=%s top_score=%s",
                len(bird_tokens),
                len(mempool),
                len(dexscreener_tokens),
                len(meteora_tokens),
                len(dexlab_tokens),
                len(final),
                f"{top_score:.4f}" if isinstance(top_score, (int, float)) else "n/a",
            )

            if not emitted or final != last_snapshot:
                yield final

        if shared_session_obj is not None:
            async with shared_session_obj as shared_session:
                async for batch in _run(shared_session):
                    yield batch
        else:
            async for batch in _run(None):
                yield batch

    return _DiscoveryResult(_generator())


def warm_cache(rpc_url: str, *, limit: int | None = None) -> None:
    """Prime the discovery cache synchronously (best-effort)."""
    api_key = _resolve_birdeye_api_key()
    if not (rpc_url or api_key):
        return

    limit = limit or min(_MAX_TOKENS, 10)
    mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)

    def _worker() -> None:
        try:
            async def _consume() -> None:
                async for _ in discover_candidates(
                    rpc_url, limit=limit, mempool_threshold=mempool_threshold
                ):
                    pass

            asyncio.run(asyncio.wait_for(_consume(), timeout=_WARM_TIMEOUT))
        except Exception as exc:
            logger.debug("Discovery warm cache failed: %s", exc)

    thread = threading.Thread(target=_worker, name="discovery-warm", daemon=True)
    thread.start()
