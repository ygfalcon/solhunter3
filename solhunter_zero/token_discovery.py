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
import inspect
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence, AsyncIterator, Tuple
from urllib.parse import urlparse

import yaml
from aiohttp import ClientTimeout
import aiohttp

from .http import HostCircuitOpenError, host_request, host_retry_config
from .http import get_session as _shared_http_session
from . import onchain_metrics
from .scanner_common import DEFAULT_BIRDEYE_API_KEY
from .providers import orca as orca_provider
from .providers import raydium as raydium_provider
from .lru import TTLCache
from .mempool_scanner import stream_ranked_mempool_tokens_with_depth
from .util.mints import is_valid_solana_mint

logger = logging.getLogger(__name__)


def _is_fast_mode() -> bool:
    return os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: str, *, fast_default: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        if fast_default is not None and _is_fast_mode():
            return float(fast_default)
        raw = default
    try:
        value = float(raw)
    except Exception:
        value = float(default)
    return value


def _env_int(
    name: str,
    default: str,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        raw = default
    try:
        value = int(raw)
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_bool(name: str, default: str = "0") -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        raw = default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_str(name: str, default: str = "", *, strip: bool = True) -> str:
    raw = os.getenv(name)
    if raw is None or raw == "":
        raw = default
    if strip:
        return str(raw).strip()
    return str(raw)


class _DiscoverySettings:
    __slots__ = ()

    @property
    def min_volume(self) -> float:
        return _env_float("DISCOVERY_MIN_VOLUME_USD", "50000", fast_default=0.0)

    @property
    def min_liquidity(self) -> float:
        return _env_float("DISCOVERY_MIN_LIQUIDITY_USD", "75000", fast_default=0.0)

    @property
    def max_tokens(self) -> int:
        return _env_int("DISCOVERY_MAX_TOKENS", "50", minimum=1)

    @property
    def page_limit(self) -> int:
        return _env_int("DISCOVERY_PAGE_SIZE", "25", minimum=1, maximum=50)

    @property
    def overfetch_factor(self) -> float:
        return _env_float("DISCOVERY_OVERFETCH_FACTOR", "0.8")

    @property
    def cache_ttl(self) -> float:
        return _env_float("DISCOVERY_CACHE_TTL", "45")

    @property
    def max_offset(self) -> int:
        return _env_int("DISCOVERY_MAX_OFFSET", "4000", minimum=0)

    @property
    def mempool_limit(self) -> int:
        return _env_int("DISCOVERY_MEMPOOL_LIMIT", "12", minimum=0)

    @property
    def enable_mempool(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_MEMPOOL", "1")

    @property
    def warm_timeout(self) -> float:
        return _env_float("DISCOVERY_WARM_TIMEOUT", "5")

    @property
    def birdeye_retries(self) -> int:
        return _env_int("DISCOVERY_BIRDEYE_RETRIES", "3", minimum=1)

    @property
    def birdeye_backoff(self) -> float:
        return _env_float("DISCOVERY_BIRDEYE_BACKOFF", "1.0")

    @property
    def birdeye_backoff_max(self) -> float:
        return _env_float("DISCOVERY_BIRDEYE_BACKOFF_MAX", "8.0")

    @property
    def enable_dexscreener(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_DEXSCREENER", "1")

    @property
    def dexscreener_url(self) -> str:
        value = _env_str(
            "DEXSCREENER_TOKENS_URL",
            "https://api.dexscreener.com/latest/dex/tokens?chainId=solana",
        )
        return value

    @property
    def dexscreener_timeout(self) -> float:
        return _env_float("DEXSCREENER_TIMEOUT", "8.0")

    @property
    def dexscreener_max_age(self) -> float:
        return _env_float("DEXSCREENER_MAX_AGE_SECONDS", "3600")

    @property
    def enable_raydium(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_RAYDIUM", "1")

    @property
    def raydium_timeout(self) -> float:
        return _env_float("RAYDIUM_TIMEOUT", "2.0")

    @property
    def enable_meteora(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_METEORA", "1")

    @property
    def meteora_pools_url(self) -> str:
        value = _env_str("METEORA_POOLS_URL") or _env_str("METEORA_DISCOVERY_URL")
        if value:
            return value
        return "https://dlmm-api.meteora.ag/api/pools/latest"

    @property
    def meteora_timeout(self) -> float:
        return _env_float("METEORA_TIMEOUT", "8.0")

    @property
    def enable_dexlab(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_DEXLAB", "1")

    @property
    def dexlab_list_url(self) -> str:
        return _env_str("DEXLAB_LIST_URL", "https://api.dexlab.space/v1/token/list")

    @property
    def dexlab_timeout(self) -> float:
        return _env_float("DEXLAB_TIMEOUT", "8.0")

    @property
    def enable_solscan(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_SOLSCAN", "1")

    @property
    def solscan_meta_url(self) -> str:
        value = _env_str("DISCOVERY_SOLSCAN_META_URL")
        if value:
            return value
        value = _env_str("SOLSCAN_DISCOVERY_URL")
        if value:
            return value
        return "https://public-api.solscan.io/token/meta"

    @property
    def solscan_api_key(self) -> str:
        return _env_str("SOLSCAN_API_KEY")

    @property
    def solscan_timeout(self) -> float:
        return _env_float("DISCOVERY_SOLSCAN_TIMEOUT", "6.0")

    @property
    def solscan_enrich_limit(self) -> int:
        return max(0, _env_int("DISCOVERY_SOLSCAN_LIMIT", "8", minimum=0))

    @property
    def enable_orca(self) -> bool:
        return _env_bool("DISCOVERY_ENABLE_ORCA", "1")

    @property
    def orca_timeout(self) -> float:
        return _env_float("ORCA_TIMEOUT", "2.0")

    @property
    def orca_catalog_ttl(self) -> float:
        return _env_float("ORCA_CATALOG_TTL", "600")

    @property
    def stage_b_score_threshold(self) -> float:
        return _env_float("DISCOVERY_STAGE_B_THRESHOLD", "0.65")

    @property
    def stage_b_min_sources(self) -> int:
        return _env_int("DISCOVERY_STAGE_B_MIN_SOURCES", "2", minimum=0)

    @property
    def solscan_negative_ttl(self) -> float:
        return _env_float("SOLSCAN_NEGATIVE_TTL", "1800")

    @property
    def trending_min_liquidity(self) -> float:
        return _env_float("TRENDING_MIN_LIQUIDITY_USD", "7500")


SETTINGS = _DiscoverySettings()


_TRENDING_MIN_LIQUIDITY = float(SETTINGS.trending_min_liquidity)


def _birdeye_tokenlist_url() -> str:
    value = _env_str(
        "BIRDEYE_TOKENLIST_URL", "https://api.birdeye.so/defi/tokenlist"
    )
    if not value:
        return "https://api.birdeye.so/defi/tokenlist"
    return value


_BIRDEYE_THROTTLE_MARKERS = (
    "compute units usage limit exceeded",
    "request limit exceeded",
    "rate limit exceeded",
    "too many requests",
    "throttle",
)

TokenEntry = Dict[str, Any]


_OPTIONAL_SOURCE_META: Dict[str, Tuple[str, str]] = {
    "bird": ("birdeye", "BirdEye"),
    "mempool": ("mempool", "Mempool"),
    "trending": ("trending", "Trending feed"),
    "dexscreener": ("dexscreener", "DexScreener"),
    "raydium": ("raydium", "Raydium"),
    "meteora": ("meteora", "Meteora"),
    "dexlab": ("dexlab", "DexLab"),
}


class DiscoveryConfigurationError(RuntimeError):
    """Raised when discovery prerequisites are misconfigured."""

    def __init__(self, source: str, message: str, *, remediation: str | None = None):
        super().__init__(message)
        self.source = source
        self.remediation = remediation

    def __str__(self) -> str:  # pragma: no cover - repr helper
        base = super().__str__()
        if self.remediation:
            return f"{base} (source={self.source}, remediation={self.remediation})"
        return f"{base} (source={self.source})"


_BIRDEYE_CACHE: TTLCache[str, List[TokenEntry]] = TTLCache(maxsize=1, ttl=SETTINGS.cache_ttl)
_CACHE_LOCK = Lock()
_BIRDEYE_DISABLED_INFO = False

_ORCA_CATALOG_CACHE: tuple[
    float, Dict[str, List[Dict[str, Any]]], float
] = (0.0, {}, 0.0)
_ORCA_CATALOG_LOCK: asyncio.Lock | None = None


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


def refresh_runtime_values() -> None:
    """Synchronise cached discovery state with the current environment."""

    global _SCORING_BIAS, _SCORING_WEIGHTS, _BIRDEYE_DISABLED_INFO, _ORCA_CATALOG_CACHE
    global _TRENDING_MIN_LIQUIDITY

    _SCORING_BIAS, _SCORING_WEIGHTS = _load_scoring_weights()

    try:
        _TRENDING_MIN_LIQUIDITY = float(SETTINGS.trending_min_liquidity)
    except Exception:
        _TRENDING_MIN_LIQUIDITY = 0.0

    try:
        _BIRDEYE_CACHE.ttl = float(SETTINGS.cache_ttl)
    except Exception:
        pass
    _cache_clear()

    try:
        _SOLSCAN_NEGATIVE_CACHE.ttl = float(SETTINGS.solscan_negative_ttl)
    except Exception:
        pass
    _SOLSCAN_NEGATIVE_CACHE.clear()

    _ORCA_CATALOG_CACHE = (0.0, {}, 0.0)
    _BIRDEYE_DISABLED_INFO = False


def get_scoring_context() -> Tuple[float, Dict[str, float]]:
    """Return the active scoring bias and weight mapping."""

    return float(_SCORING_BIAS), dict(_SCORING_WEIGHTS)


def compute_score_from_features(
    features: Mapping[str, Any],
    *,
    bias: float | None = None,
    weights: Mapping[str, float] | None = None,
) -> Tuple[float, List[Dict[str, float]], List[Dict[str, float]]]:
    """Recompute discovery score details for ``features``.

    The returned tuple consists of ``(score, breakdown, top_features)`` where the
    breakdown enumerates each feature contribution using the supplied scoring
    weights and bias.
    """

    if bias is None:
        bias = float(_SCORING_BIAS)
    if weights is None:
        weights_map: Mapping[str, float] = _SCORING_WEIGHTS
    else:
        weights_map = weights

    z = float(bias)
    breakdown: List[Dict[str, float]] = []

    for name, raw_value in features.items():
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue

        weight = float(weights_map.get(name, 0.0))
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
    top_features = sorted(
        breakdown,
        key=lambda item: abs(item.get("contribution", 0.0)),
        reverse=True,
    )[:3]

    return float(score), breakdown, top_features
_SOLSCAN_NEGATIVE_CACHE: TTLCache[str, bool] = TTLCache(maxsize=2048, ttl=SETTINGS.solscan_negative_ttl)

_ETAG_HOSTS = {
    "api.dexscreener.com",
    "public-api.birdeye.so",
    "api.meteora.ag",
    "api.dexlab.space",
}


try:  # Optional during bootstrap when config is not yet initialised
    from .config import register_runtime_init_hook

    register_runtime_init_hook(refresh_runtime_values)
except Exception:  # pragma: no cover - defensive during partial imports
    pass

_STATIC_CANDIDATE_SEEDS: Sequence[tuple[str, str, str]] = (
    (
        "So11111111111111111111111111111111111111112",
        "SOL",
        "Solana",
    ),
    (
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "USDC",
        "USD Coin",
    ),
    (
        "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "BONK",
        "Bonk",
    ),
    (
        "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
        "JUP",
        "Jupiter",
    ),
)


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


async def _get_orca_catalog_lock() -> asyncio.Lock:
    global _ORCA_CATALOG_LOCK
    if _ORCA_CATALOG_LOCK is None:
        _ORCA_CATALOG_LOCK = asyncio.Lock()
    return _ORCA_CATALOG_LOCK


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


def _fallback_candidate_tokens(limit: int) -> List[TokenEntry]:
    """Return cached BirdEye tokens or static seeds for configuration fallbacks."""

    seen: set[str] = set()
    fallback: List[TokenEntry] = []

    cache_key = _current_cache_key(limit)
    cached = _cache_get(cache_key) or []
    for item in cached:
        if not isinstance(item, Mapping):
            continue
        address = str(item.get("address") or "").strip()
        if not address or address in seen:
            continue
        entry = dict(item)
        entry["sources"] = _normalize_string_collection(entry.get("sources"))
        entry["source_categories"] = _normalize_string_collection(
            entry.get("source_categories")
        )
        _register_source(entry, "cache")
        entry.setdefault("score", 0.0)
        entry.setdefault("_stage_b_eligible", False)
        fallback.append(entry)
        seen.add(address)
        if len(fallback) >= limit:
            return fallback

    for address, symbol, name in _STATIC_CANDIDATE_SEEDS:
        if address in seen:
            continue
        entry: TokenEntry = {
            "address": address,
            "symbol": symbol,
            "name": name,
            "sources": {"static"},
            "source_categories": {"fallback"},
            "score": 0.0,
            "_stage_b_eligible": False,
        }
        fallback.append(entry)
        seen.add(address)
        if len(fallback) >= limit:
            break

    return fallback


async def _load_orca_catalog(
    *, session: aiohttp.ClientSession | None = None
) -> Dict[str, List[Dict[str, Any]]]:
    if not SETTINGS.enable_orca:
        return {}
    ttl = max(60.0, float(SETTINGS.orca_catalog_ttl))
    lock = await _get_orca_catalog_lock()
    async with lock:
        global _ORCA_CATALOG_CACHE
        expires, cached, failed_at = _ORCA_CATALOG_CACHE
        now = time.monotonic()
        if cached and expires > now:
            return cached
        try:
            payload = await orca_provider.fetch(
                None,
                timeout=SETTINGS.orca_timeout,
                session=session,
            )
        except Exception as exc:
            logger.debug("Orca catalog fetch failed: %s", exc)
            _ORCA_CATALOG_CACHE = (0.0, {}, now)
            return cached if cached else {}
        catalog_data = payload.get("catalog") if isinstance(payload, Mapping) else None
        normalized: Dict[str, List[Dict[str, Any]]] = {}
        if isinstance(catalog_data, Mapping):
            for mint, pools in catalog_data.items():
                if not isinstance(mint, str):
                    continue
                if isinstance(pools, Sequence):
                    pool_entries: List[Dict[str, Any]] = []
                    for pool in pools:
                        if isinstance(pool, Mapping):
                            pool_entries.append(dict(pool))
                    if pool_entries:
                        normalized[mint] = pool_entries
        if normalized:
            _ORCA_CATALOG_CACHE = (time.monotonic() + ttl, normalized, 0.0)
            return normalized
        _ORCA_CATALOG_CACHE = (0.0, {}, failed_at)
        return cached if cached else {}


def _cache_clear() -> None:
    with _CACHE_LOCK:
        _BIRDEYE_CACHE.clear()


def _normalize_limit(limit: int | None) -> int:
    max_tokens = int(SETTINGS.max_tokens)
    if limit is None:
        numeric = max_tokens
    else:
        try:
            numeric = int(limit)
        except (TypeError, ValueError):
            numeric = max_tokens

    if numeric <= 0:
        numeric = max_tokens

    if max_tokens > 0:
        numeric = min(numeric, max_tokens)

    return numeric


def _current_cache_key(limit: int | None = None) -> str:
    normalized_limit = _normalize_limit(limit)
    return (
        "tokens:"
        f"{int(SETTINGS.min_volume)}:"
        f"{int(SETTINGS.min_liquidity)}:"
        f"{SETTINGS.page_limit}:"
        f"{int(SETTINGS.max_tokens)}:"
        f"{normalized_limit}"
    )


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


async def fetch_trending_tokens_async(limit: int | None = None) -> List[str]:
    """Return trending token mint addresses discovered by the discovery module."""

    try:
        from . import discovery as _discovery_mod
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("Discovery module unavailable for trending fetch", exc_info=True)
        return []

    try:
        return await _discovery_mod.fetch_trending_tokens_async(limit=limit)
    except Exception:  # pragma: no cover - defensive fallback
        logger.debug("Trending token fetch failed", exc_info=True)
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


def _has_core_metrics(
    entry: Mapping[str, Any], mempool: Mapping[str, float] | None = None
) -> bool:
    """Return ``True`` if ``entry`` already exposes actionable metrics."""

    liquidity = _coerce_numeric(entry.get("liquidity"))
    volume = _coerce_numeric(entry.get("volume"))
    price = _coerce_numeric(entry.get("price"))
    if any(value > 0 for value in (liquidity, volume, price)):
        return True
    if mempool:
        mempool_score = _coerce_numeric(mempool.get("score"))
        if mempool_score > 0:
            return True
    return False


def _stage_b_required_sources(
    entry: Mapping[str, Any], mempool: Mapping[str, float] | None = None
) -> int:
    """Return the minimum sources needed before Stage-B enrichment."""

    configured = max(1, int(SETTINGS.stage_b_min_sources))
    if _has_core_metrics(entry, mempool):
        return 1
    return max(2, configured)


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
        mempool_pressure = _coerce_numeric(mempool.get("score"))
    if mempool_pressure <= 0.0:
        mempool_pressure = _coerce_numeric(
            entry.get("score_mp") or entry.get("mempool_score")
        )
    asof = entry.get("asof") or entry.get("updated_at") or discovered_at
    staleness_ms = 0.0
    if asof:
        try:
            staleness_ms = max(0.0, (now - float(asof)) * 1000.0)
        except Exception:
            staleness_ms = 0.0
    features = {
        "liquidity_usd": float(liquidity_feature),
        "vol_1h_z": float(vol_feature),
        "pool_age_min": float(age_minutes),
        "source_diversity": float(source_diversity),
        "oracle_present": float(oracle_present),
        "sellable": float(sellable),
        "mempool_pressure": float(max(0.0, mempool_pressure)),
        "staleness_ms": float(staleness_ms),
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
        logger.debug(
            "Failed to coerce numeric value %r; defaulting to 0.0", value
        )
        return 0.0
    if math.isnan(numeric):
        logger.debug(
            "Coerced NaN from value %r; defaulting to 0.0", value
        )
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


def _normalize_venues_field(value: Any) -> set[str]:
    venues: set[str] = set()
    if isinstance(value, str):
        candidate = value.strip().lower()
        if candidate:
            venues.add(candidate)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            if isinstance(item, str):
                candidate = item.strip().lower()
                if candidate:
                    venues.add(candidate)
    return venues


def _normalize_string_collection(value: Any) -> set[str]:
    if isinstance(value, set):
        iterable = value
    elif isinstance(value, (list, tuple)):
        iterable = value
    elif isinstance(value, str):
        iterable = [value]
    else:
        return set()
    normalized: set[str] = set()
    for item in iterable:
        if isinstance(item, str):
            candidate = item.strip()
        else:
            candidate = str(item).strip()
        if candidate:
            normalized.add(candidate)
    return normalized


_SOURCE_CATEGORY_MAP: Dict[str, str] = {
    "birdeye": "market_data",
    "birdeye_overview": "market_data",
    "dexscreener": "market_data",
    "meteora": "market_data",
    "dexlab": "market_data",
    "raydium": "market_data",
    "dex_metrics": "market_data",
    "pumpfun": "trending_signal",
    "mempool": "mempool_signal",
    "trending": "trending_signal",
    "social": "social_signal",
    "solscan": "metadata",
    "helius_search": "metadata",
    "onchain": "onchain_metrics",
    "onchain_fallback": "fallback",
    "cache": "fallback",
    "static": "fallback",
    "fallback": "fallback",
}


def _source_category_for(label: str) -> str | None:
    key = str(label).strip().lower()
    if not key:
        return None
    return _SOURCE_CATEGORY_MAP.get(key, "other")


def _register_source(entry: Dict[str, Any], source: str) -> bool:
    if not source:
        return False
    normalized_source = str(source).strip()
    if not normalized_source:
        return False
    existing_sources = entry.get("sources")
    if isinstance(existing_sources, set):
        before = len(existing_sources)
        existing_sources.add(normalized_source)
        sources_changed = len(existing_sources) != before
    else:
        normalized = _normalize_string_collection(existing_sources)
        before = len(normalized)
        normalized.add(normalized_source)
        entry["sources"] = normalized
        sources_changed = len(normalized) != before

    category = _source_category_for(normalized_source)
    categories_changed = False
    if category:
        existing_categories = entry.get("source_categories")
        if isinstance(existing_categories, set):
            before_cat = len(existing_categories)
            existing_categories.add(category)
            categories_changed = len(existing_categories) != before_cat
        else:
            normalized_categories = _normalize_string_collection(existing_categories)
            before_cat = len(normalized_categories)
            normalized_categories.add(category)
            entry["source_categories"] = normalized_categories
            categories_changed = len(normalized_categories) != before_cat

    return sources_changed or categories_changed


def _merge_orca_venues(
    entry: Dict[str, Any], catalog: Mapping[str, Sequence[Mapping[str, Any]]]
) -> None:
    if not catalog or not isinstance(entry, dict):
        return
    mint = entry.get("address")
    if not isinstance(mint, str):
        return
    pools = catalog.get(mint)
    if not pools:
        return
    existing = entry.get("venues")
    if isinstance(existing, set):
        venues = existing
    elif isinstance(existing, list):
        venues = {str(v).strip().lower() for v in existing if isinstance(v, str)}
        entry["venues"] = venues
    else:
        venues = set()
        entry["venues"] = venues
    venues.add("orca")
    top_pool = None
    for pool in pools:
        if isinstance(pool, Mapping) and pool.get("pool"):
            top_pool = pool
            break
    if top_pool and "pair_address" not in entry:
        pool_addr = top_pool.get("pool")
        if isinstance(pool_addr, str) and pool_addr:
            entry["pair_address"] = pool_addr


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
    address = str(raw_address).strip()
    if not address:
        return None
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
    incoming_venues = _normalize_venues_field(token.get("venues"))
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
            "source_categories": set(),
            "venues": set(incoming_venues),
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
        venues_field = entry.get("venues")
        if isinstance(venues_field, list):
            entry["venues"] = {str(v).strip().lower() for v in venues_field if isinstance(v, str)}
        elif isinstance(venues_field, set):
            pass
        elif venues_field:
            entry["venues"] = _normalize_venues_field(venues_field)
        else:
            entry["venues"] = set()
        categories_field = entry.get("source_categories")
        if isinstance(categories_field, list):
            entry["source_categories"] = _normalize_string_collection(categories_field)
        elif isinstance(categories_field, set):
            pass
        elif categories_field:
            entry["source_categories"] = _normalize_string_collection(categories_field)
        else:
            entry["source_categories"] = set()
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
        if incoming_venues:
            venues_obj = entry.get("venues")
            if isinstance(venues_obj, set):
                venues_obj.update(incoming_venues)
            else:
                merged = _normalize_venues_field(venues_obj)
                merged.update(incoming_venues)
                entry["venues"] = merged
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

    entry.setdefault("sources", set())
    entry.setdefault("source_categories", set())
    _register_source(entry, source)
    return entry


def _needs_trending_enrichment(entry: Mapping[str, Any]) -> bool:
    sources = entry.get("sources")
    if isinstance(sources, set):
        source_set = sources
    elif isinstance(sources, (list, tuple)):
        source_set = set(str(src) for src in sources if isinstance(src, str))
    else:
        source_set = set()
    if not source_set:
        return False
    if any(src != "trending" for src in source_set):
        return False
    liquidity = _coerce_numeric(entry.get("liquidity"))
    volume = _coerce_numeric(entry.get("volume"))
    price = _coerce_numeric(entry.get("price"))
    return liquidity <= 0.0 or volume <= 0.0 or price <= 0.0


def _apply_trending_enrichment(entry: Dict[str, Any], payload: Mapping[str, Any]) -> bool:
    changed = False

    price = _coerce_numeric(payload.get("price"))
    if price > 0.0 and not math.isclose(price, _coerce_numeric(entry.get("price"))):
        entry["price"] = price
        changed = True

    change = _coerce_numeric(payload.get("price_change"))
    if change != 0.0:
        existing_change = _coerce_numeric(entry.get("price_change"))
        if not math.isclose(change, existing_change):
            entry["price_change"] = change
            changed = True

    volume = _coerce_numeric(payload.get("volume_24h") or payload.get("volume"))
    if volume > _coerce_numeric(entry.get("volume")):
        entry["volume"] = volume
        changed = True

    liquidity = _coerce_numeric(payload.get("liquidity_usd") or payload.get("liquidity"))
    if liquidity > _coerce_numeric(entry.get("liquidity")):
        entry["liquidity"] = liquidity
        changed = True

    holders = payload.get("holders")
    if isinstance(holders, (int, float)) and holders > 0:
        if int(holders) != int(_coerce_numeric(entry.get("holders"))):
            entry["holders"] = int(holders)
            changed = True

    pool_count = payload.get("pool_count")
    if isinstance(pool_count, (int, float)) and pool_count > 0:
        if int(pool_count) != int(_coerce_numeric(entry.get("pool_count"))):
            entry["pool_count"] = int(pool_count)
            changed = True

    sources = entry.setdefault("sources", set())
    if not isinstance(sources, set):
        sources = {str(src) for src in sources if isinstance(src, str)}
        entry["sources"] = sources
    if changed:
        before = len(sources)
        sources.add("birdeye_overview")
        if len(sources) != before:
            changed = True

    return changed


async def _enrich_trending_candidates(
    candidates: Dict[str, Dict[str, Any]],
    addresses: Iterable[str],
    *,
    session: aiohttp.ClientSession | None = None,
) -> bool:
    pending = []
    for address in addresses:
        if address in candidates and candidates[address]:
            pending.append(address)
    if not pending:
        return False

    session_obj = session
    if session_obj is None:
        try:
            session_obj = await get_session()
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Trending enrichment session unavailable: %s", exc)
            session_obj = None

    updated = False
    semaphore = asyncio.Semaphore(4)

    async def _hydrate(address: str) -> None:
        nonlocal updated
        entry = candidates.get(address)
        if not entry:
            return
        async with semaphore:
            try:
                payload = await onchain_metrics.fetch_birdeye_overview_async(
                    address, session=session_obj
                )
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Trending enrichment failed for %s: %s", address, exc)
                return
        if not payload:
            return
        if _apply_trending_enrichment(entry, payload):
            updated = True

    tasks = [_hydrate(addr) for addr in pending]
    if tasks:
        await asyncio.gather(*tasks)

    return updated


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


async def _fetch_birdeye_tokens(*, limit: int | None = None) -> List[TokenEntry]:
    """
    Pull BirdEye token list (paginated) for Solana with correct headers & params.
    Numeric filters only; no name/suffix heuristics.
    """
    api_key = _resolve_birdeye_api_key()
    global _BIRDEYE_DISABLED_INFO
    if not api_key:
        if not _BIRDEYE_DISABLED_INFO:
            logger.warning(
                "BirdEye API key missing; BirdEye discovery disabled. Remaining discovery sources continue to operate."
            )
            _BIRDEYE_DISABLED_INFO = True
        logger.debug("BirdEye API key missing; skipping BirdEye discovery")
        raise DiscoveryConfigurationError(
            "birdeye",
            "BirdEye API key missing; BirdEye discovery disabled.",
            remediation="Set the BIRDEYE_API_KEY environment variable.",
        )

    cache_key = _current_cache_key(limit)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    tokens: Dict[str, TokenEntry] = {}
    offset = 0
    effective_limit = _normalize_limit(limit)
    target_count = max(int(effective_limit * SETTINGS.overfetch_factor), SETTINGS.page_limit)
    backoff = SETTINGS.birdeye_backoff

    logger.debug(
        "BirdEye fetch start offset=%s limit=%s target=%s effective_limit=%s",
        offset,
        SETTINGS.page_limit,
        target_count,
        effective_limit,
    )

    def _headers() -> dict:
        return {
            "X-API-KEY": api_key,
            "x-chain": "solana",
            "Accept": "application/json",
        }

    try:
        session_timeout = ClientTimeout(total=12, connect=4, sock_read=8)
    except TypeError:
        try:
            session_timeout = ClientTimeout(total=12, sock_connect=4, sock_read=8)
        except TypeError:
            session_timeout = ClientTimeout(total=12)
    try:
        session = await get_session(timeout=session_timeout)
    except TypeError:
        session = await get_session()
    while offset < SETTINGS.max_offset and len(tokens) < target_count:
        initial_count = len(tokens)
        logger.debug(
            "BirdEye fetch page offset=%s limit=%s accumulated=%s",
            offset,
            SETTINGS.page_limit,
            len(tokens),
        )
        params = {
            "offset": offset,
            "limit": SETTINGS.page_limit,
            "sortBy": "v24hUSD",
            "chain": "solana",  # also pass chain in query to satisfy stricter backends
        }
        payload: Dict[str, Any] | None = None
        for attempt in range(1, SETTINGS.birdeye_retries + 1):
            try:
                request_cm = session.get(
                    _birdeye_tokenlist_url(),
                    params=params,
                    headers=_headers(),
                )
                async with request_cm as resp:
                    if resp.status == 503 or 500 <= resp.status < 600:
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
                                ra_val = min(float(retry_after), SETTINGS.birdeye_backoff_max)
                                delay = max(delay, ra_val)
                            except ValueError:
                                pass
                        if attempt >= SETTINGS.birdeye_retries:
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
                        backoff = min(max(backoff * 2, delay), SETTINGS.birdeye_backoff_max)
                        continue

                    if 400 <= resp.status < 500:
                        body_text = await resp.text()
                        snippet = (body_text or "").strip().replace("\n", " ")[:200]
                        lower_snippet = snippet.lower()
                        reason = (getattr(resp, "reason", None) or "").strip()
                        detail_parts = [part for part in (reason, snippet) if part]
                        detail = ": ".join(detail_parts)
                        message = f"BirdEye request failed with HTTP {resp.status}"
                        if detail:
                            message = f"{message}: {detail}"

                        if resp.status == 401:
                            remediation = (
                                "Verify the BirdEye API key is configured (BIRDEYE_API_KEY) and remains active."
                            )
                        elif resp.status == 403:
                            remediation = (
                                "Ensure the BirdEye API key has access to the token list endpoint and that IP restrictions allow this host."
                            )
                        elif resp.status == 429 or any(
                            marker in lower_snippet for marker in _BIRDEYE_THROTTLE_MARKERS
                        ):
                            remediation = (
                                "Wait for BirdEye rate limits to reset or reduce discovery frequency/upgrade the plan."
                            )
                        else:
                            remediation = (
                                "Check BirdEye API parameters and account status in the BirdEye dashboard."
                            )

                        raise DiscoveryConfigurationError(
                            "birdeye",
                            message,
                            remediation=remediation,
                        )

                    resp.raise_for_status()
                    payload = await resp.json()
                    backoff = SETTINGS.birdeye_backoff
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                logger.warning(
                    "BirdEye request error attempt=%s offset=%s: %s",
                    attempt,
                    offset,
                    exc,
                )
                if attempt >= SETTINGS.birdeye_retries:
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
                backoff = min(backoff * 2, SETTINGS.birdeye_backoff_max)
                continue
            except Exception as exc:
                if isinstance(exc, DiscoveryConfigurationError):
                    raise
                logger.warning("BirdEye unexpected error offset=%s: %s", offset, exc)
                if not tokens:
                    _cache_set(cache_key, [])
                    return []
                payload = None
                break
            else:
                # Success path already breaks out of loop
                pass

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

            if SETTINGS.min_volume and volume < SETTINGS.min_volume:
                continue
            if SETTINGS.min_liquidity and liquidity < SETTINGS.min_liquidity:
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
            if len(tokens) >= effective_limit:
                break

            offset += SETTINGS.page_limit
            total = data.get("total")
            if total is None:
                total = data.get("totalCount")
            try:
                total_int = int(total) if total is not None else None
            except (TypeError, ValueError):
                total_int = None
            if total_int is not None and offset >= total_int:
                break

        if len(tokens) >= effective_limit:
            break

        if len(tokens) <= initial_count:
            break

    result = list(tokens.values())
    _cache_set(cache_key, result)
    if not result:
        logger.warning("Token discovery: BirdEye returned no items after filtering.")
    logger.debug(
        "BirdEye fetch complete total=%s cached=%s", len(result), bool(result)
    )
    return result


async def _fetch_raydium_tokens(
    *, session: aiohttp.ClientSession | None = None
) -> List[TokenEntry]:
    if not SETTINGS.enable_raydium:
        return []
    try:
        payload = await raydium_provider.fetch(
            None,
            timeout=SETTINGS.raydium_timeout,
            session=session,
        )
    except Exception as exc:
        logger.debug("Raydium discovery request failed: %s", exc)
        return []
    if not isinstance(payload, Mapping):
        return []
    pairs = payload.get("pairs")
    if not isinstance(pairs, Sequence):
        return []
    results: List[TokenEntry] = []
    for pair in pairs:
        if not isinstance(pair, Mapping):
            continue
        mint_base = pair.get("mint_base")
        mint_quote = pair.get("mint_quote")
        valid_mint = None
        for candidate in (mint_base, mint_quote):
            if isinstance(candidate, str) and is_valid_solana_mint(candidate):
                valid_mint = candidate
                break
        if not valid_mint:
            continue
        liquidity = _coerce_numeric(pair.get("liquidity_usd"))
        if SETTINGS.min_liquidity and liquidity < SETTINGS.min_liquidity:
            continue
        price = _coerce_numeric(pair.get("price_usd"))
        entry: Dict[str, Any] = {
            "address": valid_mint,
            "name": pair.get("name") or pair.get("symbol") or valid_mint,
            "symbol": pair.get("symbol") or "",
            "liquidity": liquidity,
            "price": price,
            "venues": ["raydium"],
            "sources": ["raydium"],
        }
        pool_addr = pair.get("pool")
        if isinstance(pool_addr, str) and pool_addr:
            entry["pair_address"] = pool_addr
        discovered_at = _parse_timestamp(pair.get("as_of"))
        if discovered_at is not None:
            entry["discovered_at"] = discovered_at
        results.append(entry)
    return results


async def _fetch_dexscreener_tokens(
    *, session: aiohttp.ClientSession | None = None
) -> List[TokenEntry]:
    if not SETTINGS.enable_dexscreener or not SETTINGS.dexscreener_url:
        return []

    try:
        payload = await _http_get_json(
            SETTINGS.dexscreener_url,
            headers={"Accept": "application/json"},
            timeout=SETTINGS.dexscreener_timeout,
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
    max_age = max(0.0, float(SETTINGS.dexscreener_max_age))

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
    if not SETTINGS.enable_meteora or not SETTINGS.meteora_pools_url:
        return []

    try:
        payload = await _http_get_json(
            SETTINGS.meteora_pools_url,
            headers={"Accept": "application/json"},
            timeout=SETTINGS.meteora_timeout,
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
    if not SETTINGS.enable_dexlab or not SETTINGS.dexlab_list_url:
        return []

    try:
        payload = await _http_get_json(
            SETTINGS.dexlab_list_url,
            headers={"Accept": "application/json"},
            timeout=SETTINGS.dexlab_timeout,
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

    entry.setdefault("sources", set())
    entry.setdefault("source_categories", set())
    _register_source(entry, "solscan")


async def _enrich_with_solscan(
    candidates: Dict[str, Dict[str, Any]],
    *,
    addresses: Iterable[str] | None = None,
) -> None:
    if (
        not SETTINGS.enable_solscan
        or not SETTINGS.solscan_meta_url
        or SETTINGS.solscan_enrich_limit <= 0
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
        if len(pending) >= SETTINGS.solscan_enrich_limit:
            break

    if not pending:
        return

    headers = {"Accept": "application/json"}
    if SETTINGS.solscan_api_key:
        headers["token"] = SETTINGS.solscan_api_key

    timeout = _make_timeout(SETTINGS.solscan_timeout)
    session = await get_session(timeout=timeout)

    concurrency = max(1, min(4, SETTINGS.solscan_enrich_limit))
    semaphore = asyncio.Semaphore(concurrency)

    async def _fetch_and_apply(address: str) -> None:
        params = {"tokenAddress": address, "address": address}
        async with semaphore:
            try:
                payload = await _http_get_json(
                    SETTINGS.solscan_meta_url,
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
            if len(scores) >= SETTINGS.mempool_limit:
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

    def __init__(
        self,
        agen: AsyncIterator[List[TokenEntry]],
        *,
        config_errors: List[DiscoveryConfigurationError] | None = None,
    ) -> None:
        self._agen = agen
        self._final: List[TokenEntry] | None = None
        self._consumed = False
        self._config_errors = config_errors if config_errors is not None else []

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

    @property
    def config_errors(self) -> Tuple[DiscoveryConfigurationError, ...]:
        """Return BirdEye configuration errors observed during discovery."""

        return tuple(self._config_errors)

    @property
    def primary_config_error(self) -> DiscoveryConfigurationError | None:
        """Convenience accessor for the first configuration error, if any."""

        return self._config_errors[0] if self._config_errors else None

    @property
    def degraded_sources(self) -> Tuple[str, ...]:
        """Return optional discovery sources that failed or degraded."""

        sources: set[str] = set()
        for error in self._config_errors:
            source = getattr(error, "source", None)
            if source:
                sources.add(str(source))
        return tuple(sorted(sources))

    @property
    def degraded(self) -> bool:
        """Return ``True`` when any optional discovery sources degraded."""

        return bool(self.degraded_sources)


def discover_candidates(
    rpc_url: str,
    *,
    limit: int | None = None,
    mempool_threshold: float | None = None,
) -> _DiscoveryResult:
    """Combine BirdEye numeric candidates with mempool signals and rank."""

    if limit is None or limit <= 0:
        limit = SETTINGS.max_tokens
    if mempool_threshold is None:
        mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)

    shared_http_sources = any(
        (
            SETTINGS.enable_dexscreener and SETTINGS.dexscreener_url,
            SETTINGS.enable_meteora and SETTINGS.meteora_pools_url,
            SETTINGS.enable_dexlab and SETTINGS.dexlab_list_url,
            SETTINGS.enable_raydium,
            SETTINGS.enable_orca,
        )
    )

    def _score_candidates(
        candidates: Dict[str, Dict[str, Any]],
        mempool: Dict[str, Dict[str, float]],
    ) -> None:
        for addr, entry in candidates.items():
            entry.setdefault("sources", set())
            entry.setdefault("source_categories", set())
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
            required_sources = _stage_b_required_sources(entry, mp)
            source_count = _source_count(entry)
            entry["_stage_b_eligible"] = bool(
                score >= SETTINGS.stage_b_score_threshold
                or (required_sources > 0 and source_count >= required_sources)
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
            categories = entry.get("source_categories", [])
            if isinstance(categories, set):
                category_list = sorted(categories)
            else:
                category_list = sorted(list(categories or []))
            copy["source_categories"] = category_list
            venues_field = entry.get("venues")
            if isinstance(venues_field, set):
                venues_list = sorted(venues_field)
            elif isinstance(venues_field, list):
                venues_list = list(venues_field)
            elif venues_field:
                venues_list = sorted(_normalize_venues_field(venues_field))
            else:
                venues_list = []
            copy["venues"] = venues_list
            for internal in ("_stage_b_eligible", "score_breakdown"):
                copy.pop(internal, None)
            final.append(copy)
        return final

    config_errors: List[DiscoveryConfigurationError] = []

    async def _generator() -> AsyncIterator[List[TokenEntry]]:
        async def _close_candidate_session(candidate: Any) -> None:
            if candidate is None:
                return
            close = getattr(candidate, "close", None)
            if callable(close):
                with contextlib.suppress(Exception):
                    result = close()
                    if inspect.isawaitable(result):
                        await result

        shared_session_obj: aiohttp.ClientSession | None = None
        if shared_http_sources:
            try:
                shared_session_obj = await get_session()
            except Exception as exc:
                logger.warning(
                    "Discovery shared session unavailable; falling back to per-task sessions: %s",
                    exc,
                )
                for attr in ("partial_session", "session"):
                    candidate = getattr(exc, attr, None)
                    if candidate is not None:
                        await _close_candidate_session(candidate)
                shared_session_obj = None

        async def _run(
            shared_session: aiohttp.ClientSession | None,
        ) -> AsyncIterator[List[TokenEntry]]:
            orca_catalog: Dict[str, List[Dict[str, Any]]] = {}
            if SETTINGS.enable_orca:
                try:
                    orca_catalog = await _load_orca_catalog(session=shared_session)
                except Exception as exc:
                    logger.debug("Orca catalog unavailable: %s", exc)
                    orca_catalog = {}
            bird_task = asyncio.create_task(_fetch_birdeye_tokens(limit=limit))
            trending_task = asyncio.create_task(
                fetch_trending_tokens_async(limit=limit)
            )
            mempool_task = (
                asyncio.create_task(
                    _collect_mempool_signals(rpc_url, mempool_threshold)
                )
                if SETTINGS.enable_mempool and rpc_url
                else None
            )
            if SETTINGS.enable_mempool and rpc_url:
                logger.debug("Discovery mempool threshold=%.3f", mempool_threshold)

            task_map: Dict[asyncio.Task[Any], str] = {
                bird_task: "bird",
                trending_task: "trending",
            }
            if mempool_task is not None:
                task_map[mempool_task] = "mempool"
            if SETTINGS.enable_dexscreener and SETTINGS.dexscreener_url:
                task_map[
                    asyncio.create_task(
                        _fetch_dexscreener_tokens(session=shared_session)
                    )
                ] = "dexscreener"
            if SETTINGS.enable_raydium:
                task_map[
                    asyncio.create_task(
                        _fetch_raydium_tokens(session=shared_session)
                    )
                ] = "raydium"
            if SETTINGS.enable_meteora and SETTINGS.meteora_pools_url:
                task_map[
                    asyncio.create_task(
                        _fetch_meteora_tokens(session=shared_session)
                    )
                ] = "meteora"
            if SETTINGS.enable_dexlab and SETTINGS.dexlab_list_url:
                task_map[
                    asyncio.create_task(
                        _fetch_dexlab_tokens(session=shared_session)
                    )
                ] = "dexlab"

            def _enrich_with_orca(entry: Dict[str, Any] | None) -> None:
                if not entry:
                    return
                if not SETTINGS.enable_orca or not orca_catalog:
                    return
                _merge_orca_venues(entry, orca_catalog)

            merge_locks: Dict[str, asyncio.Lock] = {
                label: asyncio.Lock() for label in set(task_map.values())
            }

            candidates: Dict[str, Dict[str, Any]] = {}
            mempool: Dict[str, Dict[str, float]] = {}

            bird_tokens: List[TokenEntry] = []
            dexscreener_tokens: List[TokenEntry] = []
            raydium_tokens: List[TokenEntry] = []
            meteora_tokens: List[TokenEntry] = []
            dexlab_tokens: List[TokenEntry] = []

            trending_enriched: set[str] = set()

            last_snapshot: List[TokenEntry] | None = None
            emitted = False

            overall_timeout_raw = os.getenv("DISCOVERY_OVERALL_TIMEOUT", "0")
            try:
                overall_timeout = float(overall_timeout_raw or 0.0)
            except Exception:
                overall_timeout = 0.0

            completed: set[asyncio.Task[Any]] = set()
            config_error: DiscoveryConfigurationError | None = None

            async def _merge_result(label: str, result: Any) -> bool:
                nonlocal mempool, bird_tokens, dexscreener_tokens, raydium_tokens
                nonlocal meteora_tokens, dexlab_tokens
                nonlocal config_error, config_errors
                lock = merge_locks[label]
                async with lock:
                    changed = False
                    source_id, friendly_name = _OPTIONAL_SOURCE_META.get(
                        label, (label, label)
                    )

                    def _record_error(
                        error: DiscoveryConfigurationError,
                        *,
                        track_primary: bool = False,
                    ) -> None:
                        nonlocal config_error
                        duplicate = False
                        for existing in config_errors:
                            if (
                                existing.source == error.source
                                and str(existing) == str(error)
                            ):
                                duplicate = True
                                break
                        if not duplicate:
                            config_errors.append(error)
                        if track_primary:
                            config_error = error

                    def _wrap_optional_error(exc: Exception) -> DiscoveryConfigurationError:
                        if isinstance(exc, DiscoveryConfigurationError):
                            return exc
                        message = f"{friendly_name} discovery unavailable: {exc}"
                        return DiscoveryConfigurationError(source_id, message)

                    if label == "bird":
                        if isinstance(result, Exception):
                            if isinstance(result, DiscoveryConfigurationError):
                                _record_error(result, track_primary=True)
                                logger.warning(
                                    "BirdEye discovery disabled due to configuration: %s",
                                    result,
                                )
                                return False
                            wrapped = _wrap_optional_error(result)
                            _record_error(wrapped, track_primary=True)
                            logger.warning("BirdEye discovery failed: %s", result)
                            return False
                        bird_tokens = list(result or [])
                        for token in bird_tokens:
                            entry = _merge_candidate_entry(candidates, dict(token), "birdeye")
                            if entry is None:
                                continue
                            _enrich_with_orca(entry)
                            changed = True
                        return changed
                    if label == "mempool":
                        if isinstance(result, Exception):
                            error = _wrap_optional_error(result)
                            _record_error(error)
                            logger.warning("Mempool signals unavailable: %s", result)
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
                            _enrich_with_orca(entry)
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
                    if label == "trending":
                        if isinstance(result, Exception):
                            error = _wrap_optional_error(result)
                            _record_error(error)
                            logger.warning("Trending discovery unavailable: %s", result)
                            return False
                        trending_items = list(result or [])
                        needs_enrichment: List[str] = []
                        for raw in trending_items:
                            if not isinstance(raw, str):
                                continue
                            address = raw.strip()
                            if not address:
                                continue
                            existing_entry = candidates.get(address)
                            had_trending = False
                            if isinstance(existing_entry, Mapping):
                                sources_field = existing_entry.get("sources")
                                if isinstance(sources_field, set):
                                    had_trending = "trending" in sources_field
                                elif sources_field:
                                    normalized_sources = _normalize_string_collection(
                                        sources_field
                                    )
                                    had_trending = "trending" in normalized_sources
                            entry = _merge_candidate_entry(
                                candidates,
                                {
                                    "address": address,
                                    "name": address,
                                    "symbol": address,
                                },
                                "trending",
                            )
                            if entry is None:
                                continue
                            if not had_trending:
                                changed = True
                            _enrich_with_orca(entry)
                            if (
                                address not in trending_enriched
                                and _needs_trending_enrichment(entry)
                            ):
                                needs_enrichment.append(address)
                        if needs_enrichment:
                            updated = await _enrich_trending_candidates(
                                candidates,
                                needs_enrichment,
                                session=shared_session,
                            )
                            trending_enriched.update(needs_enrichment)
                            if updated:
                                changed = True
                        return changed
                    if label == "dexscreener":
                        if isinstance(result, Exception):
                            error = _wrap_optional_error(result)
                            _record_error(error)
                            logger.warning(
                                "DexScreener discovery unavailable: %s", result
                            )
                            return False
                        dexscreener_tokens = list(result or [])
                        for token in dexscreener_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "dexscreener"
                            )
                            if entry is None:
                                continue
                            _enrich_with_orca(entry)
                            changed = True
                        return changed
                    if label == "raydium":
                        if isinstance(result, Exception):
                            error = _wrap_optional_error(result)
                            _record_error(error)
                            logger.warning("Raydium discovery unavailable: %s", result)
                            return False
                        raydium_tokens = list(result or [])
                        for token in raydium_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "raydium"
                            )
                            if entry is None:
                                continue
                            _enrich_with_orca(entry)
                            changed = True
                        return changed
                    if label == "meteora":
                        if isinstance(result, Exception):
                            error = _wrap_optional_error(result)
                            _record_error(error)
                            logger.warning("Meteora discovery unavailable: %s", result)
                            return False
                        meteora_tokens = list(result or [])
                        for token in meteora_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "meteora"
                            )
                            if entry is None:
                                continue
                            _enrich_with_orca(entry)
                            changed = True
                        return changed
                    if label == "dexlab":
                        if isinstance(result, Exception):
                            error = _wrap_optional_error(result)
                            _record_error(error)
                            logger.warning("DexLab discovery unavailable: %s", result)
                            return False
                        dexlab_tokens = list(result or [])
                        for token in dexlab_tokens:
                            entry = _merge_candidate_entry(
                                candidates, dict(token), "dexlab"
                            )
                            if entry is None:
                                continue
                            _enrich_with_orca(entry)
                            changed = True
                        return changed
                    return False

            task_list = list(task_map.keys())

            try:
                pending: set[asyncio.Task[Any]] = set(task_list)
                start_time = time.monotonic()
                while pending:
                    wait_timeout: float | None = None
                    if overall_timeout > 0:
                        elapsed = time.monotonic() - start_time
                        remaining = overall_timeout - elapsed
                        if remaining <= 0:
                            raise asyncio.TimeoutError
                        wait_timeout = remaining
                    done, pending = await asyncio.wait(
                        pending,
                        timeout=wait_timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if not done:
                        raise asyncio.TimeoutError
                    for fut in done:
                        label = task_map.get(fut)
                        try:
                            result = await fut
                        except Exception as exc:
                            result = exc
                        completed.add(fut)
                        if label is None:
                            continue
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
                "Discovery combine summary bird=%s mempool=%s dexscreener=%s raydium=%s meteora=%s dexlab=%s final=%s top_score=%s",
                len(bird_tokens),
                len(mempool),
                len(dexscreener_tokens),
                len(raydium_tokens),
                len(meteora_tokens),
                len(dexlab_tokens),
                len(final),
                f"{top_score:.4f}" if isinstance(top_score, (int, float)) else "n/a",
            )

            fallback_final: List[TokenEntry] | None = None
            fallback_reasons: List[str] = []

            if not final:
                fallback_entries = _fallback_candidate_tokens(limit)
                fallback_candidates: Dict[str, Dict[str, Any]] = {}
                for entry in fallback_entries:
                    if not isinstance(entry, Mapping):
                        continue
                    address = str(entry.get("address") or "").strip()
                    if not address:
                        continue
                    entry_copy = dict(entry)
                    entry_copy["sources"] = _normalize_string_collection(
                        entry_copy.get("sources")
                    )
                    entry_copy["source_categories"] = _normalize_string_collection(
                        entry_copy.get("source_categories")
                    )
                    _register_source(entry_copy, "fallback")
                    entry_copy.setdefault("score", 0.0)
                    entry_copy.setdefault("_stage_b_eligible", False)
                    fallback_candidates[address] = entry_copy

                if fallback_candidates:
                    if not candidates:
                        fallback_reasons.append("live sources empty")
                    cache_entry_count = sum(
                        1
                        for entry in fallback_candidates.values()
                        if "cache" in entry.get("sources", set())
                    )
                    if cache_entry_count == 0:
                        fallback_reasons.append("cache empty")

                    fallback_final = _snapshot(fallback_candidates, limit=limit)
                    if fallback_final:
                        degrade_reason = ", ".join(fallback_reasons) or "fallback engaged"
                        logger.warning(
                            "Discovery degraded (%s); using fallback set (%d). config_error=%s",
                            degrade_reason,
                            len(fallback_candidates),
                            config_error or "none",
                        )

            if config_error and config_error not in config_errors:
                config_errors.append(config_error)

            if not emitted or final != last_snapshot:
                if final:
                    yield final
                elif fallback_final:
                    yield fallback_final
                    return
                else:
                    yield final

        if shared_session_obj is not None:
            shared_session: aiohttp.ClientSession | None = shared_session_obj
            owns_session = False
            exit_method = None

            if not isinstance(shared_session_obj, aiohttp.ClientSession):
                enter = getattr(shared_session_obj, "__aenter__", None)
                exit_candidate = getattr(shared_session_obj, "__aexit__", None)
                if (
                    callable(enter)
                    and callable(exit_candidate)
                    and not hasattr(shared_session_obj, "get")
                ):
                    shared_session = await enter()
                    exit_method = exit_candidate
                    owns_session = True

            try:
                async for batch in _run(shared_session):
                    yield batch
            finally:
                if owns_session and exit_method is not None:
                    with contextlib.suppress(Exception):
                        result = exit_method(None, None, None)
                        if inspect.isawaitable(result):
                            await result
                elif owns_session and shared_session is not None:
                    close = getattr(shared_session, "close", None)
                    if callable(close):
                        with contextlib.suppress(Exception):
                            result = close()
                            if inspect.isawaitable(result):
                                await result
        else:
            async for batch in _run(None):
                yield batch

    return _DiscoveryResult(_generator(), config_errors=config_errors)


def warm_cache(rpc_url: str, *, limit: int | None = None) -> None:
    """Prime the discovery cache synchronously (best-effort)."""
    api_key = _resolve_birdeye_api_key()
    if not (rpc_url or api_key):
        return

    limit = limit or min(SETTINGS.max_tokens, 10)
    mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)

    def _worker() -> None:
        try:
            async def _consume() -> None:
                async for _ in discover_candidates(
                    rpc_url, limit=limit, mempool_threshold=mempool_threshold
                ):
                    pass

            asyncio.run(asyncio.wait_for(_consume(), timeout=SETTINGS.warm_timeout))
        except Exception as exc:
            logger.debug("Discovery warm cache failed: %s", exc)

    thread = threading.Thread(target=_worker, name="discovery-warm", daemon=True)
    thread.start()
