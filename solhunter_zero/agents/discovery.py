from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import os
import re
import time
import urllib.parse
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Mapping
from weakref import WeakKeyDictionary

from .. import config
from ..token_aliases import canonical_mint, validate_mint
from ..discovery import (
    _MEMPOOL_TIMEOUT,
    _MEMPOOL_TIMEOUT_RETRIES,
    merge_sources,
)
from ..event_bus import publish
from ..mempool_scanner import stream_ranked_mempool_tokens_with_depth
from ..scanner_common import (
    DEFAULT_SOLANA_RPC,
    DEFAULT_SOLANA_WS,
    scan_tokens_from_file,
)
from ..scanner_onchain import scan_tokens_onchain
from ..schemas import RuntimeLog
from ..news import fetch_token_mentions_async
from ..url_helpers import as_websocket_url
from ..token_discovery import compute_score_from_features, get_scoring_context

logger = logging.getLogger(__name__)


def resolve_discovery_limit(*, default: int = 60) -> int:
    """Return a validated discovery limit from the environment.

    A resolved limit of zero disables active discovery. Values are clamped to the
    inclusive range [0, 500] to avoid accidental runaway or disabled discovery.
    """

    raw_value = os.getenv("DISCOVERY_LIMIT")
    if raw_value is None:
        return default

    if isinstance(raw_value, str):
        text = raw_value.strip()
    else:
        text = str(raw_value).strip()

    if not text:
        return default

    try:
        resolved = int(text)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid DISCOVERY_LIMIT=%r; defaulting to %d",
            raw_value,
            default,
        )
        return default

    if resolved < 0:
        logger.warning("Discovery limit %d below 0; clamping to 0", resolved)
        return 0

    if resolved > 500:
        logger.warning("Discovery limit %d above 500; clamping to 500", resolved)
        return 500

    return resolved

try:
    from ..token_scanner import enrich_tokens_async, scan_tokens_async
except ImportError as exc:  # pragma: no cover - optional dependency guard
    logger.warning(
        "token_scanner unavailable (%s); falling back to stubbed discovery helpers",
        exc,
    )

    async def scan_tokens_async(
        *,
        rpc_url: str | None = None,
        limit: int = 50,
        enrich: bool = True,
        api_key: str | None = None,
    ) -> List[str]:
        mode = "stub-enriched" if enrich else "stub-raw"
        rpc_hint = _rpc_identity(rpc_url)
        api_hint = "provided" if api_key else "none"
        logger.info(
            "Using fallback discovery (%s; rpc=%s; api_key=%s)",
            mode,
            rpc_hint or "<default>",
            api_hint,
        )

        seeds = list(_STATIC_FALLBACK)
        limited = seeds[: max(0, limit)]
        if enrich:
            return await enrich_tokens_async(limited, rpc_url=rpc_url or DEFAULT_SOLANA_RPC)
        return limited

    async def enrich_tokens_async(
        mints: Iterable[str],
        *,
        rpc_url: str = DEFAULT_SOLANA_RPC,
    ) -> List[str]:
        del rpc_url
        enriched: List[str] = []
        for candidate in mints:
            canonical = canonical_mint(candidate)
            if canonical and validate_mint(canonical):
                enriched.append(canonical)
        return enriched

_CACHE: dict[str, object] = {
    "tokens": [],
    "ts": 0.0,
    "limit": 0,
    "method": "",
    "rpc_identity": "",
    "details": {},
    "metadata": {},
}

_CACHE_LOCKS: WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    WeakKeyDictionary()
)


_UNSET = object()


def _get_cache_lock() -> asyncio.Lock:
    loop = asyncio.get_running_loop()
    lock = _CACHE_LOCKS.get(loop)
    if lock is None:
        lock = asyncio.Lock()
        _CACHE_LOCKS[loop] = lock
    return lock


def _rpc_identity(url: Optional[str]) -> str:
    """Return a stable identity for the active RPC endpoint/cluster."""

    if not url:
        return ""
    text = url.strip()
    if not text:
        return ""
    lower_text = text.lower()
    try:
        parsed = urllib.parse.urlparse(text)
    except Exception:  # pragma: no cover - extremely defensive
        parsed = None

    cluster = "mainnet-beta"
    cluster_markers = ("devnet", "testnet", "mainnet-beta", "mainnet")
    for marker in cluster_markers:
        if marker in lower_text:
            cluster = "mainnet-beta" if marker == "mainnet" else marker
            break

    if parsed and parsed.scheme and parsed.netloc:
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        base = f"{scheme}://{netloc}"
        path = parsed.path.rstrip("/")
        if path:
            base = f"{base}{path}"
        else:
            base = f"{base}/"
    else:  # pragma: no cover - fallback for malformed URLs
        base = lower_text

    return f"{cluster}:{base}"
_STATIC_FALLBACK_ENV_KEY = "DISCOVERY_STATIC_FALLBACK_TOKENS"
_DEFAULT_STATIC_FALLBACK_MIN_SEEDS = 12
_DEFAULT_STATIC_FALLBACK: List[str] = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERbGZ7FHYg5UvRntnQvQFNR7GWhLZNnzG",  # USDT
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
    "7dHbWXmci3uvZ5mRSa3sLPj5FDQkptczkXZdSDmR6uFQ",  # stSOL
    "mSoLzHC1RPuJgC2qYGMxE8VUipPNo8Gszrj8wCN7P1w",  # mSOL
    "7vfCXTUXQ5sZevj4B6ipZDwBauCg1cjSXo59p7u2L2MF",  # soETH
    "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E",  # soBTC
    "9wFFyRfR75bMusT2PwnVr2RAqvMwXUasPdD3Z2Y6X5VW",  # SRM
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3dEHs66",  # RAY
    "orcaEKTdK7VKaeQuhXA1iG1inEDXSLjYH5SMhtcCbLt",  # ORCA
    "7xKXtg2FWu4SLWLGq1y8bDYt8BbwRwvf3X8sv6ELPvLt",  # SAMO
    "MNDEeF3W9YKaL8Z1ZNnFWsyd4SBhMZ859WJbz1b7fGz",  # MNDE
    "hntyVP9w9Jt8j6E7XEsVNpMPKS2X4WaNo7i6j7KduJw",  # HNT
    "J4sWktT7pVcFqj3Adxpv8uKUaX4nHCqB6SmRwZmL6sdr",  # PYTH
    "UXPhBoR4yYTGwFohHJM7KD3gjpDxb4JtXY6BskBFoxS",  # UXP
    "DUSTawucrTsGU8hcqRdHDCbuYhCPADMLM2VcCb8VnFnQ",  # DUST
]


def _resolve_static_fallback_min_seeds() -> int:
    raw_value = os.getenv("DISCOVERY_STATIC_FALLBACK_MIN_SEEDS")
    if raw_value in {None, ""}:
        return _DEFAULT_STATIC_FALLBACK_MIN_SEEDS
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid DISCOVERY_STATIC_FALLBACK_MIN_SEEDS=%r; defaulting to %d",
            raw_value,
            _DEFAULT_STATIC_FALLBACK_MIN_SEEDS,
        )
        return _DEFAULT_STATIC_FALLBACK_MIN_SEEDS
    if value <= 0:
        logger.warning(
            "DISCOVERY_STATIC_FALLBACK_MIN_SEEDS must be positive; defaulting to %d",
            _DEFAULT_STATIC_FALLBACK_MIN_SEEDS,
        )
        return _DEFAULT_STATIC_FALLBACK_MIN_SEEDS
    return value


def _iter_fallback_tokens(raw_value: str | None) -> Iterable[str]:
    if not raw_value:
        return []
    if not isinstance(raw_value, str):
        raw_value = str(raw_value)
    for part in re.split(r"[,\s]+", raw_value):
        candidate = part.strip()
        if candidate:
            yield candidate


def _load_static_fallback_tokens() -> List[str]:
    seen: set[str] = set()
    tokens: List[str] = []

    def _add(raw: str) -> None:
        canonical = canonical_mint(raw)
        if not canonical or canonical in seen:
            return
        if not validate_mint(canonical):
            logger.debug("Ignoring invalid static fallback token: %s", raw)
            return
        seen.add(canonical)
        tokens.append(canonical)

    env_tokens = list(_iter_fallback_tokens(os.getenv(_STATIC_FALLBACK_ENV_KEY)))
    if env_tokens:
        logger.debug(
            "Loaded %d configured static discovery fallback token(s)",
            len(env_tokens),
        )
    for token in env_tokens:
        _add(token)
    for token in _DEFAULT_STATIC_FALLBACK:
        _add(token)

    minimum = _resolve_static_fallback_min_seeds()
    if len(tokens) < minimum:
        logger.warning(
            "Static fallback token pool below minimum (%d < %d); padding with defaults",
            len(tokens),
            minimum,
        )
        for token in _DEFAULT_STATIC_FALLBACK:
            if len(tokens) >= minimum:
                break
            _add(token)
    return tokens


_STATIC_FALLBACK = _load_static_fallback_tokens()

DEFAULT_DISCOVERY_METHOD = "helius"

MIN_DISCOVERY_LIMIT = 0
DEFAULT_MAX_DISCOVERY_LIMIT = 200

DEFAULT_MEMPOOL_MAX_WAIT = 10.0

_DISCOVERY_METHOD_ALIASES: dict[str, str] = {
    "helius": "helius",
    "api": "helius",
    "rest": "helius",
    "http": "helius",
    "websocket": "websocket",
    "ws": "websocket",
    "merge": "websocket",
    "mempool": "mempool",
    "onchain": "onchain",
    "file": "file",
}

DISCOVERY_METHODS: frozenset[str] = frozenset(
    {"helius", "websocket", "mempool", "onchain", "file"}
)

# Tokens that must never be filtered out even if they match a generic rule
_FILTER_WHITELIST = {
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "11111111111111111111111111111111",  # System program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token program
}


def resolve_discovery_method(value: Any) -> Optional[str]:
    """Return a canonical discovery method or ``None`` if ``value`` is invalid."""

    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    method = value.strip().lower()
    if not method:
        return None
    canonical = _DISCOVERY_METHOD_ALIASES.get(method, method)
    if canonical in DISCOVERY_METHODS:
        return canonical
    return None


def _resolve_limit_cap(default: int = DEFAULT_MAX_DISCOVERY_LIMIT) -> int:
    """Return the maximum allowed discovery limit."""

    raw_value = os.getenv("DISCOVERY_LIMIT_CAP")
    if raw_value in {None, ""}:
        return default

    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid DISCOVERY_LIMIT_CAP=%r; defaulting to %d",
            raw_value,
            default,
        )
        return default

    if value < MIN_DISCOVERY_LIMIT:
        logger.warning(
            "DISCOVERY_LIMIT_CAP=%r below minimum %d; defaulting to %d",
            raw_value,
            MIN_DISCOVERY_LIMIT,
            default,
        )
        return default

    return value


class DiscoveryAgent:
    """Token discovery orchestrator supporting multiple discovery methods."""

    def __init__(self, *, limit: object = _UNSET) -> None:
        rpc_env = os.getenv("SOLANA_RPC_URL") or DEFAULT_SOLANA_RPC
        os.environ.setdefault("SOLANA_RPC_URL", rpc_env)
        self.rpc_url = rpc_env

        ws_env = self._resolve_ws_url()
        ws_resolved = self._as_websocket_url(ws_env) or DEFAULT_SOLANA_WS
        os.environ.setdefault("SOLANA_WS_URL", ws_resolved)
        self.ws_url = ws_resolved
        self.birdeye_api_key = os.getenv(
            "BIRDEYE_API_KEY",
            "b1e60d72780940d1bd929b9b2e9225e6",
        )
        if not self.birdeye_api_key:
            logger.warning(
                "BIRDEYE_API_KEY missing; BirdEye discovery disabled. Mempool and DEX sources remain active."
            )
        self.limit_cap = _resolve_limit_cap()
        self._disabled_skip_logged = False
        self.limit: Optional[int] = None
        initial_limit = resolve_discovery_limit(default=60) if limit is _UNSET else limit
        self.set_limit(initial_limit)
        cache_ttl_env = os.getenv("DISCOVERY_CACHE_TTL")
        cache_ttl_default = self._default_cache_ttl()
        if cache_ttl_env in {None, ""}:
            cache_ttl_value = cache_ttl_default
        else:
            try:
                cache_ttl_value = float(cache_ttl_env)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid DISCOVERY_CACHE_TTL=%r; defaulting to %.1fs",
                    cache_ttl_env,
                    cache_ttl_default,
                )
                cache_ttl_value = cache_ttl_default
        self.cache_ttl = max(0.0, float(cache_ttl_value))
        self.backoff = max(0.0, float(os.getenv("TOKEN_DISCOVERY_BACKOFF", "1") or 1.0))
        mempool_max_wait_env = os.getenv("DISCOVERY_MEMPOOL_MAX_WAIT")
        if mempool_max_wait_env in {None, ""}:
            mempool_max_wait = DEFAULT_MEMPOOL_MAX_WAIT
        else:
            try:
                mempool_max_wait = float(mempool_max_wait_env)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid DISCOVERY_MEMPOOL_MAX_WAIT=%r; using default %.1fs",
                    mempool_max_wait_env,
                    DEFAULT_MEMPOOL_MAX_WAIT,
                )
                mempool_max_wait = DEFAULT_MEMPOOL_MAX_WAIT
        self.mempool_max_wait = max(float(mempool_max_wait), 0.0)
        default_retries = 2
        retries_env = os.getenv("TOKEN_DISCOVERY_RETRIES")
        if retries_env in {None, ""}:
            retries = default_retries
        else:
            try:
                retries = int(retries_env)
            except (TypeError, ValueError):
                logger.warning(
                    "Invalid TOKEN_DISCOVERY_RETRIES=%r; using default %d",
                    retries_env,
                    default_retries,
                )
                retries = default_retries
            else:
                if retries < 1:
                    logger.warning(
                        "TOKEN_DISCOVERY_RETRIES=%r below minimum 1; using default %d",
                        retries_env,
                        default_retries,
                    )
                    retries = default_retries
        self.max_attempts = retries
        self.mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)
        enrich_env = os.getenv("DISCOVERY_ENRICH_RESULTS")
        self.enrich_results = bool(
            enrich_env and enrich_env.strip().lower() in {"1", "true", "yes", "on"}
        )
        env_method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        self.default_method = env_method or DEFAULT_DISCOVERY_METHOD
        self.last_details: Dict[str, Dict[str, Any]] = {}
        self.last_tokens: List[str] = []
        self.last_method: str | None = None
        self.last_fallback_used: bool = False
        self.filter_prefix_11 = (
            os.getenv("DISCOVERY_FILTER_PREFIX_11", "1").strip().lower()
            in {"1", "true", "yes", "on"}
        )
        self.social_limit = self._load_social_setting(
            "DISCOVERY_SOCIAL_LIMIT", default=12, minimum=0
        )
        self.social_min_mentions = self._load_social_setting(
            "DISCOVERY_SOCIAL_MIN_MENTIONS", default=2, minimum=1
        )
        self.social_sample_limit = self._load_social_setting(
            "DISCOVERY_SOCIAL_SAMPLE_LIMIT", default=3, minimum=1
        )
        self.news_feeds = self._split_env_list("NEWS_FEEDS")
        self.twitter_feeds = self._split_env_list("TWITTER_FEEDS")
        self.discord_feeds = self._split_env_list("DISCORD_FEEDS")
        # Track whether we have already logged a warning about BirdEye being
        # disabled so we avoid noisy duplicate messages when discovery runs in a
        # tight loop.
        self._birdeye_notice_logged = False

    def set_limit(self, limit: Any) -> None:
        """Update the active discovery limit, applying caps and validation."""

        self._disabled_skip_logged = False
        if limit is None:
            self.limit = None
            return
        try:
            value = int(limit)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid discovery limit override %r; keeping existing limit %r",
                limit,
                self.limit,
            )
            return
        if value < MIN_DISCOVERY_LIMIT:
            logger.warning(
                "Discovery limit %d below minimum %d; using minimum",
                value,
                MIN_DISCOVERY_LIMIT,
            )
            value = MIN_DISCOVERY_LIMIT
        if value > self.limit_cap:
            logger.warning(
                "Discovery limit %d above maximum %d; using maximum",
                value,
                self.limit_cap,
            )
            value = self.limit_cap
        self.limit = value
        if value == 0:
            logger.info(
                "Discovery limit set to 0; discovery will remain disabled",
            )

    @classmethod
    def _default_cache_ttl(cls) -> float:
        base_default = 45.0
        if not cls._fast_mode_enabled():
            return base_default

        fast_override = os.getenv("FAST_DISCOVERY_CACHE_TTL")
        if fast_override in {None, ""}:
            return base_default

        try:
            return max(0.0, float(fast_override))
        except (TypeError, ValueError):
            logger.warning(
                "Invalid FAST_DISCOVERY_CACHE_TTL=%r; using %.1fs default",
                fast_override,
                base_default,
            )
            return base_default

    @staticmethod
    def _fast_mode_enabled() -> bool:
        value = os.getenv("FAST_PIPELINE_MODE", "")
        return value.strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _resolve_mempool_enabled(flag: Optional[bool]) -> bool:
        if flag is None:
            mempool_flag = os.getenv("DISCOVERY_ENABLE_MEMPOOL")
            enabled = True
            if mempool_flag is not None:
                enabled = mempool_flag.strip().lower() in {"1", "true", "yes", "on"}
        else:
            enabled = bool(flag)
        return enabled

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def stream_mempool_events(
        self,
        rpc_url: Optional[str] = None,
        *,
        threshold: Optional[float] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Expose the ranked mempool stream for tests and advanced callers."""

        url = rpc_url or self.ws_url or self.rpc_url
        url = self._as_websocket_url(url) or self.ws_url or self.rpc_url
        try:
            url = self._validate_endpoint_url(url, kind="ws")
        except ValueError as exc:
            logger.error(
                "Invalid websocket endpoint %r for mempool streaming: %s", url, exc
            )
            raise
        thresh = self.mempool_threshold if threshold is None else float(threshold)
        return stream_ranked_mempool_tokens_with_depth(url, threshold=thresh)

    # ------------------------------------------------------------------
    # Internal wiring helpers
    # ------------------------------------------------------------------
    async def _cache_snapshot(
        self,
    ) -> tuple[
        List[str],
        Dict[str, Dict[str, Any]],
        float,
        int,
        str,
        str,
        Dict[str, Any],
    ]:
        async with _get_cache_lock():
            tokens_raw = _CACHE.get("tokens")
            details_raw = _CACHE.get("details")
            cache_ts = float(_CACHE.get("ts", 0.0))
            cache_limit = int(_CACHE.get("limit", 0))
            cache_method = str(_CACHE.get("method") or "").lower()
            cache_identity = str(_CACHE.get("rpc_identity") or "")
            cache_metadata = self._normalise_cache_metadata(
                _CACHE.get("metadata"), cache_limit, cache_method, cache_identity
            )

        tokens = list(tokens_raw) if isinstance(tokens_raw, list) else []
        details = self._normalise_cache_details(details_raw)
        return (
            tokens,
            details,
            cache_ts,
            cache_limit,
            cache_method,
            cache_identity,
            cache_metadata,
        )

    @staticmethod
    def _normalise_cache_details(payload: Any) -> Dict[str, Dict[str, Any]]:
        if not isinstance(payload, dict):
            return {}

        normalised: Dict[str, Dict[str, Any]] = {}
        for key, raw in payload.items():
            if not isinstance(key, str) or not isinstance(raw, dict):
                continue
            canonical = canonical_mint(key)
            if not canonical or not validate_mint(canonical):
                continue
            entry = dict(raw)
            entry.pop("cached", None)
            entry.pop("cache_stale", None)
            normalised[canonical] = entry
        return normalised

    @staticmethod
    def _normalise_cache_metadata(
        payload: Any, cache_limit: int, cache_method: str, cache_identity: str
    ) -> Dict[str, Any]:
        metadata = dict(payload) if isinstance(payload, dict) else {}
        metadata.setdefault("limit", cache_limit)
        metadata.setdefault("method", cache_method)
        metadata.setdefault("rpc_identity", cache_identity)
        return metadata

    def _build_cache_metadata(
        self,
        *,
        method: str,
        mempool_enabled: bool,
        token_file: Optional[str],
    ) -> Dict[str, Any]:
        metadata: Dict[str, Any] = {
            "method": method,
            "limit": self.limit,
            "rpc_identity": _rpc_identity(self.rpc_url),
            "enrich": bool(getattr(self, "enrich_results", False)),
        }
        if method in {"websocket", "mempool"}:
            metadata.update(
                mempool_enabled=bool(mempool_enabled),
                mempool_threshold=float(self.mempool_threshold),
            )
        if method == "websocket":
            metadata["ws_url"] = self.ws_url or ""
        if method == "mempool":
            metadata["mempool_max_wait"] = float(self.mempool_max_wait)
        if method == "file":
            metadata["token_file"] = token_file or ""
        return metadata

    @staticmethod
    def _cache_metadata_matches(
        cached: Mapping[str, Any], current: Mapping[str, Any]
    ) -> bool:
        if not cached:
            return False
        for key, value in current.items():
            if cached.get(key) != value:
                return False
        return True

    def _prepare_cache_details(
        self, details: Dict[str, Dict[str, Any]] | None
    ) -> Dict[str, Dict[str, Any]]:
        if not details:
            return {}

        payload: Dict[str, Dict[str, Any]] = {}
        for key, raw in details.items():
            if not isinstance(key, str) or not isinstance(raw, dict):
                continue
            canonical = canonical_mint(key)
            if not canonical or not validate_mint(canonical):
                continue
            entry = dict(raw)
            entry.pop("cached", None)
            entry.pop("cache_stale", None)
            sources = self._source_set(entry.get("sources"))
            if "sources" in entry:
                entry["sources"] = sorted(sources) if sources else []
            payload[canonical] = entry
        return payload

    def _hydrate_cached_details(
        self,
        payload: Dict[str, Dict[str, Any]] | None,
        *,
        stale: bool,
        tokens: Iterable[str] | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        if not payload:
            return {}

        wanted: set[str] | None = None
        if tokens is not None:
            wanted = set()
            for token in tokens:
                if not isinstance(token, str):
                    continue
                canonical = canonical_mint(token)
                if canonical and validate_mint(canonical):
                    wanted.add(canonical)

        try:
            bias, weights = get_scoring_context()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.debug("Failed to load scoring weights: %s", exc)
            bias = None
            weights = None

        hydrated: Dict[str, Dict[str, Any]] = {}
        for key, raw in payload.items():
            if not isinstance(key, str) or not isinstance(raw, dict):
                continue
            canonical = canonical_mint(key)
            if not canonical or not validate_mint(canonical):
                continue
            if wanted is not None and canonical not in wanted:
                continue
            entry = dict(raw)
            sources = self._source_set(entry.get("sources"))
            sources.add("cache")
            if stale:
                sources.add("cache:stale")
            entry["sources"] = sources
            entry["cached"] = True
            entry["cache_stale"] = bool(stale)

            features = entry.get("score_features")
            if isinstance(features, Mapping) and bias is not None and weights is not None:
                try:
                    score, breakdown, top = compute_score_from_features(
                        features,
                        bias=bias,
                        weights=weights,
                    )
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.debug(
                        "Failed to recompute cached score for %s: %s", canonical, exc
                    )
                else:
                    entry["score"] = float(score)
                    entry["score_breakdown"] = breakdown
                    entry["top_features"] = top

            hydrated[canonical] = entry
        return hydrated

    def _resolve_ws_url(self) -> Optional[str]:
        """Derive and cache a websocket RPC endpoint."""

        ws_url: Optional[str]
        try:
            ws_url = config.get_solana_ws_url()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("get_solana_ws_url failed: %s", exc)
            ws_url = None

        ws_url = self._as_websocket_url(ws_url) or self._as_websocket_url(
            os.getenv("SOLANA_WS_URL")
        )

        if not ws_url:
            ws_url = self._as_websocket_url(self.rpc_url)

        if ws_url:
            os.environ.setdefault("SOLANA_WS_URL", ws_url)

        return ws_url

    @staticmethod
    def _as_websocket_url(url: Optional[str]) -> Optional[str]:
        return as_websocket_url(url)

    @staticmethod
    def _validate_endpoint_url(url: Optional[str], *, kind: str) -> str:
        """Validate an RPC/WS endpoint, ensuring scheme, host, and port are sane."""

        if not url or not isinstance(url, str) or not url.strip():
            raise ValueError(f"{kind.upper()} endpoint is missing")

        parsed = urllib.parse.urlparse(url.strip())
        if not parsed.scheme:
            raise ValueError(f"{kind.upper()} endpoint missing scheme")

        allowed_schemes = {"rpc": {"http", "https"}, "ws": {"ws", "wss"}}
        expected = allowed_schemes.get(kind, set())
        if expected and parsed.scheme.lower() not in expected:
            pretty = ", ".join(sorted(expected))
            raise ValueError(
                f"{kind.upper()} endpoint must use one of [{pretty}] schemes"
            )

        if not parsed.hostname:
            raise ValueError(f"{kind.upper()} endpoint missing host")

        try:
            port = parsed.port
        except ValueError:
            raise ValueError(f"{kind.upper()} endpoint port is invalid") from None
        if port is not None and (port <= 0 or port > 65535):
            raise ValueError(f"{kind.upper()} endpoint port {port} out of range")

        return url.strip()

    # ------------------------------------------------------------------
    # Core discovery API
    # ------------------------------------------------------------------
    async def discover_tokens(
        self,
        *,
        offline: bool = False,
        token_file: Optional[str] = None,
        method: Optional[str] = None,
        mempool_enabled: Optional[bool] = None,
    ) -> List[str]:
        now = time.time()
        ttl = self.cache_ttl
        self.last_fallback_used = False
        if self.limit == 0:
            if not self._disabled_skip_logged:
                logger.info(
                    "Discovery disabled (DISCOVERY_LIMIT=0); skipping discovery cycle",
                )
                self._disabled_skip_logged = True
            self.last_tokens = []
            self.last_details = {}
            self.last_method = None
            return []
        method_override = method is not None
        requested_method = resolve_discovery_method(method)
        if requested_method is None:
            active_method = self.default_method or DEFAULT_DISCOVERY_METHOD
            if method_override and isinstance(method, str) and method.strip():
                logger.warning(
                    "Unsupported discovery method %r; falling back to %s",
                    method,
                    active_method,
                )
        else:
            active_method = requested_method
        mempool_enabled = self._resolve_mempool_enabled(mempool_enabled)
        (
            cached_tokens,
            cached_details_payload,
            cache_ts,
            cache_limit,
            cached_method,
            cached_identity,
            cached_metadata,
        ) = await self._cache_snapshot()
        current_cache_metadata = self._build_cache_metadata(
            method=active_method,
            mempool_enabled=mempool_enabled,
            token_file=token_file,
        )
        current_rpc_identity = current_cache_metadata.get("rpc_identity", "")
        cache_metadata_matches = self._cache_metadata_matches(
            cached_metadata, current_cache_metadata
        )
        if cached_tokens and not cache_metadata_matches:
            logger.debug(
                "DiscoveryAgent cache invalidated due to metadata change (cached=%s, current=%s)",
                cached_metadata,
                current_cache_metadata,
            )
            async with _get_cache_lock():
                _CACHE["tokens"] = []
                _CACHE["details"] = {}
                _CACHE["ts"] = 0.0
                _CACHE["limit"] = 0
                _CACHE["method"] = ""
                _CACHE["rpc_identity"] = ""
                _CACHE["metadata"] = {}
            cached_tokens = []
            cached_details_payload = {}
            cache_ts = 0.0
            cache_limit = 0
            cached_method = ""
            cached_identity = ""
            cached_metadata = {}
        identity_matches = bool(
            (not cached_identity and not current_cache_metadata.get("rpc_identity"))
            or cached_identity == current_cache_metadata.get("rpc_identity")
        )
        cache_age = now - cache_ts if cache_ts > 0.0 else float("inf")
        cache_stale = ttl <= 0 or cache_age >= ttl
        consider_cache = bool(cached_tokens) and cache_metadata_matches
        if offline and not method_override:
            offline_source = "fallback"
            fallback_reason = ""
            if token_file:
                try:
                    tokens = scan_tokens_from_file(token_file, limit=self.limit)
                    offline_source = "file"
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Failed to read token_file %s: %s", token_file, exc)
                    tokens = []
                details: Dict[str, Dict[str, Any]] = {}
            else:
                tokens = self._fallback_tokens()
                fallback_reason = "cache" if tokens else ""
                base_details: Dict[str, Dict[str, Any]] = {}
                if tokens:
                    offline_source = "cache"
                    base_details = self._hydrate_cached_details(
                        cached_details_payload,
                        stale=cache_stale,
                        tokens=tokens,
                    )
                else:
                    offline_source = "static"
                    tokens = self._static_fallback_tokens()
                    if tokens:
                        fallback_reason = "static"
                details = (
                    self._annotate_fallback_details(
                        tokens,
                        base_details or None,
                        reason=fallback_reason,
                    )
                    if tokens and fallback_reason
                    else base_details if base_details else {}
                )
            tokens = self._normalise(tokens)
            tokens, details = await self._apply_social_mentions(
                tokens, details, offline=True
            )
            if self.limit:
                tokens = tokens[: self.limit]
            self.last_tokens = tokens
            self.last_details = details
            self.last_method = "offline"
            detail = f"yield={len(tokens)}"
            if self.limit:
                detail = f"{detail}/{self.limit}"
            detail = f"{detail} method=offline source={offline_source}"
            if fallback_reason:
                detail = f"{detail} fallback={fallback_reason}"
            publish("runtime.log", RuntimeLog(stage="discovery", detail=detail))
            logger.info(
                "DiscoveryAgent offline mode active (source=%s, count=%d)",
                offline_source,
                len(tokens),
            )
            self.last_fallback_used = offline_source in {"cache", "static"}
            return tokens
        if cached_tokens and not identity_matches:
            logger.debug(
                "DiscoveryAgent cache invalidated due to RPC change (cached=%s, current=%s)",
                cached_identity,
                current_rpc_identity,
            )
        cache_ready = (
            consider_cache
            and not cache_stale
            and (
                (cache_limit and cache_limit >= self.limit)
                or len(cached_tokens) >= self.limit
            )
        )
        if cache_ready:
            payload = cached_tokens[: self.limit]
            details = self._hydrate_cached_details(
                cached_details_payload,
                stale=False,
                tokens=payload,
            )
            if cache_limit and cache_limit >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (ttl=%s)", ttl)
            else:
                logger.debug(
                    "DiscoveryAgent: returning cached tokens (limit=%s)", self.limit
                )
            self.last_tokens = list(payload)
            self.last_details = details
            self.last_method = cached_method or active_method
            self.last_fallback_used = False
            return list(payload)

        attempts = self.max_attempts
        details: Dict[str, Dict[str, Any]] = {}
        tokens: List[str] = []

        fallback_used = False
        fallback_reason = ""

        birdeye_configured = bool((self.birdeye_api_key or "").strip())
        if not birdeye_configured and not self._birdeye_notice_logged:
            if mempool_enabled:
                logger.warning(
                    "BirdEye API key missing; continuing discovery with mempool and DEX sources."
                )
            else:
                logger.warning(
                    "BirdEye API key missing and mempool discovery disabled; continuing with DEX and cached/static discovery sources."
                )
            self._birdeye_notice_logged = True

        for attempt in range(attempts):
            tokens, details = await self._discover_once(
                method=active_method,
                offline=offline,
                token_file=token_file,
                mempool_enabled=mempool_enabled,
            )
            tokens = self._normalise(tokens)
            tokens, details = await self._apply_social_mentions(
                tokens, details, offline=offline
            )
            if tokens:
                break
            if attempt < attempts - 1:
                logger.warning(
                    "No tokens discovered (method=%s, attempt=%d/%d)",
                    active_method,
                    attempt + 1,
                    attempts,
                )
                await asyncio.sleep(self.backoff)

        if not tokens:
            tokens = self._fallback_tokens()
            fallback_reason = "cache" if tokens else ""
            base_details: Dict[str, Dict[str, Any]] = {}
            if tokens and fallback_reason == "cache":
                base_details = self._hydrate_cached_details(
                    cached_details_payload,
                    stale=cache_stale,
                    tokens=tokens,
                )
            if not tokens:
                tokens = self._static_fallback_tokens()
                if tokens:
                    fallback_reason = "static"
            details = (
                self._annotate_fallback_details(tokens, base_details, reason=fallback_reason)
                if tokens and fallback_reason
                else base_details if base_details else {}
            )
            fallback_used = bool(tokens)

        self.last_tokens = tokens
        self.last_details = details
        self.last_method = active_method
        self.last_fallback_used = fallback_used

        detail = f"yield={len(tokens)}"
        if self.limit:
            detail = f"{detail}/{self.limit}"
        detail = f"{detail} method={active_method}"
        if self.limit and len(tokens) < self.limit:
            detail = f"{detail} partial"
        if fallback_used and fallback_reason:
            detail = f"{detail} fallback={fallback_reason}"
        publish("runtime.log", RuntimeLog(stage="discovery", detail=detail))
        logger.info(
            "DiscoveryAgent yielded %d tokens via %s", len(tokens), active_method
        )

        if ttl > 0 and tokens and not fallback_used:
            async with _get_cache_lock():
                _CACHE["tokens"] = list(tokens)
                _CACHE["ts"] = now
                _CACHE["limit"] = self.limit
                _CACHE["method"] = active_method
                _CACHE["rpc_identity"] = current_rpc_identity
                _CACHE["details"] = self._prepare_cache_details(details)
                _CACHE["metadata"] = dict(current_cache_metadata)
        elif fallback_used:
            async with _get_cache_lock():
                # Mark the cache as expired so the next call performs live discovery.
                _CACHE["tokens"] = []
                _CACHE["details"] = {}
                _CACHE["ts"] = 0.0
                _CACHE["limit"] = 0
                _CACHE["method"] = ""
                _CACHE["rpc_identity"] = ""
                _CACHE["metadata"] = {}

        return tokens

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    async def _discover_once(
        self,
        *,
        method: str,
        offline: bool,
        token_file: Optional[str],
        mempool_enabled: bool,
    ) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        if (offline or method == "file") and token_file:
            try:
                tokens = scan_tokens_from_file(token_file, limit=self.limit)
                return tokens, {}
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to read token_file %s: %s", token_file, exc)
                return [], {}

        if method == "websocket":
            if not mempool_enabled:
                logger.debug(
                    "Skipping websocket merge because mempool discovery is disabled",
                )
                return await self._fallback_after_merge_failure(
                    mempool_enabled=mempool_enabled
                )
            kwargs: Dict[str, Any] = {
                "mempool_threshold": self.mempool_threshold,
            }
            if self.limit:
                kwargs["limit"] = self.limit
            if self.ws_url:
                kwargs["ws_url"] = self.ws_url
            try:
                signature = inspect.signature(merge_sources)
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                logger.debug("Failed to inspect merge_sources signature: %s", exc)
                supported_params = set(kwargs)
            else:
                supported_params = {
                    name
                    for name, param in signature.parameters.items()
                    if name != "rpc_url"
                    and param.kind
                    in (param.POSITIONAL_OR_KEYWORD, param.KEYWORD_ONLY)
                }
            call_kwargs = {
                name: value
                for name, value in kwargs.items()
                if name in supported_params
            }
            merge_error = False
            try:
                results = await merge_sources(self.rpc_url, **call_kwargs)
            except TypeError:
                raise
            except Exception as exc:
                merge_error = True
                logger.warning("Websocket merge failed: %s", exc)
                results = []
            if isinstance(results, list) and len(results) > self.limit:
                results = results[: self.limit]
            if merge_error:
                return await self._fallback_after_merge_failure(
                    mempool_enabled=mempool_enabled
                )
            details = {}
            for item in results:
                if not isinstance(item, dict):
                    continue
                address = item.get("address")
                if not isinstance(address, str):
                    continue
                address = canonical_mint(address)
                if not validate_mint(address):
                    continue
                details[address] = item
            details = {
                mint: info
                for mint, info in details.items()
                if not self._should_skip_token(mint)
            }
            return list(details.keys()), details

        if method == "mempool":
            if not mempool_enabled:
                logger.debug(
                    "Skipping mempool discovery because it is disabled",
                )
                return [], {}
            tokens, details = await self._collect_mempool()
            filtered = [tok for tok in tokens if not self._should_skip_token(tok)]
            details = {k: v for k, v in details.items() if not self._should_skip_token(k)}
            return filtered, details

        if method == "onchain":
            try:
                rpc_url = self._validate_endpoint_url(self.rpc_url, kind="rpc")
            except ValueError as exc:
                logger.error(
                    "Invalid RPC endpoint %r for on-chain discovery: %s", self.rpc_url, exc
                )
                return [], {}
            try:
                found = await scan_tokens_onchain(
                    rpc_url,
                    return_metrics=True,
                    max_tokens=self.limit,
                )
            except Exception as exc:
                logger.warning("On-chain discovery failed: %s", exc)
                return [], {}
            tokens: List[str] = []
            details: Dict[str, Dict[str, Any]] = {}
            for item in found:
                if isinstance(item, dict):
                    mint = canonical_mint(item.get("address"))
                    if not isinstance(mint, str) or not validate_mint(mint):
                        continue
                    entry = dict(item)
                    entry.setdefault("sources", {"onchain"})
                    details[mint] = entry
                    tokens.append(mint)
                elif isinstance(item, str):
                    canonical = canonical_mint(item)
                    if validate_mint(canonical):
                        tokens.append(canonical)
            tokens = [tok for tok in tokens if not self._should_skip_token(tok)]
            details = {k: v for k, v in details.items() if not self._should_skip_token(k)}
            return tokens, details

        # Default: BirdEye/Helius trending via REST
        raw_tokens = await scan_tokens_async(
            rpc_url=None,
            limit=self.limit,
            enrich=self.enrich_results,
            api_key=self.birdeye_api_key,
        )
        tokens: List[str]
        try:
            tokens = await enrich_tokens_async(raw_tokens, rpc_url=self.rpc_url)
        except Exception as exc:  # pragma: no cover - enrichment best effort
            logger.warning("enrich_tokens_async failed: %s", exc)
            tokens = [tok for tok in raw_tokens if isinstance(tok, str)]
        else:
            if not tokens and raw_tokens:
                tokens = [tok for tok in raw_tokens if isinstance(tok, str)]
        if tokens:
            tokens = [tok for tok in tokens if not self._should_skip_token(tok)]
            return tokens, {}

        logger.warning(
            "Helius/BirdEye discovery returned no tokens; falling back to websocket merge"
        )
        if not mempool_enabled:
            logger.debug(
                "Skipping websocket merge fallback because mempool discovery is disabled",
            )
            return await self._fallback_after_merge_failure(
                mempool_enabled=mempool_enabled
            )
        try:
            merged = await merge_sources(
                self.rpc_url,
                limit=self.limit,
                mempool_threshold=self.mempool_threshold,
                ws_url=self.ws_url,
            )
        except Exception as exc:
            logger.warning("Websocket fallback failed: %s", exc)
            merged = []
        details = {
            item.get("address"): dict(item)
            for item in merged
            if isinstance(item, dict) and isinstance(item.get("address"), str)
        }
        if details:
            details = {k: v for k, v in details.items() if not self._should_skip_token(k)}
            if details:
                return list(details.keys()), details
        return await self._fallback_after_merge_failure(
            mempool_enabled=mempool_enabled
        )

    async def _collect_mempool(self) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        gen = self.stream_mempool_events(threshold=self.mempool_threshold)
        tokens: List[str] = []
        details: Dict[str, Dict[str, Any]] = {}
        timeout = max(float(_MEMPOOL_TIMEOUT), 0.1)
        retries = max(1, int(_MEMPOOL_TIMEOUT_RETRIES))
        max_wait = max(self.mempool_max_wait, 0.0)
        deadline: float | None
        if max_wait > 0.0:
            deadline = time.perf_counter() + max_wait
        else:
            deadline = None
        timeouts = 0
        timed_out = False
        deadline_triggered = False
        try:
            while len(tokens) < self.limit:
                remaining: float | None = None
                if deadline is not None:
                    remaining = max(deadline - time.perf_counter(), 0.0)
                    if remaining <= 0:
                        deadline_triggered = True
                        timed_out = True
                        break
                    logger.debug(
                        "Mempool stream waiting for events (%.2fs remaining)",
                        remaining,
                    )
                    wait_for_timeout = min(timeout, remaining)
                else:
                    wait_for_timeout = timeout

                # ``asyncio.wait_for`` does not allow a negative timeout; guard against
                # floating point drift by clamping to zero which forces an immediate
                # timeout when no item is ready.
                wait_for_timeout = max(wait_for_timeout, 0.0)

                try:
                    item = await asyncio.wait_for(anext(gen), timeout=wait_for_timeout)
                except asyncio.TimeoutError:
                    timeouts += 1
                    if deadline is not None:
                        remaining = max(deadline - time.perf_counter(), 0.0)
                        if remaining <= 0:
                            deadline_triggered = True
                            timed_out = True
                            break
                        logger.debug(
                            "Mempool stream still waiting for events (%.2fs remaining)",
                            max(remaining, 0.0),
                        )
                        if timeouts >= retries:
                            # Reset timeout counter so we can keep waiting while the
                            # configured deadline allows more time.
                            timeouts = 0
                        continue
                    if timeouts >= retries:
                        timed_out = True
                        break
                    continue
                except StopAsyncIteration:
                    break
                except Exception as exc:  # pragma: no cover - defensive
                    logger.debug("Mempool stream failed: %s", exc)
                    break
                else:
                    timeouts = 0

                if not isinstance(item, dict):
                    continue
                address = canonical_mint(item.get("address"))
                if not isinstance(address, str) or not validate_mint(address):
                    continue
                if address not in details:
                    tokens.append(address)
                details[address] = dict(item)
                if len(tokens) >= self.limit:
                    break
        finally:
            if hasattr(gen, "aclose"):
                with contextlib.suppress(Exception):
                    await gen.aclose()  # type: ignore[attr-defined]
        if timed_out and not tokens:
            if deadline_triggered and max_wait:
                wait_window = max_wait
            else:
                wait_window = timeout * retries
            logger.warning(
                "Mempool stream yielded no events within %.2fs; continuing with fallback",
                wait_window,
            )
        return tokens, details

    async def _fallback_after_merge_failure(
        self,
        *,
        mempool_enabled: bool,
    ) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        if mempool_enabled:
            logger.warning("Websocket merge yielded no tokens; trying mempool fallback")
            mem_tokens, mem_details = await self._collect_mempool()
            if mem_tokens:
                return mem_tokens, mem_details
        else:
            logger.warning(
                "Websocket merge yielded no tokens and mempool discovery is disabled",
            )

        logger.warning("All discovery sources empty; returning fallback tokens")
        fallback_tokens = self._fallback_tokens()
        fallback_reason = "cache" if fallback_tokens else ""
        base_details: Dict[str, Dict[str, Any]] = {}
        if fallback_tokens and fallback_reason == "cache":
            (
                _,
                cached_details_payload,
                cache_ts,
                _cache_limit,
                _cache_method,
                _cache_identity,
                _cache_metadata,
            ) = await self._cache_snapshot()
            cache_age = time.time() - cache_ts if cache_ts > 0.0 else float("inf")
            cache_stale = self.cache_ttl <= 0 or cache_age >= self.cache_ttl
            base_details = self._hydrate_cached_details(
                cached_details_payload,
                stale=cache_stale,
                tokens=fallback_tokens,
            )
        elif not fallback_tokens:
            cached_tokens, _, _, _, _, _, _ = await self._cache_snapshot()
            if cached_tokens:
                fallback_tokens = self._static_fallback_tokens()
                if fallback_tokens:
                    fallback_reason = "static"
        fallback = [tok for tok in fallback_tokens if not self._should_skip_token(tok)]
        if fallback and fallback_reason:
            details = self._annotate_fallback_details(
                fallback,
                base_details or None,
                reason=fallback_reason,
            )
        else:
            details = base_details if base_details else {}
        return fallback, details

    def _normalise(self, tokens: Iterable[Any]) -> List[str]:
        seen: set[str] = set()
        filtered: List[str] = []
        for token in tokens:
            if not isinstance(token, str):
                continue
            candidate = canonical_mint(token.strip())
            if not candidate or not validate_mint(candidate):
                continue
            if self._should_skip_token(candidate):
                continue
            if candidate in seen:
                continue
            seen.add(candidate)
            filtered.append(candidate)
            if len(filtered) >= self.limit:
                break
        return filtered

    def _fallback_tokens(self) -> List[str]:
        cached = list(_CACHE.get("tokens", [])) if isinstance(_CACHE.get("tokens"), list) else []
        if not cached:
            return []

        cache_ts = float(_CACHE.get("ts", 0.0))
        cache_age = time.time() - cache_ts if cache_ts > 0.0 else float("inf")
        ttl = float(self.cache_ttl)
        if ttl <= 0.0 or cache_age >= ttl:
            logger.warning(
                "DiscoveryAgent cached tokens expired (age=%.2fs >= ttl=%.2fs); using static fallback",
                cache_age,
                ttl,
            )
            return []

        cached_metadata = self._normalise_cache_metadata(
            _CACHE.get("metadata"),
            int(_CACHE.get("limit", 0)),
            str(_CACHE.get("method") or "").lower(),
            str(_CACHE.get("rpc_identity") or ""),
        )
        current_metadata = self._build_cache_metadata(
            method=cached_metadata.get("method")
            or self.default_method
            or DEFAULT_DISCOVERY_METHOD,
            mempool_enabled=self._resolve_mempool_enabled(None),
            token_file=None,
        )
        if not self._cache_metadata_matches(cached_metadata, current_metadata):
            logger.debug(
                "DiscoveryAgent cached tokens ignored due to metadata mismatch (cached=%s, current=%s)",
                cached_metadata,
                current_metadata,
            )
            return []

        logger.warning("DiscoveryAgent falling back to cached tokens (%d)", len(cached))
        result: List[str] = []
        for tok in cached[: self.limit]:
            canonical = canonical_mint(tok)
            if validate_mint(canonical):
                result.append(canonical)
        return result

    def _static_fallback_tokens(self) -> List[str]:
        logger.warning("DiscoveryAgent using static discovery fallback")
        result: List[str] = []
        for tok in _STATIC_FALLBACK[: self.limit]:
            canonical = canonical_mint(tok)
            if validate_mint(canonical):
                result.append(canonical)
        return result

    def _should_skip_token(self, token: str) -> bool:
        if not self.filter_prefix_11:
            return False
        if token in _FILTER_WHITELIST:
            return False
        return token.startswith("11") and len(token) >= 8

    @staticmethod
    def _load_social_setting(env_var: str, default: int, minimum: int) -> int:
        fallback = max(default, minimum)
        raw_value = os.getenv(env_var)
        if raw_value is None:
            return fallback

        raw = raw_value.strip()
        if not raw:
            return fallback

        try:
            value = int(raw)
        except (TypeError, ValueError):
            logger.warning(
                "Invalid %s=%r; using default %d",
                env_var,
                raw_value,
                fallback,
            )
            return fallback

        if value < minimum:
            logger.warning(
                "%s=%r below minimum (%d); using default %d",
                env_var,
                raw_value,
                minimum,
                fallback,
            )
            return fallback

        return value

    @staticmethod
    def _split_env_list(name: str) -> List[str]:
        raw = os.getenv(name, "")
        if not raw:
            return []
        return [item.strip() for item in raw.split(",") if item.strip()]

    @staticmethod
    def _source_set(value: Any) -> set[str]:
        if isinstance(value, set):
            return {str(v) for v in value if isinstance(v, str) and v}
        if isinstance(value, (list, tuple)):
            return {str(v) for v in value if isinstance(v, str) and v}
        if isinstance(value, str):
            value = value.strip()
            return {value} if value else set()
        return set()

    def _annotate_fallback_details(
        self,
        tokens: Iterable[str],
        base_details: Optional[Dict[str, Dict[str, Any]]],
        *,
        reason: str,
    ) -> Dict[str, Dict[str, Any]]:
        annotated: Dict[str, Dict[str, Any]] = {
            mint: dict(payload)
            for mint, payload in (base_details or {}).items()
            if isinstance(mint, str)
        }
        if not reason:
            return annotated

        for mint in tokens:
            if not isinstance(mint, str):
                continue
            entry = dict(annotated.get(mint, {}))
            entry["fallback_reason"] = reason
            sources = self._source_set(entry.get("sources"))
            sources.add("fallback")
            entry["sources"] = sources
            annotated[mint] = entry

        return annotated

    async def _collect_social_mentions(self) -> Dict[str, Dict[str, Any]]:
        if self.social_limit == 0:
            return {}
        feeds = list(self.news_feeds)
        twitter = list(self.twitter_feeds)
        discord = list(self.discord_feeds)
        if not (feeds or twitter or discord):
            return {}
        limit = self.social_limit or self.limit
        if limit <= 0:
            limit = self.limit
        try:
            mentions = await fetch_token_mentions_async(
                feeds,
                allowed=feeds or None,
                twitter_urls=twitter or None,
                discord_urls=discord or None,
                limit=limit,
                min_mentions=self.social_min_mentions,
                sample_limit=self.social_sample_limit,
            )
        except Exception as exc:  # pragma: no cover - social feeds best effort
            logger.debug("Social mention fetch failed: %s", exc)
            return {}

        results: Dict[str, Dict[str, Any]] = {}
        for payload in mentions:
            token = payload.get("token")
            if not isinstance(token, str):
                continue
            canonical = canonical_mint(token)
            if not validate_mint(canonical):
                continue
            if self._should_skip_token(canonical):
                continue
            mentions_raw = payload.get("mentions")
            try:
                mention_count = int(mentions_raw)
            except (TypeError, ValueError):
                continue
            if mention_count <= 0:
                continue
            entry: Dict[str, Any] = {
                "sources": {"social"},
                "social_mentions": mention_count,
            }
            rank_value = payload.get("rank")
            if isinstance(rank_value, int) and rank_value > 0:
                entry["social_rank"] = rank_value
            samples = payload.get("samples")
            if isinstance(samples, list):
                collected: List[str] = []
                for sample in samples:
                    if not isinstance(sample, str):
                        continue
                    text = sample.strip()
                    if not text:
                        continue
                    collected.append(text)
                    if len(collected) >= self.social_sample_limit:
                        break
                if collected:
                    entry["social_samples"] = collected
            results[canonical] = entry
        return results

    async def _apply_social_mentions(
        self,
        tokens: List[str],
        details: Dict[str, Dict[str, Any]] | None,
        *,
        offline: bool = False,
    ) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        if offline:
            logger.info("DiscoveryAgent skipping social mentions during offline run")
            return tokens, details or {}
        social_details = await self._collect_social_mentions()
        if not social_details:
            return tokens, details or {}

        base_details: Dict[str, Dict[str, Any]] = {}
        for mint, payload in (details or {}).items():
            key = str(mint)
            canonical = canonical_mint(key)
            if canonical and validate_mint(canonical):
                key = canonical
            base_details[key] = dict(payload)
        base_order = {mint: idx for idx, mint in enumerate(tokens)}
        new_order_index: Dict[str, int] = {}

        social_tokens: List[str] = []

        for mint, payload in social_details.items():
            key = str(mint)
            canonical = canonical_mint(key)
            if canonical and validate_mint(canonical):
                key = canonical
            social_tokens.append(key)

            entry = base_details.get(key, {})
            entry = dict(entry)
            sources = self._source_set(entry.get("sources"))
            sources.update(self._source_set(payload.get("sources")))
            sources.add("social")
            entry["sources"] = sources

            mention_val = payload.get("social_mentions")
            try:
                mention_count = int(mention_val)
            except (TypeError, ValueError):
                mention_count = 0
            if mention_count > 0:
                prev = entry.get("social_mentions")
                try:
                    prev_count = int(prev)
                except (TypeError, ValueError):
                    prev_count = 0
                entry["social_mentions"] = max(prev_count, mention_count)

            if "social_rank" in payload and payload["social_rank"]:
                entry["social_rank"] = payload["social_rank"]
            if "social_samples" in payload and payload["social_samples"]:
                entry["social_samples"] = list(payload["social_samples"])

            base_details[key] = entry
            if key not in base_order and key not in new_order_index:
                new_order_index[key] = len(new_order_index)

        def _mention_count(mint: str) -> int:
            value = base_details.get(mint, {}).get("social_mentions")
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        all_tokens = list(dict.fromkeys(tokens + social_tokens))

        def sort_key(mint: str) -> tuple[int, int, str]:
            mentions = _mention_count(mint)
            if mint in base_order:
                order = base_order[mint]
            else:
                order = len(base_order) + new_order_index.get(mint, 0)
            return (-mentions, order, mint)

        ranked_tokens = sorted(all_tokens, key=sort_key)

        if self.limit:
            ranked_tokens = ranked_tokens[: self.limit]

        ranked_tokens = self._normalise(ranked_tokens)

        final_details: Dict[str, Dict[str, Any]] = {
            mint: base_details[mint]
            for mint in ranked_tokens
            if mint in base_details
        }

        return ranked_tokens, final_details


__all__ = [
    "DISCOVERY_METHODS",
    "DEFAULT_DISCOVERY_METHOD",
    "DiscoveryAgent",
    "merge_sources",
    "resolve_discovery_method",
]
