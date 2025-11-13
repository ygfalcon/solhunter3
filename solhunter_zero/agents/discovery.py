from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
import urllib.parse
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from .. import config
from ..token_aliases import canonical_mint, validate_mint
from ..token_discovery import _load_scoring_weights
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

logger = logging.getLogger(__name__)


def resolve_discovery_limit(*, default: int = 60) -> int:
    """Return a validated discovery limit from the environment."""

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
        return int(text)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid DISCOVERY_LIMIT=%r; defaulting to %d",
            raw_value,
            default,
        )
        return default

try:
    from ..token_scanner import enrich_tokens_async, scan_tokens_async
except ImportError as exc:  # pragma: no cover - optional dependency guard
    logger.warning(
        "token_scanner unavailable (%s); falling back to stubbed discovery helpers",
        exc,
    )

    async def scan_tokens_async(
        *,
        rpc_url: str = DEFAULT_SOLANA_RPC,
        limit: int = 50,
        enrich: bool = True,
        api_key: str | None = None,
    ) -> List[str]:
        del rpc_url, api_key
        seeds = list(_STATIC_FALLBACK)
        return seeds[: max(0, limit)]

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
}

_CACHE_LOCK = asyncio.Lock()


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
_STATIC_FALLBACK = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
]

DEFAULT_DISCOVERY_METHOD = "helius"

MIN_DISCOVERY_LIMIT = 1

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


class DiscoveryAgent:
    """Token discovery orchestrator supporting multiple discovery methods."""

    def __init__(self) -> None:
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
        limit = resolve_discovery_limit(default=60)
        if limit < MIN_DISCOVERY_LIMIT:
            logger.warning(
                "Discovery limit %d below minimum %d; using minimum",
                limit,
                MIN_DISCOVERY_LIMIT,
            )
            limit = MIN_DISCOVERY_LIMIT
        self.limit = limit
        self.cache_ttl = max(0.0, float(os.getenv("DISCOVERY_CACHE_TTL", "45") or 45.0))
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
        self.max_attempts = max(1, int(os.getenv("TOKEN_DISCOVERY_RETRIES", "2") or 2))
        self.mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)
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
        self.social_limit = max(
            0, int(os.getenv("DISCOVERY_SOCIAL_LIMIT", "12") or 12)
        )
        self.social_min_mentions = max(
            1, int(os.getenv("DISCOVERY_SOCIAL_MIN_MENTIONS", "2") or 2)
        )
        self.social_sample_limit = max(
            1, int(os.getenv("DISCOVERY_SOCIAL_SAMPLE_LIMIT", "3") or 3)
        )
        self.news_feeds = self._split_env_list("NEWS_FEEDS")
        self.twitter_feeds = self._split_env_list("TWITTER_FEEDS")
        self.discord_feeds = self._split_env_list("DISCORD_FEEDS")
        # Track whether we have already logged a warning about BirdEye being
        # disabled so we avoid noisy duplicate messages when discovery runs in a
        # tight loop.
        self._birdeye_notice_logged = False

    # ------------------------------------------------------------------
    # Detail hydration helpers
    # ------------------------------------------------------------------
    def _hydrate_details_for_tokens(
        self,
        tokens: Iterable[str],
        details: Optional[Dict[str, Dict[str, Any]]],
        *,
        stale: bool,
        cached: bool,
    ) -> Dict[str, Dict[str, Any]]:
        """Return a normalised ``last_details`` payload for ``tokens``."""

        bias, weights = _load_scoring_weights()
        weight_snapshot = dict(weights)

        hydrated: Dict[str, Dict[str, Any]] = {}
        source_details = details or {}

        for token in tokens:
            canonical = canonical_mint(token) if isinstance(token, str) else ""
            if not canonical or not validate_mint(canonical):
                continue

            base_detail: Dict[str, Any] = {}
            if isinstance(source_details, dict):
                for key in (canonical, token):
                    payload = source_details.get(key)
                    if isinstance(payload, dict):
                        base_detail = dict(payload)
                        break

            entry = dict(base_detail)
            entry["score_weights"] = dict(weight_snapshot)
            entry["score_bias"] = bias
            entry["detail_cached"] = bool(cached)
            entry["detail_stale"] = bool(stale)
            hydrated[canonical] = entry

        return hydrated

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
        thresh = self.mempool_threshold if threshold is None else float(threshold)
        return stream_ranked_mempool_tokens_with_depth(url, threshold=thresh)

    # ------------------------------------------------------------------
    # Internal wiring helpers
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Core discovery API
    # ------------------------------------------------------------------
    async def discover_tokens(
        self,
        *,
        offline: bool = False,
        token_file: Optional[str] = None,
        method: Optional[str] = None,
    ) -> List[str]:
        now = time.time()
        ttl = self.cache_ttl
        self.last_fallback_used = False
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
        current_rpc_identity = _rpc_identity(self.rpc_url)
        async with _CACHE_LOCK:
            cached_tokens_raw = _CACHE.get("tokens")
            cache_limit = int(_CACHE.get("limit", 0))
            cached_method = (_CACHE.get("method") or "").lower()
            cached_identity = str(_CACHE.get("rpc_identity") or "")
            cache_ts = float(_CACHE.get("ts", 0.0))

        cached_tokens = (
            list(cached_tokens_raw)
            if isinstance(cached_tokens_raw, list)
            else []
        )
        identity_matches = bool(
            (not cached_identity and not current_rpc_identity)
            or cached_identity == current_rpc_identity
        )
        if offline and not method_override:
            offline_source = "fallback"
            cached_details = {mint: dict(payload) for mint, payload in self.last_details.items()}
            if token_file:
                try:
                    tokens = scan_tokens_from_file(token_file, limit=self.limit)
                    offline_source = "file"
                except Exception as exc:  # pragma: no cover - defensive
                    logger.warning("Failed to read token_file %s: %s", token_file, exc)
                    tokens = []
                cached_details = {}
            else:
                tokens = self._fallback_tokens()
                if tokens:
                    offline_source = "cache"
                else:
                    offline_source = "static"
                    tokens = self._static_fallback_tokens()
            tokens = self._normalise(tokens)
            tokens, details = await self._apply_social_mentions(
                tokens, cached_details, offline=True
            )
            if self.limit:
                tokens = tokens[: self.limit]
            combined_details: Dict[str, Dict[str, Any]] = {}
            for source in (cached_details, details or {}):
                for key, value in source.items():
                    if isinstance(value, dict):
                        combined_details[key] = dict(value)
            hydrated_details = self._hydrate_details_for_tokens(
                tokens,
                combined_details,
                stale=True,
                cached=True,
            )
            self.last_tokens = tokens
            self.last_details = hydrated_details
            self.last_method = "offline"
            detail = f"yield={len(tokens)}"
            if self.limit:
                detail = f"{detail}/{self.limit}"
            detail = f"{detail} method=offline source={offline_source}"
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
        if (
            ttl > 0
            and cached_tokens
            and now - cache_ts < ttl
            and (not method_override or cached_method == active_method or not cached_method)
            and identity_matches
        ):
            if cache_limit and cache_limit >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (ttl=%s)", ttl)
                selected = cached_tokens[: self.limit]
                hydrated = self._hydrate_details_for_tokens(
                    selected,
                    self.last_details,
                    stale=False,
                    cached=True,
                )
                self.last_tokens = selected
                self.last_details = hydrated
                self.last_method = cached_method or active_method
                self.last_fallback_used = False
                return selected
            if len(cached_tokens) >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (limit=%s)", self.limit)
                selected = cached_tokens[: self.limit]
                hydrated = self._hydrate_details_for_tokens(
                    selected,
                    self.last_details,
                    stale=False,
                    cached=True,
                )
                self.last_tokens = selected
                self.last_details = hydrated
                self.last_method = cached_method or active_method
                self.last_fallback_used = False
                return selected

        attempts = self.max_attempts
        details: Dict[str, Dict[str, Any]] = {}
        tokens: List[str] = []

        mempool_flag = os.getenv("DISCOVERY_ENABLE_MEMPOOL")
        mempool_enabled = True
        if mempool_flag is not None:
            mempool_enabled = mempool_flag.strip().lower() in {"1", "true", "yes", "on"}
        fallback_used = False

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
            previous_details = {
                mint: dict(payload)
                for mint, payload in self.last_details.items()
                if isinstance(payload, dict)
            }
            tokens = self._fallback_tokens()
            if tokens:
                details = self._hydrate_details_for_tokens(
                    tokens,
                    previous_details,
                    stale=True,
                    cached=True,
                )
                fallback_used = True
            else:
                tokens = self._static_fallback_tokens()
                details = self._hydrate_details_for_tokens(
                    tokens,
                    previous_details,
                    stale=True,
                    cached=True,
                )
                fallback_used = True
        else:
            details = self._hydrate_details_for_tokens(
                tokens,
                details,
                stale=False,
                cached=False,
            )

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
        publish("runtime.log", RuntimeLog(stage="discovery", detail=detail))
        logger.info(
            "DiscoveryAgent yielded %d tokens via %s", len(tokens), active_method
        )

        if ttl > 0 and tokens and not fallback_used:
            async with _CACHE_LOCK:
                _CACHE["tokens"] = list(tokens)
                _CACHE["ts"] = now
                _CACHE["limit"] = self.limit
                _CACHE["method"] = active_method
                _CACHE["rpc_identity"] = current_rpc_identity
        elif fallback_used:
            async with _CACHE_LOCK:
                # Mark the cache as expired so the next call performs live discovery.
                _CACHE["ts"] = 0.0
                _CACHE["limit"] = 0
                _CACHE["method"] = ""
                _CACHE["rpc_identity"] = ""

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
    ) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        if (offline or method == "file") and token_file:
            try:
                tokens = scan_tokens_from_file(token_file, limit=self.limit)
                return tokens, {}
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("Failed to read token_file %s: %s", token_file, exc)
                return [], {}

        if method == "websocket":
            kwargs: Dict[str, Any] = {
                "mempool_threshold": self.mempool_threshold,
            }
            if self.limit:
                kwargs["limit"] = self.limit
            if self.ws_url:
                kwargs["ws_url"] = self.ws_url
            call_kwargs = dict(kwargs)
            merge_error = False
            results: Any = []
            while True:
                try:
                    try:
                        results = await merge_sources(self.rpc_url, **call_kwargs)
                        break
                    except TypeError as exc:
                        message = str(exc)
                        handled = False
                        for key in ("ws_url", "limit"):
                            if key in call_kwargs and key in message:
                                call_kwargs.pop(key, None)
                                handled = True
                                break
                        if not handled:
                            raise
                except TypeError:
                    raise
                except Exception as exc:
                    merge_error = True
                    logger.warning("Websocket merge failed: %s", exc)
                    results = []
                    break
            if isinstance(results, list) and len(results) > self.limit:
                results = results[: self.limit]
            if merge_error:
                return await self._fallback_after_merge_failure()
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
            tokens, details = await self._collect_mempool()
            filtered = [tok for tok in tokens if not self._should_skip_token(tok)]
            details = {k: v for k, v in details.items() if not self._should_skip_token(k)}
            return filtered, details

        if method == "onchain":
            try:
                found = await scan_tokens_onchain(
                    self.rpc_url,
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
            rpc_url=self.rpc_url,
            limit=self.limit,
            enrich=False,
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
        return await self._fallback_after_merge_failure()

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
    ) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        logger.warning("Websocket merge yielded no tokens; trying mempool fallback")
        mem_tokens, mem_details = await self._collect_mempool()
        if mem_tokens:
            return mem_tokens, mem_details

        logger.warning("All discovery sources empty; returning fallback tokens")
        fallback_tokens = self._fallback_tokens()
        cached = _CACHE.get("tokens")
        if not fallback_tokens and isinstance(cached, list) and cached:
            fallback_tokens = self._static_fallback_tokens()
        fallback = [tok for tok in fallback_tokens if not self._should_skip_token(tok)]
        return fallback, {}

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

        cached_identity = str(_CACHE.get("rpc_identity") or "")
        current_identity = _rpc_identity(self.rpc_url)
        identity_matches = bool(
            (not cached_identity and not current_identity)
            or cached_identity == current_identity
        )
        if not identity_matches:
            logger.debug(
                "DiscoveryAgent cached tokens ignored due to RPC mismatch (cached=%s, current=%s)",
                cached_identity,
                current_identity,
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

        base_details: Dict[str, Dict[str, Any]] = {
            mint: dict(payload) for mint, payload in (details or {}).items()
        }
        base_order = {mint: idx for idx, mint in enumerate(tokens)}
        new_order_index: Dict[str, int] = {}

        for mint, payload in social_details.items():
            entry = base_details.get(mint, {})
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

            base_details[mint] = entry
            if mint not in base_order and mint not in new_order_index:
                new_order_index[mint] = len(new_order_index)

        def _mention_count(mint: str) -> int:
            value = base_details.get(mint, {}).get("social_mentions")
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        all_tokens = list(dict.fromkeys(tokens + list(social_details)))

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
