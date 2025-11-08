from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

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

logger = logging.getLogger(__name__)

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

_CACHE: dict[str, object] = {"tokens": [], "ts": 0.0, "limit": 0, "method": ""}
_STATIC_FALLBACK = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
]

DEFAULT_DISCOVERY_METHOD = "helius"

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
                "BIRDEYE_API_KEY missing; discovery will fall back to static tokens"
            )
        self.limit = int(os.getenv("DISCOVERY_LIMIT", "60") or 60)
        self.cache_ttl = max(0.0, float(os.getenv("DISCOVERY_CACHE_TTL", "45") or 45.0))
        self.backoff = max(0.0, float(os.getenv("TOKEN_DISCOVERY_BACKOFF", "1") or 1.0))
        self.max_attempts = max(1, int(os.getenv("TOKEN_DISCOVERY_RETRIES", "2") or 2))
        self.mempool_threshold = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)
        env_method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        self.default_method = env_method or DEFAULT_DISCOVERY_METHOD
        self.last_details: Dict[str, Dict[str, Any]] = {}
        self.last_tokens: List[str] = []
        self.last_method: str | None = None
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
        if not url:
            return None
        text = url.strip()
        if not text:
            return None
        if text.startswith("wss://") or text.startswith("ws://"):
            return text
        if text.startswith("https://"):
            return "wss://" + text[len("https://") :]
        if text.startswith("http://"):
            return "ws://" + text[len("http://") :]
        return text

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
        method_override = method is not None
        requested_method = resolve_discovery_method(method)
        if requested_method is None and offline and not method_override:
            requested_method = "helius"
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
        cached_tokens = (
            list(_CACHE.get("tokens", []))
            if isinstance(_CACHE.get("tokens"), list)
            else []
        )
        cache_limit = int(_CACHE.get("limit", 0))
        cached_method = (_CACHE.get("method") or "").lower()
        if (
            ttl > 0
            and cached_tokens
            and now - float(_CACHE.get("ts", 0.0)) < ttl
            and (not method_override or cached_method == active_method or not cached_method)
        ):
            if cache_limit and cache_limit >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (ttl=%s)", ttl)
                return cached_tokens[: self.limit]
            if len(cached_tokens) >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (limit=%s)", self.limit)
                return cached_tokens[: self.limit]

        attempts = self.max_attempts
        details: Dict[str, Dict[str, Any]] = {}
        tokens: List[str] = []

        for attempt in range(attempts):
            tokens, details = await self._discover_once(
                method=active_method,
                offline=offline,
                token_file=token_file,
            )
            tokens = self._normalise(tokens)
            tokens, details = await self._apply_social_mentions(tokens, details)
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
            details = {}

        self.last_tokens = tokens
        self.last_details = details
        self.last_method = active_method

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

        if ttl > 0 and tokens:
            _CACHE["tokens"] = list(tokens)
            _CACHE["ts"] = now
            _CACHE["limit"] = self.limit
            _CACHE["method"] = active_method

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
            while True:
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
            if isinstance(results, list) and len(results) > self.limit:
                results = results[: self.limit]
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

        logger.warning("Websocket merge yielded no tokens; trying mempool fallback")
        mem_tokens, mem_details = await self._collect_mempool()
        if mem_tokens:
            return mem_tokens, mem_details

        logger.warning("All discovery sources empty; returning static fallback")
        fallback = [tok for tok in self._fallback_tokens() if not self._should_skip_token(tok)]
        return fallback, {}

    async def _collect_mempool(self) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        gen = self.stream_mempool_events(threshold=self.mempool_threshold)
        tokens: List[str] = []
        details: Dict[str, Dict[str, Any]] = {}
        timeout = max(float(_MEMPOOL_TIMEOUT), 0.1)
        retries = max(1, int(_MEMPOOL_TIMEOUT_RETRIES))
        timeouts = 0
        try:
            while len(tokens) < self.limit:
                try:
                    item = await asyncio.wait_for(anext(gen), timeout=timeout)
                except asyncio.TimeoutError:
                    timeouts += 1
                    if timeouts >= retries:
                        logger.debug(
                            "Mempool stream timed out after %d attempts", timeouts
                        )
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
        return tokens, details

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
        if cached:
            logger.warning("DiscoveryAgent falling back to cached tokens (%d)", len(cached))
            result: List[str] = []
            for tok in cached[: self.limit]:
                canonical = canonical_mint(tok)
                if validate_mint(canonical):
                    result.append(canonical)
            return result
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
    ) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
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
