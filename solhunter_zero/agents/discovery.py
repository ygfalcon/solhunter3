from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from ..discovery import merge_sources
from ..event_bus import publish
from ..mempool_scanner import stream_ranked_mempool_tokens_with_depth
from ..scanner_common import scan_tokens_from_file
from ..scanner_onchain import scan_tokens_onchain
from ..schemas import RuntimeLog
from ..token_scanner import enrich_tokens_async, scan_tokens_async

logger = logging.getLogger(__name__)

_CACHE: dict[str, object] = {"tokens": [], "ts": 0.0, "limit": 0, "method": ""}
_STATIC_FALLBACK = [
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v",  # JUP
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
        self.rpc_url = os.getenv(
            "SOLANA_RPC_URL",
            "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
        )
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

        url = rpc_url or self.rpc_url
        thresh = self.mempool_threshold if threshold is None else float(threshold)
        return stream_ranked_mempool_tokens_with_depth(url, threshold=thresh)

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

        publish("runtime.log", RuntimeLog(stage="discovery", detail=f"yield={len(tokens)}"))
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
            results = await merge_sources(
                self.rpc_url,
                mempool_threshold=self.mempool_threshold,
            )
            if isinstance(results, list) and len(results) > self.limit:
                results = results[: self.limit]
            details = {
                item.get("address"): item
                for item in results
                if isinstance(item, dict) and isinstance(item.get("address"), str)
            }
            return list(details.keys()), details

        if method == "mempool":
            return await self._collect_mempool()

        if method == "onchain":
            try:
                found = await scan_tokens_onchain(self.rpc_url, return_metrics=False)
            except Exception as exc:
                logger.warning("On-chain discovery failed: %s", exc)
                return [], {}
            return [tok for tok in found if isinstance(tok, str)], {}

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
        return tokens, {}

    async def _collect_mempool(self) -> tuple[List[str], Dict[str, Dict[str, Any]]]:
        gen = self.stream_mempool_events(threshold=self.mempool_threshold)
        tokens: List[str] = []
        details: Dict[str, Dict[str, Any]] = {}
        try:
            async for item in gen:
                if not isinstance(item, dict):
                    continue
                address = item.get("address")
                if not isinstance(address, str):
                    continue
                if address not in details:
                    tokens.append(address)
                details[address] = dict(item)
                if len(tokens) >= self.limit:
                    break
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Mempool stream failed: %s", exc)
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
            candidate = token.strip()
            if not candidate:
                continue
            if token in seen:
                continue
            seen.add(token)
            filtered.append(token)
            if len(filtered) >= self.limit:
                break
        return filtered

    def _fallback_tokens(self) -> List[str]:
        cached = list(_CACHE.get("tokens", [])) if isinstance(_CACHE.get("tokens"), list) else []
        if cached:
            logger.warning("DiscoveryAgent falling back to cached tokens (%d)", len(cached))
            return cached[: self.limit]
        logger.warning("DiscoveryAgent using static discovery fallback")
        return _STATIC_FALLBACK[: self.limit]


__all__ = [
    "DISCOVERY_METHODS",
    "DEFAULT_DISCOVERY_METHOD",
    "DiscoveryAgent",
    "merge_sources",
    "resolve_discovery_method",
]
