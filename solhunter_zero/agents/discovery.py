from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional

from .. import config
from ..discovery import merge_sources
from ..event_bus import publish
from ..mempool_scanner import stream_ranked_mempool_tokens_with_depth
from ..scanner_common import (
    DEFAULT_SOLANA_RPC,
    DEFAULT_SOLANA_WS,
    STATIC_FALLBACK_TOKENS,
    scan_tokens_from_file,
)
from ..scanner_onchain import scan_tokens_onchain
from ..schemas import RuntimeLog
from ..token_scanner import enrich_tokens_async, scan_tokens_async

logger = logging.getLogger(__name__)

_MEMPOOL_STREAM_TIMEOUT = float(os.getenv("DISCOVERY_MEMPOOL_TIMEOUT", "2.5") or 2.5)
_MEMPOOL_STREAM_TIMEOUT_RETRIES = max(
    1, int(os.getenv("DISCOVERY_MEMPOOL_TIMEOUT_RETRIES", "3") or 3)
)

_CACHE: dict[str, object] = {
    "tokens": [],
    "ts": 0.0,
    "limit": 0,
    "method": "",
    "token_file": None,
    "token_file_mtime": None,
}
_STATIC_FALLBACK = list(STATIC_FALLBACK_TOKENS)

DEFAULT_DISCOVERY_METHOD = "helius"
OFFLINE_DEFAULT_METHOD = "file"
_OFFLINE_SAFE_METHODS = frozenset({OFFLINE_DEFAULT_METHOD})

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
        use_cache: bool = True,
    ) -> List[str]:
        now = time.time()
        ttl = self.cache_ttl
        method_override = method is not None
        requested_method = resolve_discovery_method(method)
        if requested_method is None:
            active_method = self.default_method or DEFAULT_DISCOVERY_METHOD
            if offline and not method_override:
                active_method = OFFLINE_DEFAULT_METHOD
            if method_override and isinstance(method, str) and method.strip():
                logger.warning(
                    "Unsupported discovery method %r; falling back to %s",
                    method,
                    active_method,
                )
        else:
            active_method = requested_method

        if offline and active_method not in _OFFLINE_SAFE_METHODS:
            if method_override and isinstance(method, str) and method.strip():
                logger.warning(
                    "Offline discovery cannot use %s; falling back to %s",
                    active_method,
                    OFFLINE_DEFAULT_METHOD,
                )
            active_method = OFFLINE_DEFAULT_METHOD
        normalized_token_path: str | None = None
        token_mtime: float | None = None

        if use_cache:
            cached_tokens = (
                list(_CACHE.get("tokens", []))
                if isinstance(_CACHE.get("tokens"), list)
                else []
            )
            cache_limit = int(_CACHE.get("limit", 0))
            cached_method = (_CACHE.get("method") or "").lower()

            if token_file:
                token_path = Path(token_file).expanduser()
                try:
                    resolved = token_path.resolve(strict=False)
                except (OSError, RuntimeError):
                    resolved = token_path
                normalized_token_path = str(resolved)
                try:
                    token_mtime = resolved.stat().st_mtime
                except OSError:
                    try:
                        token_mtime = token_path.stat().st_mtime
                    except OSError:
                        token_mtime = None

            cached_token_file = _CACHE.get("token_file")
            cached_token_mtime = _CACHE.get("token_file_mtime")
            cache_matches_token_file = True
            if normalized_token_path != cached_token_file:
                if normalized_token_path or cached_token_file:
                    cache_matches_token_file = False
            if cache_matches_token_file:
                if (
                    cached_token_mtime is not None
                    and token_mtime is not None
                    and token_mtime != cached_token_mtime
                ):
                    cache_matches_token_file = False
                elif (cached_token_mtime is None) != (token_mtime is None):
                    cache_matches_token_file = False

            if (
                ttl > 0
                and cached_tokens
                and now - float(_CACHE.get("ts", 0.0)) < ttl
                and (
                    not method_override
                    or cached_method == active_method
                    or not cached_method
                )
                and cache_matches_token_file
            ):
                if cache_limit and cache_limit >= self.limit:
                    logger.debug(
                        "DiscoveryAgent: returning cached tokens (ttl=%s)", ttl
                    )
                    return cached_tokens[: self.limit]
                if len(cached_tokens) >= self.limit:
                    logger.debug(
                        "DiscoveryAgent: returning cached tokens (limit=%s)",
                        self.limit,
                    )
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
            tokens = self._normalise(self._fallback_tokens())
            details = {}

        self.last_tokens = tokens
        self.last_details = details
        self.last_method = active_method

        publish("runtime.log", RuntimeLog(stage="discovery", detail=f"yield={len(tokens)}"))
        logger.info(
            "DiscoveryAgent yielded %d tokens via %s", len(tokens), active_method
        )

        if use_cache and ttl > 0 and tokens:
            _CACHE["tokens"] = list(tokens)
            _CACHE["ts"] = now
            _CACHE["limit"] = self.limit
            _CACHE["method"] = active_method
            _CACHE["token_file"] = normalized_token_path
            _CACHE["token_file_mtime"] = token_mtime

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

        if offline:
            if method != "file" or not token_file:
                return self._fallback_tokens(), {}

        if method == "websocket":
            results = await merge_sources(
                self.rpc_url,
                limit=self.limit,
                mempool_threshold=self.mempool_threshold,
                ws_url=self.ws_url,
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
        timeouts = 0
        timeout = max(_MEMPOOL_STREAM_TIMEOUT, 0.1)
        try:
            while len(tokens) < self.limit:
                try:
                    item = await asyncio.wait_for(anext(gen), timeout=timeout)
                except asyncio.TimeoutError:
                    timeouts += 1
                    if timeouts >= _MEMPOOL_STREAM_TIMEOUT_RETRIES:
                        logger.warning(
                            "Mempool stream timed out after %d attempts", timeouts
                        )
                        publish(
                            "runtime.log",
                            RuntimeLog(
                                stage="discovery",
                                detail=f"mempool-timeout:{timeouts}",
                                level="WARN",
                            ),
                        )
                        break
                    logger.debug(
                        "Mempool stream timeout (attempt %d/%d)",
                        timeouts,
                        _MEMPOOL_STREAM_TIMEOUT_RETRIES,
                    )
                    await asyncio.sleep(0)
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
                address = item.get("address")
                if not isinstance(address, str):
                    continue
                if address not in details:
                    tokens.append(address)
                details[address] = dict(item)
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
