from __future__ import annotations

import asyncio
import copy
import logging
import os
import time
from typing import Any, Dict, Iterable, List, Sequence
from urllib.parse import parse_qs, urlparse

import aiohttp

# Hard-coded Helius defaults (per your request)
DEFAULT_SOLANA_RPC = "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"

BIRDEYE_BASE = "https://public-api.birdeye.so"

HELIUS_BASE = os.getenv("HELIUS_PRICE_BASE_URL", "https://api.helius.xyz")
HELIUS_TREND_PATH = os.getenv("HELIUS_PRICE_PATH", "/v1/trending-tokens")


def _extract_helius_key() -> str:
    env_key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if env_key:
        return env_key
    rpc_url = os.getenv("SOLANA_RPC_URL") or DEFAULT_SOLANA_RPC
    try:
        parsed = urlparse(rpc_url)
    except Exception:
        return ""
    query = parse_qs(parsed.query)
    if not query:
        return ""
    api_key = query.get("api-key") or query.get("apiKey")
    if api_key:
        return api_key[0]
    return ""

logger = logging.getLogger(__name__)


class BirdeyeFatalError(RuntimeError):
    """Exception raised when Birdeye returns a non-retryable client error."""

    def __init__(
        self,
        status: int,
        message: str,
        *,
        body: str | None = None,
        throttle: bool = False,
    ) -> None:
        self.status = status
        self.body = body
        self.throttle = throttle
        super().__init__(message)

    def __str__(self) -> str:  # pragma: no cover - defensive formatting
        base = super().__str__()
        if self.body and self.body not in base:
            return f"{base} ({self.body})"
        return base


class BirdeyeThrottleError(BirdeyeFatalError):
    """Dedicated exception for Birdeye throttling responses."""

    def __init__(self, status: int, message: str, *, body: str | None = None) -> None:
        super().__init__(status, message, body=body, throttle=True)

FAST_MODE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
_BIRDEYE_TIMEOUT = float(os.getenv("FAST_BIRDEYE_TIMEOUT", "6.0")) if FAST_MODE else 10.0
_BIRDEYE_PAGE_DELAY = float(os.getenv("FAST_BIRDEYE_PAGE_DELAY", "0.35")) if FAST_MODE else 1.1
_HELIUS_TIMEOUT = float(os.getenv("FAST_HELIUS_TIMEOUT", "4.0")) if FAST_MODE else 8.0

TRENDING_METADATA: Dict[str, Dict[str, Any]] = {}

_FAILURE_THRESHOLD = max(1, int(os.getenv("TOKEN_SCAN_FAILURE_THRESHOLD", "3")))
_FAILURE_COOLDOWN = max(0.0, float(os.getenv("TOKEN_SCAN_FAILURE_COOLDOWN", "45")))
_FATAL_FAILURE_COOLDOWN = max(
    _FAILURE_COOLDOWN,
    float(os.getenv("TOKEN_SCAN_FATAL_COOLDOWN", "180")),
)
_THROTTLE_COOLDOWN = max(
    _FATAL_FAILURE_COOLDOWN,
    float(os.getenv("TOKEN_SCAN_THROTTLE_COOLDOWN", "300")),
)

_MIN_SCAN_INTERVAL = max(0.0, float(os.getenv("TOKEN_SCAN_MIN_INTERVAL", "45")))

_LAST_TRENDING_RESULT: Dict[str, Any] = {"mints": [], "metadata": {}, "timestamp": 0.0}
_FAILURE_COUNT = 0
_COOLDOWN_UNTIL = 0.0

__all__ = [
    "scan_tokens_async",
    "enrich_tokens_async",
    "TRENDING_METADATA",
]


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _normalize_helius_item(raw: Dict[str, Any], *, rank: int) -> Dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    address = None
    candidates: List[Any] = [raw]
    token_info = raw.get("token") or raw.get("tokenInfo") or raw.get("tokenMetadata")
    if isinstance(token_info, dict):
        candidates.append(token_info)
    market_info = raw.get("market") or raw.get("metadata")
    if isinstance(market_info, dict):
        candidates.append(market_info)

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        for key in ("mint", "address", "token", "mintAddress", "tokenAddress"):
            val = candidate.get(key)
            if isinstance(val, str) and len(val) > 10:
                address = val
                break
        if address:
            break
    if not address:
        return None

    entry: Dict[str, Any] = {
        "address": address,
        "source": "helius",
        "rank": rank,
    }

    symbol_sources: Iterable[Dict[str, Any]] = [raw]
    if isinstance(token_info, dict):
        symbol_sources = list(symbol_sources) + [token_info]
    for source in symbol_sources:
        for key in ("symbol", "ticker"):
            val = source.get(key)
            if isinstance(val, str) and val:
                entry.setdefault("symbol", val)
                break
        if "symbol" in entry:
            break

    name = None
    for source in symbol_sources:
        candidate = source.get("name") or source.get("tokenName")
        if isinstance(candidate, str) and candidate:
            name = candidate
            break
    if name:
        entry.setdefault("name", name)

    metrics = raw.get("metrics")
    if isinstance(metrics, dict):
        candidates.append(metrics)

    float_fields = {
        "score": ("score", "rankingScore", "momentumScore"),
        "volume": (
            "volume",
            "volume24h",
            "volume_24h",
            "volumeUSD",
            "volumeUsd",
        ),
        "market_cap": ("marketCap", "market_cap", "marketCapUsd", "marketCapUSD"),
        "price_change": (
            "priceChange",
            "priceChange24h",
            "priceChange24hPercent",
            "change24h",
            "priceChangePercent",
        ),
    }

    for dest, keys in float_fields.items():
        for key in keys:
            for source in candidates:
                if not isinstance(source, dict):
                    continue
                value = source.get(key)
                number = _coerce_float(value)
                if number is not None:
                    entry[dest] = number
                    break
            if dest in entry:
                break

    price = None
    for source in candidates:
        if not isinstance(source, dict):
            continue
        price = _coerce_float(source.get("price") or source.get("priceUsd") or source.get("price_usd"))
        if price is not None:
            break
    if price is not None:
        entry["price"] = price

    liquidity = None
    for source in candidates:
        if not isinstance(source, dict):
            continue
        liquidity = _coerce_float(source.get("liquidity") or source.get("liquidityUsd"))
        if liquidity is not None:
            break
    if liquidity is not None:
        entry["liquidity"] = liquidity

    entry["sources"] = ["helius"]
    entry["raw"] = raw
    return entry


async def _birdeye_trending(
    session: aiohttp.ClientSession,
    api_key: str,
    *,
    limit: int = 20,
    offset: int = 0,
) -> List[str]:
    """Fetch trending token mints from Birdeye's current trending endpoint."""
    api_key = api_key or ""
    if not api_key:
        logger.debug("Birdeye API key missing; skipping trending fetch")
        return []

    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-chain": os.getenv("BIRDEYE_CHAIN", "solana"),
    }
    timeframe = (
        os.getenv("BIRDEYE_TRENDING_TIMEFRAME")
        or os.getenv("BIRDEYE_TIMEFRAME")
        or "24h"
    )
    trend_type = (
        os.getenv("BIRDEYE_TRENDING_TYPE")
        or os.getenv("BIRDEYE_TYPE")
        or "trending"
    )

    params = {
        "offset": int(offset),
        "limit": int(limit),
        "timeframe": timeframe,
        "type": trend_type,
    }
    url = f"{BIRDEYE_BASE}/defi/trending"

    last_exc: Exception | None = None

    throttle_markers = (
        "compute units usage limit exceeded",
        "request limit exceeded",
        "rate limit exceeded",
        "too many requests",
        "plan limit reached",
        "exceeded your request limit",
    )

    for attempt in range(1, 4):
        try:
            async with session.get(
                url,
                headers=headers,
                params=params,
                timeout=_BIRDEYE_TIMEOUT,
            ) as resp:
                detail: str | None = None
                if 400 <= resp.status < 500:
                    try:
                        detail = (await resp.text())[:400]
                    except Exception:  # pragma: no cover - defensive guard
                        detail = ""
                    detail_lower = detail.lower() if detail else ""
                    is_throttle = resp.status == 429 or any(
                        marker in detail_lower for marker in throttle_markers
                    )
                    if is_throttle:
                        message = f"Birdeye throttle {resp.status}"
                        if resp.reason:
                            message = f"{message}: {resp.reason}"
                        if detail:
                            detail_clean = " ".join(detail.split())
                            if detail_clean:
                                message = f"{message} - {detail_clean}"
                        raise BirdeyeThrottleError(
                            resp.status,
                            message,
                            body=detail,
                        )
                    message = f"Birdeye client error {resp.status}"
                    if resp.reason:
                        message = f"{message}: {resp.reason}"
                    if detail:
                        detail_clean = " ".join(detail.split())
                        if detail_clean:
                            message = f"{message} - {detail_clean}"
                    raise BirdeyeFatalError(resp.status, message, body=detail)
                resp.raise_for_status()
                data = await resp.json(content_type=None)
                if not isinstance(data, dict):
                    logger.warning("Unexpected Birdeye payload type %s", type(data).__name__)
                    return []
                payload = data.get("data") or data.get("result") or data
                items: Any = []
                if isinstance(payload, dict):
                    for key in ("items", "tokens", "data", "results"):
                        candidate = payload.get(key)
                        if isinstance(candidate, (list, dict)):
                            items = candidate
                            break
                    else:
                        items = payload
                else:
                    items = payload
                if isinstance(items, dict):
                    for key in ("items", "tokens", "data", "results"):
                        candidate = items.get(key)
                        if isinstance(candidate, list):
                            items = candidate
                            break
                    else:
                        items = list(items.values())
                if not isinstance(items, list):
                    return []
                mints: List[str] = []
                for it in items:
                    addr = None
                    if isinstance(it, dict):
                        addr = (
                            it.get("address")
                            or it.get("mint")
                            or it.get("mintAddress")
                            or it.get("tokenAddress")
                        )
                        if not addr:
                            token_info = it.get("token") or it.get("tokenInfo")
                            if isinstance(token_info, dict):
                                addr = (
                                    token_info.get("address")
                                    or token_info.get("mint")
                                    or token_info.get("mintAddress")
                                    or token_info.get("tokenAddress")
                                )
                    elif isinstance(it, (list, tuple)) and it:
                        addr = it[0]
                    if isinstance(addr, str):
                        mints.append(addr)
                return mints
        except BirdeyeFatalError:
            raise
        except Exception as exc:
            last_exc = exc
            logger.warning("Birdeye trending request failed (%d/3): %s", attempt, exc)
            await asyncio.sleep(0.1)

    if last_exc:
        logger.debug("Birdeye last_exc: %s", last_exc)
    return []


async def _helius_trending(
    session: aiohttp.ClientSession,
    *,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """Fetch trending tokens from Helius' price or cached webhook feed.

    Returns dictionaries with an ``address`` key plus optional metadata such as
    ``score`` or ``volume`` so callers can prioritise Helius candidates.
    """

    if limit <= 0:
        return []

    url = os.getenv("HELIUS_TRENDING_URL", "").strip()
    params: Dict[str, Any] = {}
    headers = {"accept": "application/json"}
    method = os.getenv("HELIUS_TRENDING_METHOD", "POST").upper()
    sort_override = os.getenv("HELIUS_PRICE_SORT") or os.getenv("HELIUS_TREND_SORT")
    timeframe_override = (
        os.getenv("HELIUS_PRICE_TIMEFRAME")
        or os.getenv("HELIUS_TREND_TIMEFRAME")
        or os.getenv("HELIUS_TREND_TIMEFRAME_DEFAULT")
    )
    api_key = _extract_helius_key()

    if api_key:
        params.setdefault("api-key", api_key)
        headers.setdefault("Authorization", f"Bearer {api_key}")

    if not url:
        base = HELIUS_BASE.rstrip("/")
        path = HELIUS_TREND_PATH
        if not path.startswith("/"):
            path = "/" + path
        url = f"{base}{path}"

    if not url:
        logger.debug("Helius trending URL missing; skipping fetch")
        return []

    request_payload: Dict[str, Any] | None = None
    if method == "POST":
        request_payload = {
            "timeframe": (timeframe_override or "24h"),
            "limit": int(limit),
        }
        # Cursor/offset preference defaults to zero; allow overriding via env.
        cursor = os.getenv("HELIUS_TREND_CURSOR")
        if cursor:
            request_payload["cursor"] = cursor
        else:
            request_payload["offset"] = int(os.getenv("HELIUS_TREND_OFFSET", "0"))

        network = os.getenv("HELIUS_TREND_NETWORK")
        if network:
            request_payload["network"] = network

        if sort_override:
            request_payload["sortBy"] = sort_override

        include_nsfw = os.getenv("HELIUS_TREND_INCLUDE_NSFW")
        if include_nsfw:
            request_payload["includeNsfw"] = include_nsfw.lower() in {"1", "true", "yes", "on"}

        categories = os.getenv("HELIUS_TREND_CATEGORIES")
        if categories:
            request_payload["categories"] = [
                cat.strip()
                for cat in categories.split(",")
                if cat.strip()
            ]
        # remove None values for cleanliness
        request_payload = {
            key: value
            for key, value in request_payload.items()
            if value is not None and value != ""
        }
    else:
        params.setdefault("limit", int(limit))
        if sort_override:
            params["sortBy"] = sort_override
        if timeframe_override:
            params["timeframe"] = timeframe_override

    try:
        request_coro: Any
        if method == "POST":
            request_coro = session.post(
                url,
                json=request_payload or None,
                params={"api-key": params.get("api-key")} if params.get("api-key") else None,
                headers=headers,
                timeout=_HELIUS_TIMEOUT,
            )
        else:
            request_coro = session.get(
                url,
                params=params or None,
                headers=headers,
                timeout=_HELIUS_TIMEOUT,
            )

        async with request_coro as resp:
            resp.raise_for_status()
            payload = await resp.json(content_type=None)
    except Exception as exc:
        logger.debug("Helius trending request failed: %s", exc)
        return []

    tokens: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _walk(obj: Any) -> None:
        if len(tokens) >= limit:
            return
        if isinstance(obj, list):
            for item in obj:
                if len(tokens) >= limit:
                    break
                _walk(item)
            return
        if isinstance(obj, dict):
            normalized = _normalize_helius_item(obj, rank=len(tokens))
            if normalized:
                addr = normalized["address"]
                if addr not in seen:
                    seen.add(addr)
                    tokens.append(normalized)
                    if len(tokens) >= limit:
                        return
            for key in ("tokens", "items", "data", "results"):
                value = obj.get(key)
                if isinstance(value, (list, dict)):
                    _walk(value)
            for value in obj.values():
                if len(tokens) >= limit:
                    break
                if isinstance(value, (list, dict)):
                    _walk(value)

    _walk(payload)

    return tokens[:limit]


async def scan_tokens_async(
    *,
    rpc_url: str = DEFAULT_SOLANA_RPC,
    limit: int = 50,
    enrich: bool = True,   # kept for compatibility; enrichment is separate call
    api_key: str | None = None,
) -> List[str]:
    """
    Pull trending mints preferring Helius price data, falling back to BirdEye.
    If both sources fail, return a small static set so the loop can proceed.
    """
    _ = rpc_url  # reserved for future use; enrichment uses this parameter
    global _FAILURE_COUNT, _COOLDOWN_UNTIL

    requested = max(1, int(limit))
    api_key = api_key or ""

    now = time.time()

    def _apply_cached() -> List[str]:
        cached_mints = list(_LAST_TRENDING_RESULT.get("mints") or [])
        if not cached_mints:
            return []
        TRENDING_METADATA.clear()
        cached_meta = _LAST_TRENDING_RESULT.get("metadata") or {}
        for mint, meta in cached_meta.items():
            TRENDING_METADATA[mint] = copy.deepcopy(meta)
        return cached_mints[:requested]

    def _apply_static() -> List[str]:
        TRENDING_METADATA.clear()
        fallback = [
            "So11111111111111111111111111111111111111112",  # SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
            "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v",  # JUP
        ]
        for mint in fallback:
            TRENDING_METADATA.setdefault(
                mint,
                {
                    "address": mint,
                    "source": "static",
                    "sources": ["static"],
                    "rank": len(TRENDING_METADATA),
                },
            )
        return fallback[:requested]

    if _COOLDOWN_UNTIL and now < _COOLDOWN_UNTIL:
        cached = _apply_cached()
        if cached:
            _LAST_TRENDING_RESULT["timestamp"] = now
            return cached
        return _apply_static()

    last_ts = float(_LAST_TRENDING_RESULT.get("timestamp") or 0.0)
    if _MIN_SCAN_INTERVAL and last_ts and (now - last_ts) < _MIN_SCAN_INTERVAL:
        cached = _apply_cached()
        if cached:
            _LAST_TRENDING_RESULT["timestamp"] = now
            return cached

    mints: List[str] = []
    TRENDING_METADATA.clear()
    success = False

    forced_cooldown_reason: str | None = None
    forced_cooldown_seconds: float | None = None

    async with aiohttp.ClientSession() as session:
        try:
            helius_tokens = await _helius_trending(session, limit=requested)
        except Exception as exc:
            logger.debug("Helius trending threw: %s", exc)
            helius_tokens = []

        for item in helius_tokens:
            if not isinstance(item, dict):
                continue
            mint = item.get("address")
            if not isinstance(mint, str):
                continue
            if mint in mints:
                existing = TRENDING_METADATA.setdefault(mint, dict(item))
                if isinstance(existing, dict):
                    sources = existing.setdefault("sources", [])
                    if isinstance(sources, list) and "helius" not in sources:
                        sources.append("helius")
                continue
            mints.append(mint)
            meta = dict(item)
            meta.setdefault("sources", ["helius"])
            TRENDING_METADATA[mint] = meta
            if len(mints) >= requested:
                break
        if mints:
            success = True

        offset = 0
        while len(mints) < requested:
            page_size = min(20, requested - len(mints))
            try:
                batch = await _birdeye_trending(
                    session,
                    api_key,
                    limit=page_size,
                    offset=offset,
                )
            except BirdeyeThrottleError as exc:
                reason = str(exc)
                logger.warning(
                    "Birdeye trending throttle; entering extended cooldown: %s",
                    reason,
                )
                forced_cooldown_reason = reason or f"Birdeye status {exc.status}"
                forced_cooldown_seconds = max(
                    _THROTTLE_COOLDOWN,
                    _FATAL_FAILURE_COOLDOWN,
                    _FAILURE_COOLDOWN,
                )
                break
            except BirdeyeFatalError as exc:
                reason = str(exc)
                logger.warning(
                    "Birdeye trending fatal error; entering cooldown: %s",
                    reason,
                )
                forced_cooldown_reason = reason or f"Birdeye status {exc.status}"
                if exc.throttle:
                    forced_cooldown_seconds = max(
                        _FATAL_FAILURE_COOLDOWN,
                        _FAILURE_COOLDOWN,
                    )
                break
            if not batch:
                break
            for mint in batch:
                if not isinstance(mint, str):
                    continue
                if mint in mints:
                    existing = TRENDING_METADATA.setdefault(
                        mint,
                        {
                            "address": mint,
                            "source": "birdeye",
                            "sources": ["birdeye"],
                        },
                    )
                    if isinstance(existing, dict):
                        sources = existing.setdefault("sources", [])
                        if isinstance(sources, list) and "birdeye" not in sources:
                            sources.append("birdeye")
                    continue
                mints.append(mint)
                TRENDING_METADATA[mint] = {
                    "address": mint,
                    "source": "birdeye",
                    "sources": ["birdeye"],
                    "rank": len(TRENDING_METADATA),
                }
                success = True
            offset += len(batch)
            if len(batch) < page_size or len(mints) >= requested:
                break
            # Gentle pacing to respect Birdeye rate limits (60 req/min)
            await asyncio.sleep(_BIRDEYE_PAGE_DELAY)

    result: List[str]
    failure = False

    if not mints:
        cached = _apply_cached()
        if cached:
            result = cached
        else:
            result = _apply_static()
        failure = True
    else:
        result = mints[:requested]

    if result:
        _LAST_TRENDING_RESULT["mints"] = list(result)
        _LAST_TRENDING_RESULT["metadata"] = copy.deepcopy(TRENDING_METADATA)
        _LAST_TRENDING_RESULT["timestamp"] = now

    if forced_cooldown_reason:
        _FAILURE_COUNT = _FAILURE_THRESHOLD
        cooldown_window = forced_cooldown_seconds or _FAILURE_COOLDOWN
        _COOLDOWN_UNTIL = now + cooldown_window
    elif failure or not success:
        _FAILURE_COUNT += 1
        if _FAILURE_COUNT >= _FAILURE_THRESHOLD:
            _COOLDOWN_UNTIL = now + _FAILURE_COOLDOWN
    else:
        _FAILURE_COUNT = 0
        _COOLDOWN_UNTIL = 0.0

    return result


async def _rpc_get_multiple_accounts(
    session: aiohttp.ClientSession,
    rpc_url: str,
    mints: Sequence[str],
) -> Dict[str, dict]:
    """
    Lightweight enrichment via getMultipleAccounts (jsonParsed).
    Returns a map mint -> account payload.
    """
    result: Dict[str, dict] = {}
    CHUNK = 50
    for i in range(0, len(mints), CHUNK):
        chunk = mints[i : i + CHUNK]
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getMultipleAccounts",
            "params": [list(chunk), {"encoding": "jsonParsed"}],
        }
        async with session.post(rpc_url, json=payload, timeout=20) as resp:
            resp.raise_for_status()
            data = await resp.json()
            value = ((data or {}).get("result") or {}).get("value") or []
            for mint, acc in zip(chunk, value):
                if acc:
                    result[mint] = acc
    return result


async def enrich_tokens_async(
    mints: Iterable[str],
    *,
    rpc_url: str = DEFAULT_SOLANA_RPC,
) -> List[str]:
    """
    Verify token accounts exist and filter obviously bad ones.
    Keeps tokens if decimals parsing fails (agents can still decide).
    """
    as_list = [m for m in mints if isinstance(m, str) and len(m) > 10]
    if not as_list:
        return []

    async with aiohttp.ClientSession() as session:
        try:
            accs = await _rpc_get_multiple_accounts(session, rpc_url, as_list)
        except Exception as exc:
            logger.warning("RPC enrichment failed: %s", exc)
            return as_list

    filtered: List[str] = []
    for m in as_list:
        info = accs.get(m)
        if not info:
            continue
        try:
            parsed = ((info.get("data") or {}).get("parsed") or {}).get("info") or {}
            decimals = int(parsed.get("decimals", 0))
            # basic sanity bound
            if 0 <= decimals <= 12:
                filtered.append(m)
        except Exception:
            # if schema unexpected, don't block trading — keep it
            filtered.append(m)
    return filtered
