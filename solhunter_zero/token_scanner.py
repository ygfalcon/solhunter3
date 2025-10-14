from __future__ import annotations

import asyncio
import copy
import logging
import os
import random
import time
from collections import deque
from typing import Any, Dict, Iterable, List, Sequence, Set
from urllib.parse import parse_qs, urlparse

import aiohttp

from .discovery.mint_resolver import normalize_candidate
from .token_aliases import canonical_mint, validate_mint
from .clients.helius_das import get_asset_batch
from .util.env import optional_helius_rpc_url
from .util.mints import clean_candidate_mints

# Resolved on import to support tooling that inspects the module constants.
DEFAULT_SOLANA_RPC = optional_helius_rpc_url("")

BIRDEYE_BASE = "https://public-api.birdeye.so"

HELIUS_BASE = os.getenv("HELIUS_PRICE_BASE_URL", "https://api.helius.xyz")
HELIUS_TREND_PATH = os.getenv("HELIUS_PRICE_PATH", "/v1/trending-tokens")
SOLSCAN_META_URL = os.getenv(
    "SOLSCAN_META_URL", "https://pro-api.solscan.io/v1.0/token/meta"
)
SOLSCAN_API_KEY = (os.getenv("SOLSCAN_API_KEY") or "").strip() or None
PUMP_LEADERBOARD_URL = os.getenv(
    "PUMP_LEADERBOARD_URL", "https://pumpportal.fun/api/leaderboard"
)


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


def _normalize_mint_candidate(candidate: object) -> str | None:
    """Return canonical mint when ``candidate`` looks like a plausible address."""

    if not isinstance(candidate, str):
        return None
    normalized = normalize_candidate(candidate)
    if not normalized:
        return None
    canonical = canonical_mint(normalized)
    if not validate_mint(canonical):
        return None
    return canonical

FAST_MODE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
_BIRDEYE_TIMEOUT = float(os.getenv("FAST_BIRDEYE_TIMEOUT", "6.0")) if FAST_MODE else 10.0
_BIRDEYE_PAGE_DELAY = float(os.getenv("FAST_BIRDEYE_PAGE_DELAY", "0.35")) if FAST_MODE else 1.1

_SPL_TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_TOKEN_2022_PREFIX = "Tokenz"
_TRENDING_MIN_LIQUIDITY = float(os.getenv("TRENDING_MIN_LIQUIDITY_USD", "7500") or 7500.0)
_TRENDING_MIN_VOLUME = float(os.getenv("TRENDING_MIN_VOLUME_USD", "20000") or 20000.0)
_MAX_DAS_CHUNK = max(1, int(os.getenv("DAS_TRENDING_CHUNK", "80") or 80))

def _resolve_birdeye_key(candidate: str | None = None) -> str | None:
    key = (candidate or os.getenv("BIRDEYE_API_KEY") or "").strip()
    return key or None


def _birdeye_enabled(candidate: str | None = None) -> bool:
    flag = os.getenv("BIRDEYE_ENABLED")
    if flag is not None and flag.strip():
        return flag.lower() in {"1", "true", "yes", "on"}
    return _resolve_birdeye_key(candidate) is not None
_HELIUS_TIMEOUT = float(os.getenv("FAST_HELIUS_TIMEOUT", "4.0")) if FAST_MODE else 8.0
_SOLSCAN_TIMEOUT = float(os.getenv("FAST_SOLSCAN_TIMEOUT", "4.0")) if FAST_MODE else 8.0

_ALLOW_PARTIAL_RESULTS = (
    (os.getenv("TOKEN_SCAN_ALLOW_PARTIAL") or ("1" if FAST_MODE else "0"))
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)
_PARTIAL_THRESHOLD = max(
    1,
    int(
        os.getenv(
            "TOKEN_SCAN_PARTIAL_THRESHOLD",
            "6" if FAST_MODE else "10",
        )
        or ("6" if FAST_MODE else "10")
    ),
)

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


def _normalise_key(text: str) -> str:
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _search_numeric(
    obj: Any,
    include: Sequence[str],
    *,
    avoid: Sequence[str] = (),
) -> float | None:
    visited: Set[int] = set()

    def _inner(current: Any, matched: bool = False) -> float | None:
        if isinstance(current, dict):
            ident = id(current)
            if ident in visited:
                return None
            visited.add(ident)
            for key, value in current.items():
                if not isinstance(key, str):
                    continue
                key_norm = _normalise_key(key)
                if avoid and any(exclude in key_norm for exclude in avoid):
                    continue
                next_matched = matched or any(target in key_norm for target in include)
                result = _inner(value, next_matched)
                if result is not None:
                    return result
            return None
        if isinstance(current, (list, tuple, set)):
            ident = id(current)
            if ident in visited:
                return None
            visited.add(ident)
            for item in current:
                result = _inner(item, matched)
                if result is not None:
                    return result
            return None
        if matched:
            return _coerce_float(current)
        return None

    return _inner(obj)


def _normalize_helius_item(raw: Dict[str, Any], *, rank: int) -> Dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    relevant_keys = {
        "token",
        "tokenInfo",
        "tokenMetadata",
        "metrics",
        "market",
        "mint",
        "address",
        "tokenAddress",
        "mintAddress",
    }
    if not any(key in raw for key in relevant_keys):
        return None

    address = None
    candidates: List[Any] = [raw]
    token_info = raw.get("token") or raw.get("tokenInfo") or raw.get("tokenMetadata")
    if isinstance(token_info, dict):
        candidates.append(token_info)
    market_info = raw.get("market") or raw.get("metadata")
    if isinstance(market_info, dict):
        candidates.append(market_info)

    metrics = raw.get("metrics")
    if isinstance(metrics, dict):
        candidates.append(metrics)

    def _iter_nested_dicts(*objects: Any) -> Iterable[Dict[str, Any]]:
        queue: deque[Dict[str, Any]] = deque()
        seen: Set[int] = set()
        for obj in objects:
            if isinstance(obj, dict):
                queue.append(obj)
        while queue:
            current = queue.popleft()
            ident = id(current)
            if ident in seen:
                continue
            seen.add(ident)
            yield current
            for value in current.values():
                if isinstance(value, dict):
                    queue.append(value)
                elif isinstance(value, (list, tuple, set)):
                    for item in value:
                        if isinstance(item, dict):
                            queue.append(item)

    nested_candidates = list(_iter_nested_dicts(*candidates)) or [raw]

    for candidate in nested_candidates:
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

    symbol_sources = list(_iter_nested_dicts(raw, token_info, market_info))
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

    search_space: List[Dict[str, Any]] = []
    if isinstance(metrics, dict):
        search_space.append(metrics)
    search_space.append(raw)
    if isinstance(market_info, dict):
        search_space.append(market_info)
    if isinstance(token_info, dict):
        search_space.append(token_info)

    float_fields = {
        "score": ("score", "rankingscore", "momentumscore"),
        "volume": ("volume", "volume24h", "volumeusd", "usdvolume"),
        "market_cap": ("marketcap", "marketcapusd", "usdmarketcap"),
        "price_change": (
            "pricechange",
            "pricechange24h",
            "pricechange24hpercent",
            "change24h",
            "changepercent",
        ),
    }

    for dest, keys in float_fields.items():
        number = None
        for source in search_space:
            number = _search_numeric(source, keys)
            if number is not None:
                entry[dest] = number
                break

    price = None
    for source in search_space:
        price = _search_numeric(
            source,
            ("price", "priceusd", "usdprice", "lastprice", "currentprice"),
            avoid=("change", "percent", "delta"),
        )
        if price is not None:
            entry["price"] = price
            break

    liquidity = None
    for source in search_space:
        liquidity = _search_numeric(
            source,
            ("liquidity", "liquidityusd", "usdliquidity", "totalliquidity"),
        )
        if liquidity is not None:
            entry["liquidity"] = liquidity
            break

    entry["sources"] = ["helius"]
    entry["raw"] = raw
    return entry


async def _birdeye_trending(
    session: aiohttp.ClientSession,
    api_key: str,
    *,
    limit: int = 20,
    offset: int = 0,
    return_entries: bool = False,
) -> List[Any]:
    """Fetch trending token mints from Birdeye's trending/top-mover endpoint."""
    api_key = api_key or ""

    resolved_birdeye_key = _resolve_birdeye_key(api_key)
    birdeye_allowed = _birdeye_enabled(resolved_birdeye_key)
    if (os.getenv("PREFLIGHT_RUNS") or "").strip():
        birdeye_allowed = False
    if not api_key:
        logger.debug("Birdeye API key missing; skipping trending fetch")
        return []

    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-api-key": api_key,
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

    backoff_schedule = (0.5, 1.0, 2.0, 4.0)

    for attempt, delay in enumerate(backoff_schedule, start=1):
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
                seen: Set[str] = set()
                addresses: List[str] = []
                records: List[Dict[str, Any]] = []
                for it in items:
                    addr: object = None
                    entry_dict: Dict[str, Any] | None = it if isinstance(it, dict) else None
                    if entry_dict is not None:
                        addr = (
                            entry_dict.get("address")
                            or entry_dict.get("mint")
                            or entry_dict.get("mintAddress")
                            or entry_dict.get("tokenAddress")
                        )
                        if not addr:
                            token_info = entry_dict.get("token") or entry_dict.get("tokenInfo")
                            if isinstance(token_info, dict):
                                addr = (
                                    token_info.get("address")
                                    or token_info.get("mint")
                                    or token_info.get("mintAddress")
                                    or token_info.get("tokenAddress")
                                )
                    elif isinstance(it, (list, tuple)) and it:
                        addr = it[0]
                    normalized = _normalize_mint_candidate(addr)
                    if not normalized:
                        if not return_entries and isinstance(addr, str):
                            raw_addr = addr.strip()
                            if raw_addr and raw_addr not in seen:
                                addresses.append(raw_addr)
                                seen.add(raw_addr)
                        continue
                    if normalized in seen:
                        if return_entries and entry_dict is not None:
                            for record in records:
                                if record.get("address") == normalized:
                                    record.setdefault("metadata", {}).setdefault(
                                        "birdeye", entry_dict
                                    )
                                    liquidity_val = _search_numeric(
                                        entry_dict,
                                        ("liquidity", "liquidityusd", "usdliquidity"),
                                    )
                                    if liquidity_val is not None and "liquidity" not in record:
                                        record["liquidity"] = liquidity_val
                                    volume_val = _search_numeric(
                                        entry_dict,
                                        ("volume", "volume24h", "volumeusd", "usdvolume"),
                                    )
                                    if volume_val is not None and "volume" not in record:
                                        record["volume"] = volume_val
                                    break
                        continue
                    seen.add(normalized)
                    addresses.append(normalized)
                    if return_entries:
                        record: Dict[str, Any] = {
                            "address": normalized,
                            "source": "birdeye",
                            "sources": ["birdeye"],
                        }
                        if entry_dict is not None:
                            record["raw"] = entry_dict
                            record.setdefault("metadata", {})["birdeye"] = entry_dict
                            liquidity_val = _search_numeric(
                                entry_dict,
                                ("liquidity", "liquidityusd", "usdliquidity"),
                            )
                            if liquidity_val is not None:
                                record["liquidity"] = liquidity_val
                            volume_val = _search_numeric(
                                entry_dict,
                                ("volume", "volume24h", "volumeusd", "usdvolume"),
                            )
                            if volume_val is not None:
                                record["volume"] = volume_val
                            sym = entry_dict.get("symbol") or entry_dict.get("tokenSymbol")
                            if isinstance(sym, str) and sym:
                                record["symbol"] = sym
                            name = entry_dict.get("name") or entry_dict.get("tokenName")
                            if isinstance(name, str) and name:
                                record["name"] = name
                        records.append(record)
                    if len(addresses) >= limit:
                        break
                return records if return_entries else addresses
        except BirdeyeThrottleError as exc:
            last_exc = exc
            wait_for = delay + random.random() * 0.3
            logger.debug("Birdeye throttle hit; sleeping %.2fs", wait_for)
            await asyncio.sleep(wait_for)
            continue
        except BirdeyeFatalError:
            raise
        except Exception as exc:
            last_exc = exc
            logger.warning("Birdeye trending request failed (%d/%d): %s", attempt, len(backoff_schedule), exc)
            await asyncio.sleep(delay)

    if isinstance(last_exc, BirdeyeThrottleError):
        raise last_exc
    if last_exc:
        logger.debug("Birdeye last_exc: %s", last_exc)
    return []


async def _dexscreener_trending_movers(
    session: aiohttp.ClientSession,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    url = os.getenv(
        "DEXSCREENER_TRENDING_URL",
        "https://api.dexscreener.com/latest/dex/tokens/trending",
    ).strip()
    if not url:
        return []
    params = {
        "type": os.getenv("DEXSCREENER_TRENDING_TYPE", "movers"),
        "chain": os.getenv("DEXSCREENER_TRENDING_CHAIN", "solana"),
        "limit": max(1, int(limit)),
    }
    try:
        async with session.get(url, params=params, timeout=_HELIUS_TIMEOUT) as resp:
            resp.raise_for_status()
            payload = await resp.json(content_type=None)
    except Exception as exc:  # pragma: no cover - network/runtime failures
        logger.debug("Dexscreener trending request failed: %s", exc)
        return []

    items: Any = []
    if isinstance(payload, dict):
        for key in ("movers", "tokens", "pairs", "items", "data", "results"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                items = candidate
                break
        else:
            items = payload.get("data") or payload
    else:
        items = payload

    if isinstance(items, dict):
        for key in ("items", "tokens", "pairs", "results"):
            candidate = items.get(key)
            if isinstance(candidate, list):
                items = candidate
                break
        else:
            items = list(items.values())

    if not isinstance(items, list):
        return []

    seen: Set[str] = set()
    results: List[Dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, dict):
            continue
        candidates: List[object] = [
            raw.get("tokenAddress"),
            raw.get("address"),
            raw.get("mint"),
            raw.get("mintAddress"),
            raw.get("baseMint"),
        ]
        base_token = raw.get("baseToken") or raw.get("token")
        if isinstance(base_token, dict):
            candidates.extend(
                [
                    base_token.get("address"),
                    base_token.get("mint"),
                    base_token.get("mintAddress"),
                ]
            )
        token_info = raw.get("tokenInfo")
        if isinstance(token_info, dict):
            candidates.extend(
                [
                    token_info.get("address"),
                    token_info.get("mint"),
                    token_info.get("mintAddress"),
                ]
            )
        mint: str | None = None
        for candidate in candidates:
            mint = _normalize_mint_candidate(candidate)
            if mint:
                break
        if not mint or mint in seen:
            continue
        seen.add(mint)
        record: Dict[str, Any] = {
            "address": mint,
            "source": "dexscreener",
            "sources": ["dexscreener"],
            "raw": raw,
            "metadata": {"dexscreener": raw},
        }
        liquidity_dict = raw.get("liquidity") if isinstance(raw.get("liquidity"), dict) else None
        liquidity_val = None
        if isinstance(liquidity_dict, dict):
            liquidity_val = _coerce_float(liquidity_dict.get("usd"))
        if liquidity_val is None:
            liquidity_val = _search_numeric(
                raw,
                ("liquidityusd", "usdliquidity", "totalliquidity", "liquidity"),
            )
        if liquidity_val is not None:
            record["liquidity"] = liquidity_val

        volume_val: float | None = None
        volume_info = raw.get("volume")
        if isinstance(volume_info, dict):
            for key in ("h24", "h6", "h1", "m5"):
                value = _coerce_float(volume_info.get(key))
                if value:
                    volume_val = value
                    break
        if volume_val is None:
            volume_val = _search_numeric(
                raw,
                ("volume24h", "volumeusd", "usdvolume", "volume"),
                avoid=("change", "ratio"),
            )
        if volume_val is not None:
            record["volume"] = volume_val

        base_meta = raw.get("baseToken") or raw.get("token") or {}
        if isinstance(base_meta, dict):
            symbol = base_meta.get("symbol") or base_meta.get("tokenSymbol")
            if isinstance(symbol, str) and symbol:
                record["symbol"] = symbol
            name = base_meta.get("name") or base_meta.get("tokenName")
            if isinstance(name, str) and name:
                record["name"] = name

        results.append(record)
        if len(results) >= limit:
            break

    return results


async def _das_enrich_candidates(
    session: aiohttp.ClientSession,
    mints: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    if not mints:
        return {}
    enriched: Dict[str, Dict[str, Any]] = {}
    for idx in range(0, len(mints), _MAX_DAS_CHUNK):
        chunk = list(mints[idx : idx + _MAX_DAS_CHUNK])
        try:
            assets = await get_asset_batch(session, chunk)
        except Exception as exc:
            logger.warning("DAS getAssetBatch failed: %s", exc)
            return {}
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            address = asset.get("id") or asset.get("mint") or asset.get("address")
            mint = _normalize_mint_candidate(address)
            if not mint:
                continue
            enriched[mint] = asset
    return enriched


def _asset_is_valid(asset: Dict[str, Any]) -> bool:
    interface = asset.get("interface") or asset.get("tokenType")
    if isinstance(interface, str):
        iface = interface.lower()
        if "fungible" not in iface:
            return False
    token_info = asset.get("token_info") or asset.get("tokenInfo") or {}
    if not isinstance(token_info, dict):
        token_info = {}
    program = token_info.get("token_program") or token_info.get("tokenProgram")
    if isinstance(program, str):
        if program != _SPL_TOKEN_PROGRAM and not program.startswith(_TOKEN_2022_PREFIX):
            return False
    decimals = token_info.get("decimals")
    if decimals is None:
        content = asset.get("content")
        if isinstance(content, dict):
            meta = content.get("metadata")
            if isinstance(meta, dict):
                decimals = meta.get("decimals")
    if decimals is not None:
        try:
            dec_value = int(decimals)
        except Exception:
            return False
        if dec_value < 0 or dec_value > 12:
            return False
    return True


def _extract_asset_metadata(asset: Dict[str, Any]) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    token_info = asset.get("token_info") or asset.get("tokenInfo") or {}
    if isinstance(token_info, dict):
        program = token_info.get("token_program") or token_info.get("tokenProgram")
        if isinstance(program, str):
            metadata["token_program"] = program
        decimals = token_info.get("decimals")
        try:
            if decimals is not None:
                metadata["decimals"] = int(decimals)
        except Exception:
            pass
        symbol = token_info.get("symbol")
        if isinstance(symbol, str) and symbol:
            metadata.setdefault("symbol", symbol)
        name = token_info.get("name")
        if isinstance(name, str) and name:
            metadata.setdefault("name", name)
    content = asset.get("content")
    if isinstance(content, dict):
        meta = content.get("metadata")
        if isinstance(meta, dict):
            symbol = meta.get("symbol")
            if isinstance(symbol, str) and symbol:
                metadata.setdefault("symbol", symbol)
            name = meta.get("name")
            if isinstance(name, str) and name:
                metadata.setdefault("name", name)
            decimals = meta.get("decimals")
            try:
                if decimals is not None:
                    metadata.setdefault("decimals", int(decimals))
            except Exception:
                pass
    return metadata


async def _collect_trending_seeds(
    session: aiohttp.ClientSession,
    *,
    limit: int,
    birdeye_api_key: str | None,
) -> List[Dict[str, Any]]:
    if not hasattr(session, "post"):
        return []
    desired = max(limit, 10)
    seeds: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    dexscreener_candidates = await _dexscreener_trending_movers(session, limit=desired)
    for item in dexscreener_candidates:
        mint = _normalize_mint_candidate(item.get("address"))
        if not mint or mint in seen:
            continue
        item = dict(item)
        item["address"] = mint
        seeds.append(item)
        seen.add(mint)
        if len(seeds) >= desired:
            break

    if birdeye_api_key:
        birdeye_limit = max(desired - len(seeds), desired)
        try:
            birdeye_entries = await _birdeye_trending(
                session,
                birdeye_api_key,
                limit=birdeye_limit,
                offset=0,
                return_entries=True,
            )
        except BirdeyeFatalError:
            raise
        except BirdeyeThrottleError:
            raise
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Birdeye top movers fetch failed: %s", exc)
            birdeye_entries = []
        for entry in birdeye_entries:
            if not isinstance(entry, dict):
                continue
            mint = _normalize_mint_candidate(entry.get("address"))
            if not mint:
                continue
            entry = dict(entry)
            entry["address"] = mint
            existing: Dict[str, Any] | None = None
            for candidate in seeds:
                if candidate.get("address") == mint:
                    existing = candidate
                    break
            if existing is not None:
                sources = existing.setdefault("sources", [])
                if isinstance(sources, list) and "birdeye" not in sources:
                    sources.append("birdeye")
                existing.setdefault("metadata", {}).setdefault("birdeye", entry.get("raw", entry))
                if "liquidity" not in existing and isinstance(entry.get("liquidity"), (int, float)):
                    existing["liquidity"] = entry["liquidity"]
                if "volume" not in existing and isinstance(entry.get("volume"), (int, float)):
                    existing["volume"] = entry["volume"]
                if not existing.get("symbol") and entry.get("symbol"):
                    existing["symbol"] = entry["symbol"]
                if not existing.get("name") and entry.get("name"):
                    existing["name"] = entry["name"]
                continue
            entry.setdefault("source", "birdeye")
            entry.setdefault("sources", ["birdeye"])
            entry.setdefault("metadata", {})
            if "birdeye" not in entry["metadata"]:
                raw = entry.get("raw") or entry
                entry["metadata"]["birdeye"] = raw
            seeds.append(entry)
            seen.add(mint)
            if len(seeds) >= desired:
                break

    filtered: List[Dict[str, Any]] = []
    for candidate in seeds:
        liquidity = _coerce_float(candidate.get("liquidity"))
        volume = _coerce_float(candidate.get("volume"))
        if liquidity is not None and liquidity < _TRENDING_MIN_LIQUIDITY:
            continue
        if volume is not None and volume < _TRENDING_MIN_VOLUME:
            continue
        filtered.append(candidate)

    if not filtered:
        filtered = list(seeds)

    metadata = await _das_enrich_candidates(
        session,
        [item["address"] for item in filtered],
    )
    if not metadata:
        return [dict(candidate) for candidate in filtered[:limit]]

    final: List[Dict[str, Any]] = []
    for candidate in filtered:
        mint = candidate.get("address")
        if not isinstance(mint, str):
            continue
        asset = metadata.get(mint)
        if not asset or not _asset_is_valid(asset):
            continue
        enriched = dict(candidate)
        enriched.setdefault("sources", [])
        sources = enriched["sources"]
        if isinstance(sources, list) and "das" not in sources:
            sources.append("das")
        enriched.setdefault("metadata", {})["das"] = asset
        enriched.update(_extract_asset_metadata(asset))
        enriched["address"] = mint
        final.append(enriched)
        if len(final) >= limit:
            break

    return final


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


async def _solscan_enrich(
    session: aiohttp.ClientSession,
    mint: str,
    api_key: str | None = None,
) -> Dict[str, Any] | None:
    """Fetch token metadata (name/symbol/decimals) from Solscan."""

    if not SOLSCAN_META_URL:
        return None
    headers: Dict[str, str] = {
        "accept": "application/json",
    }
    if api_key:
        headers["token"] = api_key

    params = {"address": mint}

    try:
        async with session.get(
            SOLSCAN_META_URL,
            params=params,
            headers=headers,
            timeout=_SOLSCAN_TIMEOUT,
        ) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            payload = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:  # pragma: no cover - network failures
        logger.debug("Solscan metadata fetch failed for %s: %s", mint, exc)
        return None
    except Exception as exc:  # pragma: no cover - parsing failures
        logger.exception("Solscan metadata error for %s: %s", mint, exc)
        return None

    if not isinstance(payload, dict):
        return None

    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    if not isinstance(data, dict):
        return None

    name = data.get("name") or data.get("symbolName")
    symbol = data.get("symbol")
    decimals = data.get("decimals")
    icon = data.get("tokenIcon") or data.get("icon")
    if name is None and symbol is None and decimals is None and icon is None:
        return None

    result: Dict[str, Any] = {}
    if isinstance(name, str):
        result["name"] = name
    if isinstance(symbol, str):
        result["symbol"] = symbol
    try:
        if decimals is not None:
            result["decimals"] = int(decimals)
    except Exception:
        pass
    if isinstance(icon, str):
        result["icon"] = icon
    return result or None

async def _pump_trending(
    session: aiohttp.ClientSession,
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    """Return trending Pump.fun tokens when DAS is unavailable."""

    url = PUMP_LEADERBOARD_URL.strip()
    if not url or limit <= 0:
        return []

    params = {
        "sort": os.getenv("PUMP_LEADERBOARD_SORT", "volume_24h"),
        "timeframe": os.getenv("PUMP_LEADERBOARD_TIMEFRAME", "24h"),
        "limit": max(1, int(limit)),
    }

    try:
        async with session.get(url, params=params, timeout=_HELIUS_TIMEOUT) as resp:
            resp.raise_for_status()
            payload = await resp.json(content_type=None)
    except Exception as exc:  # pragma: no cover - network failure
        logger.debug("Pump.fun trending request failed: %s", exc)
        return []

    if not isinstance(payload, list):
        return []

    tokens: List[Dict[str, Any]] = []
    seen: set[str] = set()

    for entry in payload:
        if not isinstance(entry, dict):
            continue
        mint = _normalize_mint_candidate(entry.get("mint") or entry.get("tokenMint"))
        if not mint or mint in seen:
            continue
        seen.add(mint)
        meta = {
            "address": mint,
            "source": "pumpfun",
            "sources": ["pumpfun"],
            "rank": len(tokens),
        }
        name = entry.get("name")
        symbol = entry.get("symbol")
        icon = entry.get("image_url") or entry.get("imageUrl")
        if isinstance(name, str):
            meta["name"] = name
        if isinstance(symbol, str):
            meta["symbol"] = symbol
        if isinstance(icon, str):
            meta["icon"] = icon
        meta.setdefault("metadata", {})["pumpfun"] = {
            k: entry.get(k)
            for k in (
                "name",
                "symbol",
                "liquidity",
                "volume_24h",
                "volume_1h",
                "volume_5m",
                "market_cap",
                "price",
            )
            if k in entry
        }
        tokens.append(meta)
        if len(tokens) >= limit:
            break

    if tokens and logger.isEnabledFor(logging.INFO):
        logger.info("Pump.fun leaderboard returned %d candidate(s)", len(tokens))

    return tokens


async def _helius_search_assets(
    session: aiohttp.ClientSession,
    *,
    limit: int,
    rpc_url: str,
) -> List[Dict[str, Any]]:
    """Return fungible assets sorted by recent activity using DAS searchAssets."""

    url = rpc_url.strip()
    if not url:
        logger.debug("Helius searchAssets skipped: empty RPC URL")
        return []
    if not hasattr(session, "post"):
        logger.debug("Helius searchAssets skipped: session missing POST support")
        return []

    normalized: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    pagination_token: str | None = None
    per_page = max(1, min(int(limit) if limit > 0 else 50, 100))
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(
            "Helius searchAssets: requesting up to %s token(s) via %s",
            limit,
            url.split("?", 1)[0],
        )

    while len(normalized) < limit:
        params: Dict[str, Any] = {
            "tokenType": "fungible",
            "page": 1,
            "limit": per_page,
            "sortBy": "created",
            "sortDirection": "desc",
        }
        if pagination_token:
            params["paginationToken"] = pagination_token

        payload = {
            "jsonrpc": "2.0",
            "id": f"trending-{len(normalized)}",
            "method": "searchAssets",
            "params": params,
        }

        if not hasattr(session, "post"):
            logger.debug("Helius searchAssets session missing post(); skipping request")
            break

        try:
            async with session.post(url, json=payload, timeout=_HELIUS_TIMEOUT) as resp:
                resp.raise_for_status()
                data = await resp.json(content_type=None)
        except aiohttp.ClientError as exc:  # pragma: no cover - network failure
            logger.warning("Helius searchAssets fetch failed: %s", exc)
            break
        except Exception as exc:  # pragma: no cover - parsing failure
            logger.exception("Helius searchAssets unexpected error: %s", exc)
            break

        result = data.get("result") if isinstance(data, dict) else None
        if isinstance(result, list):
            items = result
            pagination_token = None
        elif isinstance(result, dict):
            items = result.get("items") or result.get("tokens") or result.get("assets") or []
            pagination_token = (
                result.get("paginationToken")
                or result.get("pagination_token")
                or result.get("cursor")
                or result.get("next")
                or (result.get("pagination") or {}).get("next")
                or (result.get("pagination") or {}).get("cursor")
            )
        else:
            items = []
            pagination_token = None

        if not isinstance(items, list) or not items:
            break

        new_entries = 0
        for entry in items:
            if not isinstance(entry, dict):
                continue
            address: object = entry.get("id") or entry.get("mint")
            if not isinstance(address, str):
                token_info = entry.get("token_info") or entry.get("tokenInfo")
                if isinstance(token_info, dict):
                    address = token_info.get("mint") or token_info.get("address")
            mint = _normalize_mint_candidate(address)
            if not mint or mint in seen:
                continue
            token_info = entry.get("token_info") or entry.get("tokenInfo")
            if isinstance(token_info, dict):
                program = token_info.get("token_program") or token_info.get("tokenProgram")
                if isinstance(program, str):
                    if program != _SPL_TOKEN_PROGRAM and not program.startswith(_TOKEN_2022_PREFIX):
                        continue
            normalized.append(
                {
                    "address": mint,
                    "source": "helius_search",
                    "rank": len(normalized),
                    "raw": entry,
                }
            )
            seen.add(mint)
            new_entries += 1
            if len(normalized) >= limit:
                break
        if len(normalized) >= limit:
            break
        if new_entries == 0 or not pagination_token:
            break

    if logger.isEnabledFor(logging.INFO):
        logger.info("Helius searchAssets returned %d candidate(s)", len(normalized))

    return normalized


async def scan_tokens_async(
    *,
    rpc_url: str | None = None,
    limit: int = 50,
    enrich: bool = True,   # kept for compatibility; enrichment is separate call
    api_key: str | None = None,
) -> List[str]:
    """
    Pull trending mints preferring Helius price data, falling back to BirdEye.
    If both sources fail, return a small static set so the loop can proceed.
    """
    rpc_url = rpc_url or optional_helius_rpc_url("")
    _ = rpc_url  # reserved for future use; enrichment uses this parameter
    global _FAILURE_COUNT, _COOLDOWN_UNTIL

    requested = max(1, int(limit))
    api_key = api_key or ""

    resolved_birdeye_key = _resolve_birdeye_key(api_key)
    birdeye_allowed = _birdeye_enabled(resolved_birdeye_key)

    now = time.time()

    def _apply_cached() -> List[str]:
        cached_mints = list(_LAST_TRENDING_RESULT.get("mints") or [])
        if not cached_mints:
            return []
        TRENDING_METADATA.clear()
        cached_meta = _LAST_TRENDING_RESULT.get("metadata") or {}
        canonical_cached: List[str] = []
        for mint in cached_mints:
            if not isinstance(mint, str):
                continue
            canonical = canonical_mint(mint)
            if validate_mint(canonical):
                canonical_cached.append(canonical)
            else:
                canonical_cached.append(mint)
        for mint, meta in cached_meta.items():
            if not isinstance(mint, str) or not isinstance(meta, dict):
                continue
            canonical = canonical_mint(mint)
            target = canonical if validate_mint(canonical) else mint
            TRENDING_METADATA[target] = copy.deepcopy(meta)
            TRENDING_METADATA[target]["address"] = target
        return canonical_cached[:requested]

    def _apply_static() -> List[str]:
        TRENDING_METADATA.clear()
        fallback = [
            "So11111111111111111111111111111111111111112",  # SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
            "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",  # JUP
        ]
        fallback_valid: List[str] = []
        for candidate in fallback:
            canonical = canonical_mint(candidate)
            if not validate_mint(canonical):
                continue
            fallback_valid.append(canonical)
            TRENDING_METADATA.setdefault(
                canonical,
                {
                    "address": canonical,
                    "source": "static",
                    "sources": ["static"],
                    "rank": len(TRENDING_METADATA),
                },
            )
        return fallback_valid[:requested]

    if _COOLDOWN_UNTIL and now < _COOLDOWN_UNTIL:
        cached = _apply_cached()
        if cached:
            logger.warning(
                "Trending fetch cooling down for %.1fs; returning cached set (%d tokens)",
                max(0.0, _COOLDOWN_UNTIL - now),
                len(cached),
            )
            _LAST_TRENDING_RESULT["timestamp"] = now
            return cached
        logger.warning(
            "Trending fetch cooling down but cache empty; returning static fallback tokens",
        )
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

    session = aiohttp.ClientSession()
    try:
        birdeye_key = resolved_birdeye_key if birdeye_allowed else None
        try:
            seed_candidates = await _collect_trending_seeds(
                session,
                limit=requested,
                birdeye_api_key=birdeye_key,
            )
        except BirdeyeThrottleError as exc:
            reason = str(exc)
            logger.warning(
                "Birdeye top movers throttle; entering extended cooldown: %s",
                reason,
            )
            forced_cooldown_reason = reason or f"Birdeye status {exc.status}"
            forced_cooldown_seconds = max(
                _THROTTLE_COOLDOWN,
                _FATAL_FAILURE_COOLDOWN,
                _FAILURE_COOLDOWN,
            )
            seed_candidates = []
        except BirdeyeFatalError as exc:
            reason = str(exc)
            logger.warning(
                "Birdeye top movers fatal error; entering cooldown: %s",
                reason,
            )
            forced_cooldown_reason = reason or f"Birdeye status {exc.status}"
            if exc.throttle:
                forced_cooldown_seconds = max(
                    _FATAL_FAILURE_COOLDOWN,
                    _FAILURE_COOLDOWN,
                )
            seed_candidates = []
        except Exception as exc:
            logger.debug("Trending seed fetch failed: %s", exc)
            seed_candidates = []

        if seed_candidates:
            logger.info(
                "Trending seed fetch returned %d candidate(s)",
                len(seed_candidates),
            )
        else:
            logger.warning(
                "Trending seed fetch returned no candidates; attempting Birdeye fallback",
            )
            forced_cooldown_seconds = max(forced_cooldown_seconds or 0.0, 15.0)

        for item in seed_candidates:
            if not isinstance(item, dict):
                continue
            mint = _normalize_mint_candidate(item.get("address"))
            if not mint:
                continue
            if mint in mints:
                existing = TRENDING_METADATA.setdefault(mint, dict(item))
                if isinstance(existing, dict):
                    sources = existing.setdefault("sources", [])
                    source_name = item.get("source") or "trend"
                    if isinstance(sources, list) and source_name not in sources:
                        sources.append(source_name)
                    meta_map = existing.setdefault("metadata", {})
                    if isinstance(meta_map, dict):
                        incoming_meta = item.get("metadata")
                        if isinstance(incoming_meta, dict):
                            meta_map.update(incoming_meta)
                    if SOLSCAN_API_KEY and (
                        not existing.get("name")
                        or not existing.get("symbol")
                        or "decimals" not in existing
                    ):
                        solscan_meta = await _solscan_enrich(
                            session,
                            mint,
                            SOLSCAN_API_KEY,
                        )
                        if solscan_meta:
                            existing.update(solscan_meta)
                            meta_sources = existing.setdefault("sources", [])
                            if (
                                isinstance(meta_sources, list)
                                and "solscan" not in meta_sources
                            ):
                                meta_sources.append("solscan")
                            details = existing.setdefault("metadata", {})
                            if isinstance(details, dict):
                                details.setdefault("solscan", solscan_meta)
                continue
            mints.append(mint)
            meta = dict(item)
            meta["address"] = mint
            sources = meta.setdefault("sources", [])
            if isinstance(sources, list):
                default_source = item.get("source") or "trend"
                if default_source not in sources:
                    sources.append(default_source)
            else:
                meta["sources"] = [item.get("source") or "trend"]
            meta_store = meta.setdefault("metadata", {})
            if not isinstance(meta_store, dict):
                meta_store = {}
                meta["metadata"] = meta_store
            incoming_meta = item.get("metadata")
            if isinstance(incoming_meta, dict):
                meta_store.update(incoming_meta)
            if SOLSCAN_API_KEY and (
                not meta.get("name")
                or not meta.get("symbol")
                or "decimals" not in meta
            ):
                solscan_meta = await _solscan_enrich(
                    session,
                    mint,
                    SOLSCAN_API_KEY,
                )
                if solscan_meta:
                    meta.update(solscan_meta)
                    sources = meta.setdefault("sources", [])
                    if isinstance(sources, list) and "solscan" not in sources:
                        sources.append("solscan")
                    details = meta.setdefault("metadata", {})
                    if isinstance(details, dict):
                        details.setdefault("solscan", solscan_meta)
            meta.setdefault("rank", len(TRENDING_METADATA))
            TRENDING_METADATA[mint] = meta
            if len(mints) >= requested:
                break
        if mints:
            success = True

        offset = 0
        allow_partial = (
            _ALLOW_PARTIAL_RESULTS
            and len(mints) >= min(_PARTIAL_THRESHOLD, requested)
        )
        if allow_partial and len(mints) < requested:
            logger.debug(
                "Token scan fast-mode returning partial result (%d/%d)",
                len(mints),
                requested,
            )

        while birdeye_allowed and not allow_partial and len(mints) < requested:
            page_size = min(20, requested - len(mints))
            try:
                batch = await _birdeye_trending(
                    session,
                    resolved_birdeye_key or "",
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
            for candidate in batch:
                mint = _normalize_mint_candidate(candidate)
                if not mint:
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
            if _ALLOW_PARTIAL_RESULTS and len(mints) >= min(_PARTIAL_THRESHOLD, requested):
                if len(mints) < requested:
                    logger.debug(
                        "Token scan fast-mode returning partial result (%d/%d)",
                        len(mints),
                        requested,
                    )
                break
            if len(batch) < page_size or len(mints) >= requested:
                break
            # Gentle pacing to respect Birdeye rate limits (60 req/min)
            await asyncio.sleep(_BIRDEYE_PAGE_DELAY)

        result: List[str]
        failure = False

        if not mints:
            rpc_candidate = os.getenv("SOLANA_RPC_URL") or optional_helius_rpc_url("")
            search_items = await _helius_search_assets(
                session,
                limit=requested,
                rpc_url=rpc_candidate,
            )
            for item in search_items:
                address = _normalize_mint_candidate(item.get("address"))
                if not address or address in mints:
                    continue
                mints.append(address)
                meta = {
                    "address": address,
                    "source": "helius_search",
                    "sources": ["helius_search"],
                    "rank": len(TRENDING_METADATA),
                }
                raw = item.get("raw")
                if isinstance(raw, dict):
                    meta["raw"] = raw
                if SOLSCAN_API_KEY:
                    solscan_meta = await _solscan_enrich(
                        session,
                        address,
                        SOLSCAN_API_KEY,
                    )
                    if solscan_meta:
                        meta.update(solscan_meta)
                        sources = meta.setdefault("sources", [])
                        if isinstance(sources, list) and "solscan" not in sources:
                            sources.append("solscan")
                        details = meta.setdefault("metadata", {})
                        if isinstance(details, dict):
                            details.setdefault("solscan", solscan_meta)
                TRENDING_METADATA[address] = meta
                if len(mints) >= requested:
                    break
            if mints:
                logger.info("Helius searchAssets filled %d candidates", len(mints))
                success = True

        if not mints and not forced_cooldown_reason:
            pump_tokens = await _pump_trending(session, limit=requested)
            for meta in pump_tokens:
                mint = _normalize_mint_candidate(meta.get("address"))
                if not mint or mint in mints:
                    continue
                mints.append(mint)
                TRENDING_METADATA[mint] = meta
                if len(mints) >= requested:
                    break
            if pump_tokens:
                logger.info("Pump.fun fallback supplied %d candidate(s)", len(pump_tokens))
                success = True

        if not mints:
            cached = _apply_cached()
            if cached:
                logger.info("Trending lookup empty; serving cached set of %d tokens", len(cached))
                result = cached
            else:
                logger.warning("Trending lookup empty; using static fallback tokens")
                result = _apply_static()
            failure = True
        else:
            deduped = list(dict.fromkeys(mints))
            cleaned, dropped = clean_candidate_mints(deduped)
            if dropped:
                logger.warning(
                    "Dropped %d invalid mint(s) at validator edge", len(dropped)
                )
                for _original, sanitized in dropped:
                    if sanitized:
                        TRENDING_METADATA.pop(sanitized, None)
            result = cleaned[:requested]

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
    finally:
        close_fn = getattr(session, "close", None)
        if close_fn:
            try:
                if asyncio.iscoroutinefunction(close_fn):
                    await close_fn()
                else:
                    close_fn()
            except Exception:
                pass


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
    rpc_url: str | None = None,
) -> List[str]:
    """
    Verify token accounts exist and filter obviously bad ones.
    Keeps tokens if decimals parsing fails (agents can still decide).
    """
    as_list = []
    for candidate in mints:
        mint = _normalize_mint_candidate(candidate)
        if mint:
            as_list.append(mint)
    if not as_list:
        return []

    rpc_url = rpc_url or optional_helius_rpc_url("")
    if not rpc_url:
        logger.warning("Token enrichment skipped: RPC URL not configured")
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
