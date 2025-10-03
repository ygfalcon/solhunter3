from __future__ import annotations

import asyncio
import json
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
HELIUS_TREND_PATH = os.getenv("HELIUS_PRICE_PATH", "/v0/token-trending")


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

FAST_MODE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
_BIRDEYE_TIMEOUT = float(os.getenv("FAST_BIRDEYE_TIMEOUT", "6.0")) if FAST_MODE else 10.0
_BIRDEYE_PAGE_DELAY = float(os.getenv("FAST_BIRDEYE_PAGE_DELAY", "0.35")) if FAST_MODE else 1.1
_HELIUS_TIMEOUT = float(os.getenv("FAST_HELIUS_TIMEOUT", "4.0")) if FAST_MODE else 8.0

TRENDING_METADATA: Dict[str, Dict[str, Any]] = {}
BIRDEYE_TRENDING_STATUS: Dict[str, Any] = {"ok": True, "last_error": None, "cooldown_seconds": 0.0}

_BIRDEYE_FAILURE_STATE: Dict[str, Any] = {
    "consecutive_failures": 0,
    "cooldown_until": 0.0,
    "last_error": None,
}

BIRDEYE_TRENDING_PATH = os.getenv("BIRDEYE_TRENDING_PATH", "/v1/trending")
BIRDEYE_TRENDING_TYPE = os.getenv("BIRDEYE_TRENDING_TYPE", "token")
BIRDEYE_TRENDING_TIMEFRAME = os.getenv("BIRDEYE_TRENDING_TIMEFRAME", "24h")
BIRDEYE_CHAIN = os.getenv("BIRDEYE_CHAIN", "solana")

__all__ = [
    "scan_tokens_async",
    "enrich_tokens_async",
    "TRENDING_METADATA",
    "BIRDEYE_TRENDING_STATUS",
]


def _monotonic() -> float:
    try:
        return asyncio.get_running_loop().time()
    except RuntimeError:
        return time.monotonic()


def _reset_birdeye_failures() -> None:
    state = _BIRDEYE_FAILURE_STATE
    state["consecutive_failures"] = 0
    state["cooldown_until"] = 0.0
    state["last_error"] = None
    BIRDEYE_TRENDING_STATUS.update({"ok": True, "last_error": None, "cooldown_seconds": 0.0})


def _summarize_birdeye_error(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("message", "msg", "error", "description", "detail"):
            val = payload.get(key)
            if isinstance(val, str) and val:
                return val
        try:
            return json.dumps(payload)
        except Exception:
            return str(payload)
    if isinstance(payload, str):
        return payload
    return repr(payload)


def _record_birdeye_failure(status: int, payload: Any) -> None:
    now = _monotonic()
    state = _BIRDEYE_FAILURE_STATE
    state["consecutive_failures"] = state.get("consecutive_failures", 0) + 1
    cooldown = min(900.0, 30.0 * (2 ** (state["consecutive_failures"] - 1)))
    state["cooldown_until"] = max(state.get("cooldown_until", 0.0), now + cooldown)
    state["last_error"] = {"status": status, "payload": payload, "timestamp": now}
    BIRDEYE_TRENDING_STATUS.update(
        {
            "ok": False,
            "last_error": {
                "status": status,
                "summary": _summarize_birdeye_error(payload),
                "payload": payload,
            },
            "cooldown_seconds": max(0.0, state["cooldown_until"] - now),
        }
    )


def _birdeye_cooldown_remaining() -> float:
    now = _monotonic()
    remaining = max(0.0, float(_BIRDEYE_FAILURE_STATE.get("cooldown_until", 0.0) - now))
    BIRDEYE_TRENDING_STATUS["cooldown_seconds"] = remaining
    return remaining


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
    for key in ("mint", "address", "token", "mintAddress", "tokenAddress"):
        val = raw.get(key)
        if isinstance(val, str) and len(val) > 10:
            address = val
            break
    if not address:
        return None

    entry: Dict[str, Any] = {
        "address": address,
        "source": "helius",
        "rank": rank,
    }

    for key in ("symbol", "ticker"):
        val = raw.get(key)
        if isinstance(val, str) and val:
            entry.setdefault("symbol", val)
            break

    name = raw.get("name") or raw.get("tokenName")
    if isinstance(name, str) and name:
        entry.setdefault("name", name)

    float_fields = {
        "score": ("score", "rankingScore", "momentumScore"),
        "volume": ("volume", "volume24h", "volume_24h", "volumeUSD"),
        "market_cap": ("marketCap", "market_cap", "marketCapUsd"),
        "price_change": (
            "priceChange", "priceChange24h", "priceChange24hPercent", "change24h"
        ),
    }

    for dest, keys in float_fields.items():
        for key in keys:
            value = raw.get(key)
            number = _coerce_float(value)
            if number is not None:
                entry[dest] = number
                break

    price = _coerce_float(raw.get("price") or raw.get("priceUsd"))
    if price is not None:
        entry["price"] = price

    liquidity = _coerce_float(raw.get("liquidity") or raw.get("liquidityUsd"))
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
    """
    Fetch trending token mints from Birdeye. Returns a list of mint addresses.
    Retries politely on 429; logs and returns [] on persistent failure.
    """
    api_key = api_key or ""
    if not api_key:
        logger.debug("Birdeye API key missing; skipping trending fetch")
        return []

    remaining = _birdeye_cooldown_remaining()
    if remaining > 0:
        state = _BIRDEYE_FAILURE_STATE
        error = state.get("last_error") or {}
        summary = "cooldown active"
        if isinstance(error, dict):
            detail = error.get("payload") or error.get("summary")
            if detail:
                summary = _summarize_birdeye_error(detail)
        logger.debug(
            "Skipping BirdEye trending request due to cooldown (%.1fs remaining, last=%s)",
            remaining,
            summary,
        )
        return []

    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-chain": BIRDEYE_CHAIN,
    }
    params = {
        "offset": max(0, int(offset)),
        "limit": max(1, int(limit)),
        "type": BIRDEYE_TRENDING_TYPE,
        "timeframe": BIRDEYE_TRENDING_TIMEFRAME,
        "chain": BIRDEYE_CHAIN,
    }
    url = f"{BIRDEYE_BASE}{BIRDEYE_TRENDING_PATH}"
    BIRDEYE_TRENDING_STATUS.update(
        {
            "route": url,
            "params": dict(params),
        }
    )

    backoffs = [0.1, 0.5, 1.0]
    last_exc: Exception | None = None

    for attempt in range(1, 4):
        try:
            async with session.get(
                url,
                headers=headers,
                params=params,
                timeout=_BIRDEYE_TIMEOUT,
            ) as resp:
                if resp.status == 429:
                    if attempt < 3:
                        wait = backoffs[attempt - 1]
                        logger.info("BirdEye 429; backoff and retry (%d/3)", attempt + 1)
                        await asyncio.sleep(wait)
                        continue
                if 400 <= resp.status < 500:
                    body_text = await resp.text()
                    try:
                        payload = json.loads(body_text)
                    except Exception:
                        payload = body_text.strip()
                    _record_birdeye_failure(resp.status, payload)
                    summary = _summarize_birdeye_error(payload)
                    logger.warning(
                        "Birdeye trending %s error (attempt %d/3): %s",
                        resp.status,
                        attempt,
                        summary,
                    )
                    return []
                resp.raise_for_status()
                data = await resp.json(content_type=None)
                _reset_birdeye_failures()
                if not isinstance(data, dict):
                    logger.warning("Unexpected Birdeye payload type %s", type(data).__name__)
                    return []
                payload = data.get("data") or {}
                # Newer API versions wrap results under data['tokens']
                items = payload.get("tokens") or payload.get("items") or payload
                if isinstance(items, dict):
                    items = items.get("tokens") or items.get("items") or []
                if not isinstance(items, list):
                    return []
                mints: List[str] = []
                for it in items:
                    addr = None
                    if isinstance(it, dict):
                        addr = it.get("address") or it.get("mint")
                    elif isinstance(it, (list, tuple)) and it:
                        addr = it[0]
                    if isinstance(addr, str):
                        mints.append(addr)
                return mints
        except Exception as exc:
            last_exc = exc
            logger.warning("Birdeye trending request failed (%d/3): %s", attempt, exc)
            await asyncio.sleep(0.1)

    if last_exc:
        logger.debug("Birdeye last_exc: %s", last_exc)
        if isinstance(getattr(last_exc, "status", None), int) and 400 <= last_exc.status < 500:
            payload = getattr(last_exc, "message", str(last_exc))
            _record_birdeye_failure(last_exc.status, payload)
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
    method = os.getenv("HELIUS_TRENDING_METHOD", "GET").upper()
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
        params.setdefault("limit", int(limit))
        sort = os.getenv("HELIUS_PRICE_SORT") or os.getenv("HELIUS_TREND_SORT")
        if sort:
            params["sortBy"] = sort
        timeframe = os.getenv("HELIUS_PRICE_TIMEFRAME") or os.getenv("HELIUS_TREND_TIMEFRAME")
        if timeframe:
            params["timeframe"] = timeframe

    if not url:
        logger.debug("Helius trending URL missing; skipping fetch")
        return []

    try:
        request_coro: Any
        if method == "POST":
            body = {k: v for k, v in params.items() if k not in {"api-key"}}
            request_coro = session.post(
                url,
                json=body or None,
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
            for value in obj.values():
                if len(tokens) >= limit:
                    break
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
    requested = max(1, int(limit))
    api_key = api_key or ""
    mints: List[str] = []
    TRENDING_METADATA.clear()

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

        offset = 0
        while len(mints) < requested:
            page_size = min(20, requested - len(mints))
            batch = await _birdeye_trending(
                session,
                api_key,
                limit=page_size,
                offset=offset,
            )
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
            offset += len(batch)
            if len(batch) < page_size or len(mints) >= requested:
                break
            # Gentle pacing to respect Birdeye rate limits (60 req/min)
            await asyncio.sleep(_BIRDEYE_PAGE_DELAY)

    if not mints:
        # Fallback to a few known mints so evaluation can run
        mints = [
            "So11111111111111111111111111111111111111112",  # SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
            "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v",  # JUP
        ]
        for mint in mints:
            TRENDING_METADATA.setdefault(
                mint,
                {
                    "address": mint,
                    "source": "static",
                    "sources": ["static"],
                    "rank": len(TRENDING_METADATA),
                },
            )

    return mints[:requested]


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
