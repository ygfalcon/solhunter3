# solhunter_zero/onchain_metrics.py
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, Optional, Tuple, List
from urllib.parse import urlparse, parse_qs

import copy

import aiohttp

from .scanner_onchain import (
    fetch_average_swap_size as _scanner_fetch_average_swap_size,
    fetch_mempool_tx_rate as _scanner_fetch_mempool_tx_rate,
    fetch_volume_onchain_async as _scanner_fetch_volume_async,
    fetch_volume_onchain as _scanner_fetch_volume,
    fetch_whale_wallet_activity as _scanner_fetch_whale_wallet_activity,
)
from .http import get_session
from .lru import TTLCache

FAST_MODE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}

DEX_METRICS_CACHE_TTL = float(os.getenv("DEX_METRICS_CACHE_TTL", "20") or 20.0)
DEX_METRICS_CACHE: TTLCache = TTLCache(maxsize=512, ttl=DEX_METRICS_CACHE_TTL)

HELIUS_SLOT_CACHE_TTL = float(os.getenv("HELIUS_SLOT_CACHE_TTL", "5") or 5.0)
HELIUS_SLOT_CACHE: TTLCache = TTLCache(maxsize=8, ttl=HELIUS_SLOT_CACHE_TTL)

HELIUS_DECIMALS_CACHE_TTL = float(os.getenv("HELIUS_DECIMALS_CACHE_TTL", "600") or 600.0)
HELIUS_DECIMALS_CACHE: TTLCache = TTLCache(maxsize=4096, ttl=HELIUS_DECIMALS_CACHE_TTL)

TOKEN_VOLUME_CACHE_TTL = float(os.getenv("TOKEN_VOLUME_CACHE_TTL", "60") or 60.0)
TOKEN_VOLUME_CACHE: TTLCache = TTLCache(maxsize=1024, ttl=TOKEN_VOLUME_CACHE_TTL)

TOP_VOLUME_TOKENS_CACHE_TTL = float(os.getenv("TOP_VOLUME_TOKENS_CACHE_TTL", "60") or 60.0)
TOP_VOLUME_TOKENS_CACHE: TTLCache = TTLCache(maxsize=64, ttl=TOP_VOLUME_TOKENS_CACHE_TTL)

_BIRDEYE_MAX_CONCURRENCY = max(1, int(os.getenv("BIRDEYE_MAX_CONCURRENCY", "8") or 8))
_BIRDEYE_SEMAPHORE: asyncio.Semaphore | None = None

ORDER_BOOK_DEPTH_CACHE_TTL = float(os.getenv("ORDER_BOOK_DEPTH_CACHE_TTL", "30") or 30.0)
ORDER_BOOK_DEPTH_CACHE: TTLCache = TTLCache(maxsize=1024, ttl=ORDER_BOOK_DEPTH_CACHE_TTL)

DEXSCREENER_BASE = os.getenv(
    "DEXSCREENER_BASE", "https://api.dexscreener.com/latest/dex"
)


def _get_birdeye_semaphore() -> asyncio.Semaphore:
    global _BIRDEYE_SEMAPHORE
    if _BIRDEYE_SEMAPHORE is None:
        _BIRDEYE_SEMAPHORE = asyncio.Semaphore(_BIRDEYE_MAX_CONCURRENCY)
    return _BIRDEYE_SEMAPHORE

# === Hard defaults per your request ===
DEFAULT_SOLANA_RPC = "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", DEFAULT_SOLANA_RPC)

# Your Birdeye key; allow env override but default to what you provided
DEFAULT_BIRDEYE_API_KEY = "b1e60d72780940d1bd929b9b2e9225e6"
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY", DEFAULT_BIRDEYE_API_KEY)

# Birdeye base (key goes in X-API-KEY header)
BIRDEYE_BASE = "https://public-api.birdeye.so"  # requires ?chain=solana

# Helius slippage / price impact endpoint
HELIUS_PRICE_IMPACT_URL = os.getenv(
    "HELIUS_PRICE_IMPACT_URL", "https://api.helius.xyz/v0/price-impact"
)

# Canonical SOL mint for quotes
SOL_MINT = "So11111111111111111111111111111111111111112"

HELIUS_API_BASE = os.getenv("HELIUS_API_BASE", "https://api.helius.xyz")


def _helius_api_key() -> str:
    key = os.getenv("HELIUS_API_KEY", "").strip()
    if key:
        return key
    try:
        parsed = urlparse(SOLANA_RPC_URL)
    except Exception:
        return ""
    qs = parse_qs(parsed.query or "")
    vals = qs.get("api-key") or qs.get("apikey") or qs.get("apiKey")
    if vals:
        return vals[0]
    return ""

__all__ = [
    "fetch_dex_metrics_async",
    "fetch_dex_metrics",
    "fetch_slippage_onchain_async",
    "fetch_slippage_onchain",
    "order_book_depth_change",
    "collect_onchain_insights_async",
    "collect_onchain_insights",
    "fetch_mempool_tx_rate",
    "fetch_whale_wallet_activity",
    "fetch_average_swap_size",
    "fetch_liquidity_onchain_async",
    "fetch_liquidity_onchain",
    "fetch_volume_onchain_async",
    "fetch_volume_onchain",
    "LiquiditySnapshot",
]

# -------------------- lightweight value container --------------------

class LiquiditySnapshot(dict):
    """Mapping with float semantics for downstream compatibility.

    Code that previously treated the liquidity helper as returning a numeric
    value can continue to call ``float(snapshot)`` while richer callers can
    inspect the stored metadata (pool count, slot, timestamp, ...).
    """

    __slots__ = ()

    def __float__(self) -> float:  # pragma: no cover - trivial conversion
        try:
            return float(self.get("liquidity_usd", 0.0) or 0.0)
        except Exception:
            return 0.0

    def __int__(self) -> int:  # pragma: no cover - trivial conversion
        return int(float(self))

    @property
    def value(self) -> float:
        """Explicit accessor matching the numeric semantics."""
        return float(self)

# -------------------- small utils --------------------

def _now_ts() -> int:
    return int(time.time())

def _numeric(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value))
    except Exception:
        return float(default)

def _int_numeric(value: Any, default: int = 0) -> int:
    try:
        v = _numeric(value, default)
        return int(v)
    except Exception:
        return int(default)

def _build_default_metrics(mint: str) -> Dict[str, Any]:
    return {
        "mint": mint,
        "price": 0.0,
        "price_24h_change": 0.0,
        "volume_24h": 0.0,
        "liquidity_usd": 0.0,
        "pool_count": 0,
        "holders": 0,
        "decimals": 9,
        "slot": 0,
        "ts": _now_ts(),
        "ohlcv_5m": [],
        "ohlcv_1h": [],
    }

async def _json(session: aiohttp.ClientSession, r: aiohttp.ClientResponse) -> Dict[str, Any]:
    try:
        return await r.json(content_type=None)
    except Exception:
        try:
            txt = await r.text()
            return {"text": txt}
        except Exception:
            return {}

# -------------------- Birdeye --------------------

async def _birdeye_get(session: aiohttp.ClientSession, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    params = dict(params or {})
    params.setdefault("chain", "solana")
    url = f"{BIRDEYE_BASE}{path}"
    sem = _get_birdeye_semaphore()
    for attempt in range(3):
        try:
            async with sem:
                async with session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=8),
                ) as r:
                    if r.status == 429:
                        await asyncio.sleep(0.6 * (attempt + 1))
                        continue
                    if r.status >= 400:
                        return {}
                    return await _json(session, r)
        except asyncio.TimeoutError:
            continue
        except Exception:
            return {}
    return {}

async def _birdeye_price_overview(session: aiohttp.ClientSession, mint: str) -> Tuple[float, float, float, float, int, int, Optional[int]]:
    """
    Returns: price, change24h, volume24h, liquidity, holders, pool_count, decimals_or_none
    """
    price = 0.0
    change24h = 0.0

    p_json = await _birdeye_get(session, "/defi/price", {"address": mint})
    p_data = (p_json.get("data") if isinstance(p_json, dict) else None) or {}
    price = _numeric(p_data.get("value"), 0.0)
    if "priceChange24h" in p_data:
        change24h = _numeric(p_data.get("priceChange24h"), 0.0)

    o_json = await _birdeye_get(session, "/defi/token_overview", {"address": mint})
    o_data = (o_json.get("data") if isinstance(o_json, dict) else None) or {}

    volume24h = _numeric(o_data.get("v24h"), 0.0) or _numeric(o_data.get("volume24h"), 0.0)
    liquidity = _numeric(o_data.get("liquidity"), 0.0)
    holders = _int_numeric(o_data.get("holders"), 0)
    pool_count = _int_numeric(o_data.get("poolCount", o_data.get("pools")), 0)
    decimals = _int_numeric(o_data.get("decimals"), 0) if "decimals" in o_data else None

    if "priceChange24h" in o_data:
        change24h = _numeric(o_data.get("priceChange24h"), change24h)

    return price, change24h, volume24h, liquidity, holders, pool_count, decimals

async def _birdeye_ohlcv(session: aiohttp.ClientSession, mint: str, tf: str, limit: int = 60) -> List[Dict[str, Any]]:
    j = await _birdeye_get(session, "/defi/ohlcv", {"address": mint, "type": tf, "limit": limit})
    data = (j.get("data") if isinstance(j, dict) else None) or {}
    items = data.get("items") or data.get("list") or []
    out: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            try:
                ts, o, h, l, c, v = it
                out.append({
                    "ts": _int_numeric(ts, 0),
                    "o": _numeric(o, 0.0),
                    "h": _numeric(h, 0.0),
                    "l": _numeric(l, 0.0),
                    "c": _numeric(c, 0.0),
                    "v": _numeric(v, 0.0),
                })
            except Exception:
                continue
        else:
            out.append({
                "ts": _int_numeric(it.get("unixTime") or it.get("time") or it.get("ts"), 0),
                "o": _numeric(it.get("o") or it.get("open"), 0.0),
                "h": _numeric(it.get("h") or it.get("high"), 0.0),
                "l": _numeric(it.get("l") or it.get("low"), 0.0),
                "c": _numeric(it.get("c") or it.get("close"), 0.0),
                "v": _numeric(it.get("v") or it.get("volume"), 0.0),
            })
    return out


def _metrics_has_data(metrics: Dict[str, Any] | None) -> bool:
    if not isinstance(metrics, dict):
        return False
    return any(
        [
            _numeric(metrics.get("price"), 0.0) > 0.0,
            _numeric(metrics.get("volume_24h"), 0.0) > 0.0,
            _numeric(metrics.get("liquidity_usd"), 0.0) > 0.0,
            _int_numeric(metrics.get("holders"), 0) > 0,
            _int_numeric(metrics.get("pool_count"), 0) > 0,
        ]
    )


async def _fetch_metrics_birdeye(
    session: aiohttp.ClientSession, mint: str
) -> Dict[str, Any]:
    cache_key = ("birdeye_metrics", FAST_MODE, mint)
    cached = DEX_METRICS_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    price_task = asyncio.create_task(_birdeye_price_overview(session, mint))
    ohlcv5_task: asyncio.Task | None = None
    ohlcv1h_task: asyncio.Task | None = None
    if not FAST_MODE:
        ohlcv5_task = asyncio.create_task(_birdeye_ohlcv(session, mint, "5m", limit=60))
        ohlcv1h_task = asyncio.create_task(_birdeye_ohlcv(session, mint, "1h", limit=48))

    try:
        price, chg, vol, liq, holders, pools, decimals_be = await price_task
    except Exception:
        price, chg, vol, liq, holders, pools, decimals_be = 0.0, 0.0, 0.0, 0.0, 0, 0, None

    o5: List[Dict[str, Any]] = []
    o1: List[Dict[str, Any]] = []
    if ohlcv5_task is not None:
        try:
            o5 = await ohlcv5_task
        except Exception:
            o5 = []
    if ohlcv1h_task is not None:
        try:
            o1 = await ohlcv1h_task
        except Exception:
            o1 = []

    result = {
        "price": _numeric(price, 0.0),
        "price_24h_change": _numeric(chg, 0.0),
        "volume_24h": _numeric(vol, 0.0),
        "liquidity_usd": _numeric(liq, 0.0),
        "holders": _int_numeric(holders, 0),
        "pool_count": _int_numeric(pools, 0),
        "decimals": _int_numeric(decimals_be, 0) if decimals_be is not None else None,
        "ohlcv_5m": o5,
        "ohlcv_1h": o1,
    }

    DEX_METRICS_CACHE.set(cache_key, copy.deepcopy(result))
    return copy.deepcopy(result)


async def _fetch_metrics_dexscreener(
    session: aiohttp.ClientSession, mint: str
) -> Dict[str, Any]:
    cache_key = ("dexscreener_metrics", FAST_MODE, mint)
    cached = DEX_METRICS_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    endpoint = f"{DEXSCREENER_BASE.rstrip('/')}/tokens/{mint}"
    try:
        async with session.get(endpoint, timeout=aiohttp.ClientTimeout(total=8)) as resp:
            if resp.status >= 400:
                data: Dict[str, Any] | None = None
            else:
                data = await _json(session, resp)
    except Exception:
        data = None

    pairs = []
    if isinstance(data, dict):
        raw_pairs = data.get("pairs")
        if isinstance(raw_pairs, list):
            pairs = [p for p in raw_pairs if isinstance(p, dict)]

    best: Dict[str, Any] | None = None
    if pairs:
        best = max(
            pairs,
            key=lambda p: _numeric((p.get("liquidity") or {}).get("usd"), 0.0),
        )

    liquidity = 0.0
    volume = 0.0
    price = 0.0
    change = 0.0
    holders = 0
    decimals: Optional[int] = None

    if isinstance(best, dict):
        liquidity = _numeric((best.get("liquidity") or {}).get("usd"), 0.0)
        volume = _numeric((best.get("volume") or {}).get("h24"), 0.0)
        change = _numeric((best.get("priceChange") or {}).get("h24"), 0.0)
        price = _numeric(best.get("priceUsd") or best.get("priceUSD"), 0.0)
        holders = _int_numeric((best.get("holders") or 0), 0)
        base_token = best.get("baseToken")
        if isinstance(base_token, dict) and "decimals" in base_token:
            cand = _int_numeric(base_token.get("decimals"), 0)
            if cand > 0:
                decimals = cand

    result = {
        "price": price,
        "price_24h_change": change,
        "volume_24h": volume,
        "liquidity_usd": liquidity,
        "holders": holders,
        "pool_count": len(pairs),
        "decimals": decimals,
        "ohlcv_5m": [],
        "ohlcv_1h": [],
    }

    DEX_METRICS_CACHE.set(cache_key, copy.deepcopy(result))
    return copy.deepcopy(result)

# -------------------- Helius REST --------------------

def _helius_pick_entry(payload: Any, mint: str) -> Dict[str, Any] | None:
    if isinstance(payload, dict):
        for key in (mint, mint.lower(), mint.upper()):
            val = payload.get(key)
            if isinstance(val, dict):
                return val
        if len(payload) == 1:
            sole = next(iter(payload.values()))
            if isinstance(sole, dict):
                return sole
    if isinstance(payload, list):
        mint_lower = mint.lower()
        for item in payload:
            if isinstance(item, dict):
                ident = item.get("id") or item.get("mint") or item.get("address") or item.get("symbol")
                if isinstance(ident, str) and ident.lower() == mint_lower:
                    return item
        if len(payload) == 1 and isinstance(payload[0], dict):
            return payload[0]
    return None


async def _helius_price_overview(session: aiohttp.ClientSession, mint: str) -> Tuple[float, float, float, float, int, int, Optional[int]]:
    cache_key = ("helius_price", mint)
    cached = DEX_METRICS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    api_key = _helius_api_key()
    if not api_key:
        result = (0.0, 0.0, 0.0, 0.0, 0, 0, None)
        DEX_METRICS_CACHE.set(cache_key, result)
        return result

    headers = {"accept": "application/json"}
    price_params = {"ids": mint, "vs": "usd", "api-key": api_key}
    market_params = {"mint": mint, "api-key": api_key}

    async def _request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{HELIUS_API_BASE.rstrip('/')}{path}"
        try:
            async with session.get(url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as r:
                if r.status >= 400:
                    return {}
                return await _json(session, r)
        except Exception:
            return {}

    price_json = await _request("/v0/price", price_params)
    price_data = price_json.get("data") if isinstance(price_json, dict) else None
    if price_data is None:
        price_data = price_json
    price_entry = _helius_pick_entry(price_data, mint)

    market_json = await _request("/v0/market-stats", market_params)
    market_data = market_json.get("data") if isinstance(market_json, dict) else None
    if market_data is None:
        market_data = market_json
    market_entry = _helius_pick_entry(market_data, mint)

    price = _numeric(price_entry.get("price") if price_entry else 0.0, 0.0)
    change = 0.0
    if isinstance(price_entry, dict):
        for key in ("priceChange24h", "change24h", "percentChange24h", "price_change_24h"):
            if key in price_entry:
                change = _numeric(price_entry.get(key), change)
                break
        if "value" in price_entry and price == 0.0:
            price = _numeric(price_entry.get("value"), price)

    volume = 0.0
    liquidity = 0.0
    holders = 0
    pool_count = 0
    decimals: Optional[int] = None

    if isinstance(market_entry, dict):
        for key in ("volume24hUsd", "volume24h", "volumeUsd24h", "totalVolume24hUsd", "volume_24h_usd"):
            if key in market_entry:
                volume = _numeric(market_entry.get(key), volume)
                break
        for key in ("liquidityUsd", "liquidity", "liquidity_usd"):
            if key in market_entry:
                liquidity = _numeric(market_entry.get(key), liquidity)
                break
        for key in ("holders", "holderCount", "holder_count"):
            if key in market_entry:
                holders = _int_numeric(market_entry.get(key), holders)
                break
        for key in ("poolCount", "marketCount", "dexCount"):
            if key in market_entry:
                pool_count = _int_numeric(market_entry.get(key), pool_count)
                break
        for key in ("decimals", "tokenDecimals"):
            if key in market_entry:
                cand = _int_numeric(market_entry.get(key), 0)
                if cand > 0:
                    decimals = cand
                break

    result = (price, change, volume, liquidity, holders, pool_count, decimals)
    DEX_METRICS_CACHE.set(cache_key, result)
    return result

# -------------------- Helius RPC --------------------

async def _rpc_post(session: aiohttp.ClientSession, method: str, params: Any, rpc_url: Optional[str]) -> Dict[str, Any]:
    url = rpc_url or SOLANA_RPC_URL
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=6)) as r:
            if r.status >= 400:
                return {}
            return await _json(session, r)
    except Exception:
        return {}

async def _helius_slot(session: aiohttp.ClientSession, rpc_url: Optional[str]) -> int:
    cache_key = rpc_url or "default"
    cached = HELIUS_SLOT_CACHE.get(cache_key)
    if cached is not None:
        return int(cached)

    j = await _rpc_post(session, "getSlot", [], rpc_url)
    slot = _int_numeric((j.get("result") if isinstance(j, dict) else None), 0)
    if slot > 0:
        HELIUS_SLOT_CACHE.set(cache_key, slot)
    return slot

async def _helius_decimals(session: aiohttp.ClientSession, mint: str, rpc_url: Optional[str]) -> int:
    # getTokenSupply returns decimals
    cache_key = (mint, rpc_url or "default")
    cached = HELIUS_DECIMALS_CACHE.get(cache_key)
    if cached is not None:
        return int(cached)

    params = [mint, {"commitment": "processed"}]
    j = await _rpc_post(session, "getTokenSupply", params, rpc_url)
    res = (j.get("result") if isinstance(j, dict) else None) or {}
    value = (res.get("value") if isinstance(res, dict) else None) or {}
    decimals = _int_numeric(value.get("decimals"), 9)
    HELIUS_DECIMALS_CACHE.set(cache_key, decimals)
    return decimals

# -------------------- Jupiter slippage / price impact --------------------

def _extract_price_impact(data: Any) -> Optional[float]:
    if data is None:
        return None
    if isinstance(data, (int, float)):
        try:
            return float(data)
        except Exception:
            return None
    if isinstance(data, dict):
        for key in (
            "priceImpactPct",
            "price_impact_pct",
            "impactPct",
            "impactPercentage",
            "priceImpactPercent",
        ):
            if key in data:
                val = _numeric(data.get(key), 0.0)
                return float(val)
        for key in ("data", "result", "routePlan", "routes", "route"):
            sub = data.get(key)
            cand = _extract_price_impact(sub)
            if cand is not None:
                return cand
        if "swapInfo" in data:
            cand = _extract_price_impact(data.get("swapInfo"))
            if cand is not None:
                return cand
    if isinstance(data, list):
        for item in data:
            cand = _extract_price_impact(item)
            if cand is not None:
                return cand
    return None


async def _jupiter_quote_price_impact_pct(
    session: aiohttp.ClientSession,
    out_mint: str,
    *,
    in_mint: str = SOL_MINT,
    in_amount_sol: float = 0.1,
) -> float:
    api_key = _helius_api_key()
    url = HELIUS_PRICE_IMPACT_URL
    if not api_key or not url:
        return 0.0

    lamports = max(1, int(in_amount_sol * 1_000_000_000))
    params = {
        "inputMint": in_mint,
        "outputMint": out_mint,
        "amount": str(lamports),
        "api-key": api_key,
    }
    headers = {"accept": "application/json", "x-api-key": api_key}
    timeout = aiohttp.ClientTimeout(total=8)

    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=headers, timeout=timeout) as r:
                if r.status == 429:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                if r.status in {401, 403}:
                    return 0.0
                if r.status >= 400:
                    break
                data = await _json(session, r)
        except asyncio.TimeoutError:
            continue
        except Exception:
            return 0.0

        impact = _extract_price_impact(data)
        if impact is not None:
            return float(impact)
    return 0.0

async def fetch_slippage_onchain_async(
    mint: str,
    rpc_url: Optional[str] = None,
    *,
    in_amount_sol: float = 0.1,
) -> Dict[str, Any]:
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        dec_task = asyncio.create_task(_helius_decimals(session, mint, rpc_url))
        impact = await _jupiter_quote_price_impact_pct(session, mint, in_amount_sol=in_amount_sol)
        try:
            _ = await dec_task
        except Exception:
            pass

    bps = int(abs(impact) * 10_000)
    return {
        "mint": mint,
        "price_impact_pct": float(impact),
        "slippage_bps_est": int(bps),
        "in_amount_sol": float(in_amount_sol),
        "ts": _now_ts(),
    }

def fetch_slippage_onchain(
    mint: str,
    rpc_url: Optional[str] = None,
    *,
    in_amount_sol: float = 0.1,
) -> Dict[str, Any]:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        return asyncio.run_coroutine_threadsafe(
            fetch_slippage_onchain_async(mint, rpc_url=rpc_url, in_amount_sol=in_amount_sol), loop
        ).result()
    return asyncio.run(fetch_slippage_onchain_async(mint, rpc_url=rpc_url, in_amount_sol=in_amount_sol))

# -------------------- Order book / insight helpers --------------------

async def _order_book_depth_change_async(
    mint: str,
    *,
    rpc_url: Optional[str] = None,
    base_url: str | None = None,
) -> float:
    rpc = rpc_url or SOLANA_RPC_URL
    cache_key = (mint, rpc, base_url or "")
    try:
        metrics = await fetch_dex_metrics_async(mint, rpc_url=rpc, base_url=base_url)
    except Exception:
        metrics = {}
    depth_val = 0.0
    if isinstance(metrics, dict):
        depth_val = _numeric(metrics.get("depth"), 0.0)
    previous = ORDER_BOOK_DEPTH_CACHE.get(cache_key)
    ORDER_BOOK_DEPTH_CACHE.set(cache_key, depth_val)
    if previous is None:
        return 0.0
    prev_val = _numeric(previous, 0.0)
    return _numeric(depth_val - prev_val, 0.0)


def order_book_depth_change(
    mint: str,
    *,
    base_url: str | None = None,
    rpc_url: Optional[str] = None,
) -> float:
    """Return the change in reported order book depth since the previous call."""

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    coro = _order_book_depth_change_async(mint, rpc_url=rpc_url, base_url=base_url)
    if loop and loop.is_running():
        return asyncio.run_coroutine_threadsafe(coro, loop).result()
    return asyncio.run(coro)


def fetch_mempool_tx_rate(
    token: str,
    rpc_url: Optional[str] = None,
    *,
    limit: int = 20,
) -> float:
    rpc = rpc_url or SOLANA_RPC_URL
    return _scanner_fetch_mempool_tx_rate(token, rpc, limit=limit)


def fetch_whale_wallet_activity(
    token: str,
    rpc_url: Optional[str] = None,
    *,
    threshold: float = 1_000_000.0,
) -> float:
    rpc = rpc_url or SOLANA_RPC_URL
    return _scanner_fetch_whale_wallet_activity(token, rpc, threshold=threshold)


def fetch_average_swap_size(
    token: str,
    rpc_url: Optional[str] = None,
    *,
    limit: int = 20,
) -> float:
    rpc = rpc_url or SOLANA_RPC_URL
    return _scanner_fetch_average_swap_size(token, rpc, limit=limit)


async def collect_onchain_insights_async(
    token: str,
    rpc_url: Optional[str] = None,
) -> Dict[str, float]:
    rpc = rpc_url or SOLANA_RPC_URL
    depth = await _order_book_depth_change_async(token, rpc_url=rpc)
    tx_rate_task = asyncio.to_thread(fetch_mempool_tx_rate, token, rpc, limit=20)
    whale_task = asyncio.to_thread(fetch_whale_wallet_activity, token, rpc)
    swap_task = asyncio.to_thread(fetch_average_swap_size, token, rpc, limit=20)
    tx_rate, whale_activity, avg_swap = await asyncio.gather(
        tx_rate_task,
        whale_task,
        swap_task,
    )
    return {
        "depth_change": _numeric(depth, 0.0),
        "tx_rate": _numeric(tx_rate, 0.0),
        "whale_activity": _numeric(whale_activity, 0.0),
        "avg_swap_size": _numeric(avg_swap, 0.0),
    }


def collect_onchain_insights(
    token: str,
    rpc_url: Optional[str] = None,
) -> Dict[str, float]:
    rpc = rpc_url or SOLANA_RPC_URL
    depth = order_book_depth_change(token)
    tx_rate = fetch_mempool_tx_rate(token, rpc)
    whale_activity = fetch_whale_wallet_activity(token, rpc)
    avg_swap = fetch_average_swap_size(token, rpc)
    return {
        "depth_change": _numeric(depth, 0.0),
        "tx_rate": _numeric(tx_rate, 0.0),
        "whale_activity": _numeric(whale_activity, 0.0),
        "avg_swap_size": _numeric(avg_swap, 0.0),
    }

# -------------------- DEX metrics (Birdeye + Helius) --------------------

async def fetch_dex_metrics_async(
    mint: str,
    *,
    rpc_url: Optional[str] = None,
    base_url: str | None = None,
) -> Dict[str, Any]:
    _ = base_url  # retained for backward compatibility with older tests/callers
    cache_key = (mint, FAST_MODE, rpc_url or "")
    cached = DEX_METRICS_CACHE.get(cache_key)
    if cached is not None:
        return copy.deepcopy(cached)

    base = _build_default_metrics(mint)
    session = await get_session()
    slot_task = asyncio.create_task(_helius_slot(session, rpc_url))
    dec_task = asyncio.create_task(_helius_decimals(session, mint, rpc_url))

    helius_price: Tuple[float, float, float, float, int, int, Optional[int]]
    try:
        helius_price = await _helius_price_overview(session, mint)
    except Exception:
        helius_price = (0.0, 0.0, 0.0, 0.0, 0, 0, None)

    price_h, chg_h, vol_h, liq_h, holders_h, pools_h, decimals_h = helius_price
    helius_metrics = {
        "price": _numeric(price_h, 0.0),
        "price_24h_change": _numeric(chg_h, 0.0),
        "volume_24h": _numeric(vol_h, 0.0),
        "liquidity_usd": _numeric(liq_h, 0.0),
        "holders": _int_numeric(holders_h, 0),
        "pool_count": _int_numeric(pools_h, 0),
        "decimals": _int_numeric(decimals_h, 0) if decimals_h is not None else None,
        "ohlcv_5m": [],
        "ohlcv_1h": [],
    }

    selected_metrics: Dict[str, Any] | None
    if _metrics_has_data(helius_metrics):
        selected_metrics = helius_metrics
    else:
        try:
            birdeye_metrics = await _fetch_metrics_birdeye(session, mint)
        except Exception:
            birdeye_metrics = {}
        if _metrics_has_data(birdeye_metrics):
            selected_metrics = birdeye_metrics
        else:
            try:
                tertiary_metrics = await _fetch_metrics_dexscreener(session, mint)
            except Exception:
                tertiary_metrics = {}
            if _metrics_has_data(tertiary_metrics):
                selected_metrics = tertiary_metrics
            else:
                selected_metrics = None

    try:
        slot = await slot_task
    except Exception:
        slot = 0
    try:
        decimals_rpc = await dec_task
    except Exception:
        decimals_rpc = 0

    selected = selected_metrics or {}
    decimals_hint = selected.get("decimals") if isinstance(selected, dict) else None
    decimals_final = decimals_rpc if decimals_rpc > 0 else (
        decimals_hint if isinstance(decimals_hint, (int, float)) and decimals_hint > 0 else 9
    )

    ohlcv5_val = selected.get("ohlcv_5m") if isinstance(selected, dict) else []
    ohlcv1_val = selected.get("ohlcv_1h") if isinstance(selected, dict) else []

    base.update({
        "price": _numeric(selected.get("price"), 0.0),
        "price_24h_change": _numeric(selected.get("price_24h_change"), 0.0),
        "volume_24h": _numeric(selected.get("volume_24h"), 0.0),
        "liquidity_usd": _numeric(selected.get("liquidity_usd"), 0.0),
        "holders": _int_numeric(selected.get("holders"), 0),
        "pool_count": _int_numeric(selected.get("pool_count"), 0),
        "decimals": _int_numeric(decimals_final, 9),
        "slot": _int_numeric(slot, 0),
        "ohlcv_5m": list(ohlcv5_val) if isinstance(ohlcv5_val, list) else [],
        "ohlcv_1h": list(ohlcv1_val) if isinstance(ohlcv1_val, list) else [],
        "ts": _now_ts(),
    })
    cached_entry = copy.deepcopy(base)
    DEX_METRICS_CACHE.set(cache_key, cached_entry)
    return copy.deepcopy(cached_entry)

def fetch_dex_metrics(mint: str, *, rpc_url: Optional[str] = None) -> Dict[str, Any]:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        return asyncio.run_coroutine_threadsafe(
            fetch_dex_metrics_async(mint, rpc_url=rpc_url), loop
        ).result()
    return asyncio.run(fetch_dex_metrics_async(mint, rpc_url=rpc_url))

# -------------------- liquidity-only facade (what callers expect) --------------------

async def fetch_liquidity_onchain_async(
    mint: str,
    rpc_url: Optional[str] = None,
) -> LiquiditySnapshot:
    """
    Lightweight facade used by other modules.
    Uses Birdeye liquidity (USD) and pool_count, plus a slot touch via Helius.
    Returns a :class:`LiquiditySnapshot` with float semantics (``float(snapshot)``
    yields ``liquidity_usd``) so existing numeric consumers remain compatible.
    """
    metrics = await fetch_dex_metrics_async(mint, rpc_url=rpc_url)
    metrics_dict = metrics if isinstance(metrics, dict) else {}
    ts_val = metrics_dict.get("ts")
    snapshot = LiquiditySnapshot(
        {
            "mint": mint,
            "liquidity_usd": _numeric(metrics_dict.get("liquidity_usd"), 0.0),
            "pool_count": _int_numeric(metrics_dict.get("pool_count"), 0),
            "slot": _int_numeric(metrics_dict.get("slot"), 0),
            "ts": _int_numeric(ts_val, _now_ts()),
        }
    )
    return snapshot

def fetch_liquidity_onchain(
    mint: str,
    rpc_url: Optional[str] = None,
) -> LiquiditySnapshot:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        return asyncio.run_coroutine_threadsafe(
            fetch_liquidity_onchain_async(mint, rpc_url=rpc_url), loop
        ).result()
    return asyncio.run(fetch_liquidity_onchain_async(mint, rpc_url=rpc_url))


# -------------------- Volume helpers (reuse scanner implementation) --------------------

async def fetch_volume_onchain_async(
    mint: str,
    rpc_url: Optional[str] = None,
) -> float:
    """Proxy to scanner_onchain volume helper with default RPC."""

    rpc = rpc_url or SOLANA_RPC_URL
    return await _scanner_fetch_volume_async(mint, rpc)


def fetch_volume_onchain(
    mint: str,
    rpc_url: Optional[str] = None,
) -> float:
    rpc = rpc_url or SOLANA_RPC_URL
    return _scanner_fetch_volume(mint, rpc)
