# solhunter_zero/onchain_metrics.py
from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, Optional, Tuple, List

import copy

import aiohttp

from .scanner_onchain import (
    fetch_volume_onchain_async as _scanner_fetch_volume_async,
    fetch_volume_onchain as _scanner_fetch_volume,
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

# Jupiter v6 quote API for price impact / slippage
JUPITER_QUOTE_V6 = "https://quote-api.jup.ag/v6/quote"

# Canonical SOL mint for quotes
SOL_MINT = "So11111111111111111111111111111111111111112"

__all__ = [
    "fetch_dex_metrics_async",
    "fetch_dex_metrics",
    "fetch_slippage_onchain_async",
    "fetch_slippage_onchain",
    "fetch_liquidity_onchain_async",
    "fetch_liquidity_onchain",
    "fetch_volume_onchain_async",
    "fetch_volume_onchain",
]

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

async def _jupiter_quote_price_impact_pct(
    session: aiohttp.ClientSession,
    out_mint: str,
    *,
    in_mint: str = SOL_MINT,
    in_amount_sol: float = 0.1,
) -> float:
    lamports = max(1, int(in_amount_sol * 1_000_000_000))
    params = {
        "inputMint": in_mint,
        "outputMint": out_mint,
        "amount": str(lamports),
        "onlyDirectRoutes": "false",
        "asLegacyTransaction": "false",
    }
    for attempt in range(3):
        try:
            async with session.get(JUPITER_QUOTE_V6, params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 429:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                if r.status >= 400:
                    return 0.0
                data = await _json(session, r)
        except asyncio.TimeoutError:
            continue
        except Exception:
            return 0.0

        if isinstance(data, dict):
            if "priceImpactPct" in data:
                return float(_numeric(data.get("priceImpactPct"), 0.0))
            for key in ("data", "routes", "routePlan"):
                rt = data.get(key)
                if isinstance(rt, list) and rt:
                    cand = rt[0]
                    if isinstance(cand, dict):
                        if "priceImpactPct" in cand:
                            return float(_numeric(cand.get("priceImpactPct"), 0.0))
                        swap = cand.get("swapInfo") or {}
                        if "priceImpactPct" in swap:
                            return float(_numeric(swap.get("priceImpactPct"), 0.0))
        break
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

    price_task = asyncio.create_task(_birdeye_price_overview(session, mint))
    ohlcv5_task: asyncio.Task | None = None
    ohlcv1h_task: asyncio.Task | None = None
    if not FAST_MODE:
        ohlcv5_task = asyncio.create_task(_birdeye_ohlcv(session, mint, "5m", limit=60))
        ohlcv1h_task = asyncio.create_task(_birdeye_ohlcv(session, mint, "1h", limit=48))
    slot_task = asyncio.create_task(_helius_slot(session, rpc_url))
    dec_task = asyncio.create_task(_helius_decimals(session, mint, rpc_url))

    try:
        price, chg, vol, liq, holders, pools, decimals_be = await price_task
    except Exception:
        price, chg, vol, liq, holders, pools, decimals_be = 0.0, 0.0, 0.0, 0.0, 0, 0, None

    o5: list[Dict[str, Any]] = []
    o1: list[Dict[str, Any]] = []
    if ohlcv5_task:
        try:
            o5 = await ohlcv5_task
        except Exception:
            o5 = []
    if ohlcv1h_task:
        try:
            o1 = await ohlcv1h_task
        except Exception:
            o1 = []

    try:
        slot = await slot_task
    except Exception:
        slot = 0
    try:
        decimals_rpc = await dec_task
    except Exception:
        decimals_rpc = 0

    decimals_final = decimals_rpc if decimals_rpc > 0 else (decimals_be if decimals_be is not None else 9)

    base.update({
        "price": _numeric(price, 0.0),
        "price_24h_change": _numeric(chg, 0.0),
        "volume_24h": _numeric(vol, 0.0),
        "liquidity_usd": _numeric(liq, 0.0),
        "holders": _int_numeric(holders, 0),
        "pool_count": _int_numeric(pools, 0),
        "decimals": _int_numeric(decimals_final, 9),
        "slot": _int_numeric(slot, 0),
        "ohlcv_5m": o5,
        "ohlcv_1h": o1,
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
) -> Dict[str, Any]:
    """
    Lightweight facade used by other modules.
    Uses Birdeye liquidity (USD) and pool_count, plus a slot touch via Helius.
    Returns:
      {
        "mint": str,
        "liquidity_usd": float,
        "pool_count": int,
        "slot": int,
        "ts": int
      }
    """
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        # Run BE overview + slot concurrently
        ov_task = asyncio.create_task(_birdeye_price_overview(session, mint))
        slot_task = asyncio.create_task(_helius_slot(session, rpc_url))
        try:
            _, _, _, liq, _, pools, _ = await ov_task
        except Exception:
            liq, pools = 0.0, 0
        try:
            slot = await slot_task
        except Exception:
            slot = 0

    return {
        "mint": mint,
        "liquidity_usd": _numeric(liq, 0.0),
        "pool_count": _int_numeric(pools, 0),
        "slot": _int_numeric(slot, 0),
        "ts": _now_ts(),
    }

def fetch_liquidity_onchain(
    mint: str,
    rpc_url: Optional[str] = None,
) -> Dict[str, Any]:
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
