from __future__ import annotations

import asyncio
import logging
import os
from typing import Iterable, List, Sequence, Dict

import aiohttp

# Hard-coded Helius defaults (per your request)
DEFAULT_SOLANA_RPC = "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"

BIRDEYE_BASE = "https://public-api.birdeye.so"

logger = logging.getLogger(__name__)

FAST_MODE = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
_BIRDEYE_TIMEOUT = float(os.getenv("FAST_BIRDEYE_TIMEOUT", "6.0")) if FAST_MODE else 10.0
_BIRDEYE_PAGE_DELAY = float(os.getenv("FAST_BIRDEYE_PAGE_DELAY", "0.35")) if FAST_MODE else 1.1

__all__ = [
    "scan_tokens_async",
    "enrich_tokens_async",
]


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

    headers = {
        "accept": "application/json",
        "X-API-KEY": api_key,
        "x-chain": os.getenv("BIRDEYE_CHAIN", "solana"),
    }
    params = {"offset": int(offset), "limit": int(limit)}
    url = f"{BIRDEYE_BASE}/defi/token_trending"

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
                resp.raise_for_status()
                data = await resp.json(content_type=None)
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
    return []


async def scan_tokens_async(
    *,
    rpc_url: str = DEFAULT_SOLANA_RPC,
    limit: int = 50,
    enrich: bool = True,   # kept for compatibility; enrichment is separate call
    api_key: str | None = None,
) -> List[str]:
    """
    Pull trending mints from Birdeye (using api_key).
    If empty, fall back to a small static set so the loop can proceed.
    """
    _ = rpc_url  # reserved for future use; enrichment uses this parameter
    requested = max(1, int(limit))
    api_key = api_key or ""
    mints: List[str] = []

    async with aiohttp.ClientSession() as session:
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
                if mint not in mints:
                    mints.append(mint)
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
