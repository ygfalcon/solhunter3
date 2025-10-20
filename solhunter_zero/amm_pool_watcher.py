"""AMM pool watcher fallback publisher."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import time
from typing import Dict, Iterable, List, Sequence, Tuple

import aiohttp
import redis.asyncio as aioredis

DEFAULT_STABLE_TOKENS = {
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9",  # USDC
    "So11111111111111111111111111111111111111112",  # wSOL
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "USDH1M9swnX8i1C5mFZWw577zvuEwmwqsJLAC9B5S3y",  # USDH
}


_SEEN_POOLS: Dict[str, float] = {}
_SEEN_MINT: Dict[str, float] = {}
_PROMOTED: Dict[str, float] = {}
_PRICED: Dict[str, float] = {}


def _now() -> float:
    return time.time()


def _keep_ttl(store: Dict[str, float], ttl: float) -> None:
    if ttl <= 0:
        store.clear()
        return
    if len(store) < 50000:
        return
    cutoff = _now() - ttl
    for key, recorded in list(store.items()):
        if recorded < cutoff:
            store.pop(key, None)


def _parse_float_env(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = default
    if minimum is not None:
        return max(minimum, value)
    return value


def _parse_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = default
    return max(minimum, value)


def _parse_list_env(name: str) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return tuple()
    values: List[str] = []
    for token in raw.split(","):
        token = token.strip()
        if token:
            values.append(token)
    return tuple(values)


def _looks_like_pubkey(value: str) -> bool:
    return isinstance(value, str) and 30 <= len(value) <= 44


def _iter_entries(payload: object) -> Iterable[Dict[str, object]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict):
        for key in ("data", "items", "result", "pools"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                for item in candidate:
                    if isinstance(item, dict):
                        yield item
                return
        if payload:
            for item in payload.values():
                if isinstance(item, list):
                    for entry in item:
                        if isinstance(entry, dict):
                            yield entry
                    return


def _extract_pool_identifier(entry: Dict[str, object]) -> str | None:
    for key in ("poolAddress", "pool", "ammId", "address", "id", "whirlpoolAddress"):
        value = entry.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _extract_candidate_mints(entry: Dict[str, object]) -> List[str]:
    candidates: List[str] = []
    keys = [
        "mint",
        "mintA",
        "mintB",
        "baseMint",
        "quoteMint",
        "tokenMintA",
        "tokenMintB",
        "tokenMint",
        "tokenA",
        "tokenB",
    ]
    for key in keys:
        value = entry.get(key)
        if isinstance(value, str) and _looks_like_pubkey(value):
            candidates.append(value)
        elif isinstance(value, dict):
            for subkey in ("mint", "address", "publicKey"):
                candidate = value.get(subkey)
                if isinstance(candidate, str) and _looks_like_pubkey(candidate):
                    candidates.append(candidate)
    if not candidates:
        for value in entry.values():
            if isinstance(value, str) and _looks_like_pubkey(value):
                candidates.append(value)
    # Preserve order while deduplicating
    seen: Dict[str, None] = {}
    for token in candidates:
        seen.setdefault(token, None)
    return list(seen.keys())


def _extract_pair(entry: Dict[str, object]) -> Tuple[str | None, str | None]:
    pairs = [
        (entry.get("baseMint"), entry.get("quoteMint")),
        (entry.get("mintA"), entry.get("mintB")),
        (entry.get("tokenMintA"), entry.get("tokenMintB")),
        (entry.get("tokenA"), entry.get("tokenB")),
        (entry.get("tokenMint"), entry.get("quoteToken")),
    ]
    for base, quote in pairs:
        if isinstance(base, dict):
            base = base.get("mint") or base.get("address") or base.get("publicKey")
        if isinstance(quote, dict):
            quote = quote.get("mint") or quote.get("address") or quote.get("publicKey")
        if isinstance(base, str) and isinstance(quote, str):
            return base, quote
    return None, None


def _extract_price(entry: Dict[str, object], base: str | None, quote: str | None) -> float | None:
    price_keys = ["price", "price_usd", "priceUsd", "midPrice", "mid_price", "lastPrice"]
    for key in price_keys:
        value = entry.get(key)
        if isinstance(value, (int, float)) and value > 0:
            return float(value)
        if isinstance(value, str):
            with contextlib.suppress(ValueError):
                parsed = float(value)
                if parsed > 0:
                    return parsed
    reserve_pairs = [
        ("baseReserve", "quoteReserve"),
        ("reserveBase", "reserveQuote"),
        ("tokenAmountA", "tokenAmountB"),
        ("base_liquidity", "quote_liquidity"),
        ("coinVault", "pcVault"),
    ]
    for base_key, quote_key in reserve_pairs:
        base_res = entry.get(base_key)
        quote_res = entry.get(quote_key)
        try:
            base_val = float(base_res)
            quote_val = float(quote_res)
            if base_val > 0 and quote_val > 0:
                return quote_val / base_val
        except (TypeError, ValueError):
            continue
    return None


def _extract_liquidity(entry: Dict[str, object]) -> float:
    keys = [
        "liquidityUsd",
        "liquidity_usd",
        "liquidity",
        "totalLiquidity",
        "total_liquidity",
        "volume24hUsd",
        "volume24h",
        "tvl",
        "tvlUsd",
    ]
    for key in keys:
        value = entry.get(key)
        try:
            liquidity = float(value)
            if liquidity > 0:
                return liquidity
        except (TypeError, ValueError):
            continue
    return 0.0


def _choose_focus_mint(
    base: str | None,
    quote: str | None,
    candidates: Sequence[str],
    stable_tokens: Sequence[str],
) -> Tuple[str | None, str | None]:
    stable_set = set(stable_tokens)
    if base and base not in stable_set and quote and quote in stable_set:
        return base, quote
    if quote and quote not in stable_set and base and base in stable_set:
        return quote, base
    for candidate in candidates:
        if candidate not in stable_set:
            if candidate == base and quote:
                return candidate, quote
            if candidate == quote and base:
                return candidate, base
    if candidates:
        focus = candidates[0]
        if focus == base:
            return focus, quote
        if focus == quote:
            return focus, base
    return base, quote


async def _fetch_json(session: aiohttp.ClientSession, url: str) -> object:
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "json" in content_type.lower():
            return await resp.json()
        text = await resp.text()
        with contextlib.suppress(json.JSONDecodeError):
            return json.loads(text)
        return {}


async def run_amm_pool_watcher() -> None:
    redis_url = os.getenv("AMM_WATCH_REDIS_URL", "redis://localhost:6379/0")
    channel = os.getenv("AMM_WATCH_BROKER_CHANNEL", "solhunter-events-v2")
    poll_interval = _parse_int_env("AMM_WATCH_INTERVAL", 90, minimum=15)
    dedup_ttl = _parse_float_env("AMM_WATCH_DEDUP_TTL_SEC", 3600.0, minimum=60.0)

    targets: List[Tuple[str, str]] = []
    raydium_url = os.getenv("RAYDIUM_POOLS_URL")
    meteora_url = os.getenv("METEORA_POOLS_URL")
    orca_url = os.getenv("ORCA_POOLS_URL")

    if raydium_url:
        targets.append(("raydium", raydium_url))
    if meteora_url:
        targets.append(("meteora", meteora_url))
    if orca_url:
        targets.append(("orca", orca_url))

    additional_programs = _parse_list_env("AMM_PROGRAM_IDS")

    if not targets and not additional_programs:
        raise RuntimeError("No AMM endpoints configured for AMM watcher")

    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    stable_tokens = set(DEFAULT_STABLE_TOKENS)
    stable_tokens.update(_parse_list_env("STABLE_TOKENS"))
    stable_tokens.update(_parse_list_env("GOLDEN_SEED_MINTS"))

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                for label, url in targets:
                    try:
                        payload = await _fetch_json(session, url)
                    except Exception:
                        continue
                    for entry in _iter_entries(payload):
                        pool_id = _extract_pool_identifier(entry)
                        if pool_id:
                            if pool_id in _SEEN_POOLS:
                                continue
                            _SEEN_POOLS[pool_id] = _now()
                        now_ts = _now()
                        candidates = _extract_candidate_mints(entry)
                        for mint in candidates:
                            if mint in _SEEN_MINT:
                                continue
                            _SEEN_MINT[mint] = now_ts
                            event = {
                                "topic": "token_discovered",
                                "ts": now_ts,
                                "source": "amm_watch",
                                "mint": mint,
                                "tx": pool_id or "",
                                "tags": ["pool_created", label],
                                "interface": "FungibleToken",
                                "discovery": {
                                    "method": f"pool_{label}",
                                    "endpoint": url,
                                },
                            }
                            await redis_client.publish(channel, json.dumps(event, separators=(",", ":")))
                        base, quote = _extract_pair(entry)
                        focus_mint, quote_token = _choose_focus_mint(base, quote, candidates, stable_tokens)
                        if not focus_mint or not quote_token:
                            continue
                        if focus_mint in _PROMOTED:
                            continue
                        liquidity = _extract_liquidity(entry)
                        price = _extract_price(entry, base, quote)
                        _PROMOTED[focus_mint] = now_ts
                        promoted = {
                            "topic": "token_promoted",
                            "ts": now_ts,
                            "source": "amm_watch",
                            "mint": focus_mint,
                            "pool": pool_id or "",
                            "quote": quote_token,
                            "liquidity": liquidity,
                            "price": price,
                            "dex": label,
                        }
                        await redis_client.publish(channel, json.dumps(promoted, separators=(",", ":")))
                        if liquidity > 0:
                            depth_payload = {
                                "topic": "depth_update",
                                "entries": {
                                    focus_mint: {
                                        "bids": liquidity,
                                        "asks": liquidity,
                                        "dex": {
                                            label: {
                                                "bids": liquidity,
                                                "asks": liquidity,
                                                "tx_rate": 0.0,
                                            }
                                        },
                                        "tx_rate": 0.0,
                                        "ts": int(now_ts),
                                    }
                                },
                            }
                            await redis_client.publish(channel, json.dumps(depth_payload, separators=(",", ":")))
                        if price and (_PRICED.get(focus_mint, 0) + 30) < now_ts:
                            _PRICED[focus_mint] = now_ts
                            price_event = {
                                "topic": "price_update",
                                "venue": f"{label}_bootstrap",
                                "token": focus_mint,
                                "price": price,
                                "ts": now_ts,
                            }
                            await redis_client.publish(channel, json.dumps(price_event, separators=(",", ":")))
        except Exception:
            await asyncio.sleep(min(5, poll_interval))
        else:
            await asyncio.sleep(poll_interval)
        finally:
            _keep_ttl(_SEEN_POOLS, dedup_ttl)
            _keep_ttl(_SEEN_MINT, dedup_ttl)
            _keep_ttl(_PROMOTED, dedup_ttl)
            _keep_ttl(_PRICED, dedup_ttl)
