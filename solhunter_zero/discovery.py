from __future__ import annotations

import asyncio
import os
import aiohttp
from typing import List, Dict, Any

# Use only user-provided RPC (e.g. Helius). No public fallback.
SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL")
if not SOLANA_RPC_URL:
    raise RuntimeError("SOLANA_RPC_URL must be set to a valid RPC (e.g. Helius)")

# BirdEye setup (hardcoded API key)
BIRDEYE_API = "https://public-api.birdeye.so/defi/tokenlist"
BIRDEYE_KEY = "b1e60d72780940d1bd929b9b2e9225e6"

HEADERS = {
    "accept": "application/json",
    "x-chain": "solana",
    "X-API-KEY": BIRDEYE_KEY,
}


async def fetch_birdeye_tokens(limit: int = 50) -> List[Dict[str, Any]]:
    """Fetch a batch of new tokens from BirdEye API."""
    url = f"{BIRDEYE_API}?limit={limit}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status != 200:
                raise RuntimeError(f"BirdEye fetch failed: {resp.status}")
            data = await resp.json()
            return data.get("data", [])


async def enrich_with_rpc(tokens: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Enrich BirdEye tokens with Solana RPC metadata (Helius only)."""
    enriched = []
    async with aiohttp.ClientSession() as session:
        for t in tokens:
            mint = t.get("address")
            if not mint:
                continue
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getAccountInfo",
                "params": [mint, {"encoding": "jsonParsed"}],
            }
            async with session.post(SOLANA_RPC_URL, json=body) as resp:
                if resp.status == 200:
                    info = await resp.json()
                    t["rpc_info"] = info.get("result")
            enriched.append(t)
    return enriched


async def discover_tokens(limit: int = 50, enrich: bool = True) -> List[Dict[str, Any]]:
    """Discover new tokens using BirdEye and optionally enrich with RPC data."""
    tokens = await fetch_birdeye_tokens(limit=limit)
    if enrich:
        tokens = await enrich_with_rpc(tokens)
    return tokens


if __name__ == "__main__":
    async def _main():
        out = await discover_tokens(limit=10)
        for t in out:
            print(t)

    asyncio.run(_main())
