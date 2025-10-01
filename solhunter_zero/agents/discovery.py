from __future__ import annotations

import logging
import os
import time
from typing import List, Optional

from ..event_bus import publish
from ..schemas import RuntimeLog
from ..token_scanner import scan_tokens_async, enrich_tokens_async

logger = logging.getLogger(__name__)


_CACHE: dict[str, object] = {"tokens": [], "ts": 0.0, "limit": 0}


class DiscoveryAgent:
    """
    Agent-first discovery:
      1) pull trending/new from Birdeye (with your API key)
      2) enrich via RPC (decimals/account existence)
      3) return a sane, non-empty list
    """

    def __init__(self) -> None:
        # Hard-code Helius defaults (you asked for this)
        self.rpc_url = os.getenv(
            "SOLANA_RPC_URL",
            "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
        )
        self.ws_url = os.getenv(
            "SOLANA_WS_URL",
            "wss://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d",
        )
        self.birdeye_api_key = os.getenv(
            "BIRDEYE_API_KEY",
            "b1e60d72780940d1bd929b9b2e9225e6",
        )
        if not self.birdeye_api_key:
            logger.warning("BIRDEYE_API_KEY missing; discovery will fall back to static tokens")
        self.limit = int(os.getenv("DISCOVERY_LIMIT", "60") or 60)

    async def discover_tokens(
        self,
        *,
        offline: bool = False,
        token_file: Optional[str] = None,
        method: Optional[str] = None,
    ) -> List[str]:
        ttl = float(os.getenv("DISCOVERY_CACHE_TTL", "20") or 20.0)
        now = time.time()
        cached = list(_CACHE.get("tokens", [])) if isinstance(_CACHE.get("tokens"), list) else []
        cache_limit = int(_CACHE.get("limit", 0))
        if ttl > 0 and cached and now - float(_CACHE.get("ts", 0.0)) < ttl:
            if cache_limit and cache_limit >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (ttl=%s)", ttl)
                return cached[: self.limit]
            if len(cached) >= self.limit:
                logger.debug("DiscoveryAgent: returning cached tokens (limit=%s)", self.limit)
                return cached[: self.limit]
        # File mode (offline)
        if offline and token_file:
            try:
                with open(token_file, "r", encoding="utf-8") as fh:
                    rows = [ln.strip() for ln in fh if ln.strip()]
                rows = rows[: self.limit]
                publish("runtime.log", RuntimeLog(stage="discovery", detail=f"loaded {len(rows)} from file"))
                return rows
            except Exception as exc:
                logger.warning("Failed to read token_file %s: %s", token_file, exc)

        # Online: Birdeye + RPC enrichment
        mints = await scan_tokens_async(
            rpc_url=self.rpc_url,
            limit=self.limit,
            enrich=True,                  # extra flag kept for compat; enrichment is below
            api_key=self.birdeye_api_key, # matches token_scanner signature
        )
        try:
            mints = await enrich_tokens_async(mints, rpc_url=self.rpc_url)
        except Exception as exc:
            logger.warning("enrich_tokens_async failed: %s", exc)

        mints = [m for m in mints if isinstance(m, str) and len(m) > 10]
        publish("runtime.log", RuntimeLog(stage="discovery", detail=f"yield={len(mints)}"))
        logger.info("DiscoveryAgent pulled %d tokens (limit=%d, enrich=True)", len(mints), self.limit)
        if ttl > 0 and mints:
            _CACHE["tokens"] = list(mints)
            _CACHE["ts"] = now
            _CACHE["limit"] = self.limit
        return mints
