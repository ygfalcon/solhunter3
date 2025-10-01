from __future__ import annotations

import asyncio
import logging
import os
from typing import Iterable, AsyncGenerator

from .mempool_scanner import stream_mempool_tokens
from .onchain_metrics import fetch_liquidity_onchain, fetch_token_age
from .agent_manager import AgentManager
from .portfolio import Portfolio
from .event_bus import publish

logger = logging.getLogger(__name__)


async def listen_mempool(
    rpc_url: str,
    agent_manager: AgentManager,
    portfolio: Portfolio,
    *,
    blacklist: Iterable[str] | None = None,
    age_limit: float = 3600.0,
    min_liquidity: float = 0.0,
    risk_base: float = 0.05,
) -> AsyncGenerator[str, None]:
    """Yield validated tokens from the mempool and optionally execute buys."""

    if blacklist is None:
        env = os.getenv("TOKEN_BLACKLIST", "")
        blacklist = [t.strip().lower() for t in env.split(",") if t.strip()]
    bl_set = {t.lower() for t in blacklist}

    async for token in stream_mempool_tokens(rpc_url):
        if token.lower() in bl_set:
            continue
        age = await asyncio.to_thread(fetch_token_age, token, rpc_url)
        if age > age_limit:
            continue
        liq = await asyncio.to_thread(fetch_liquidity_onchain, token, rpc_url)
        if liq < min_liquidity:
            continue
        risk = risk_base
        if min_liquidity:
            risk *= max(1.0, liq / min_liquidity)
        os.environ["RISK_MULTIPLIER"] = str(risk)
        publish("risk_updated", {"multiplier": risk})
        try:
            await agent_manager.execute(token, portfolio)
        except Exception as exc:  # pragma: no cover - agent errors
            logger.warning("Agent execution failed for %s: %s", token, exc)
            continue
        yield token
