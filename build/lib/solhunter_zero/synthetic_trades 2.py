from __future__ import annotations

import asyncio
import datetime
import random
from typing import Any, Dict

from .agents.price_utils import resolve_price
from .prices import update_price_cache
from .token_aliases import canonical_mint


async def ensure_synthetic_baseline(token: str, portfolio, memory_agent, *, trades: int = 3) -> Dict[str, Any]:
    """
    Ensure ``token`` has recent price/trade context by inserting synthetic fills.

    - Uses resolve_price() (with Helius fallback) to get a base USD price.
    - If the portfolio already has history and memory has trades in the last 5 minutes, it returns.
    - Otherwise logs a small sequence of alternating buy/sell trades via the memory agent,
      records prices into the portfolio, and updates the price cache.

    Returns the pricing context used (useful for logging/diagnostics).
    """

    # Skip when we already have some history
    canonical_token = canonical_mint(token)

    history = portfolio.price_history.get(canonical_token, [])
    if history and len(history) >= trades:
        return {}

    memory = getattr(memory_agent, "memory", None) if memory_agent else None
    if memory is not None:
        try:
            latest = await memory.latest_trade_time(canonical_token)
        except Exception:
            latest = None
        else:
            if latest is not None:
                delta = datetime.datetime.utcnow() - latest
                if delta.total_seconds() < 300:
                    return {}

    price, context = await resolve_price(canonical_token, portfolio)
    if price <= 0:
        return context

    update_price_cache(canonical_token, price)

    # Build a few synthetic fills around the base price
    now = datetime.datetime.utcnow()
    trades_to_create = max(1, trades)
    steps = max(1, trades_to_create - 1)
    for idx in range(trades_to_create):
        # Small +/-1.5% variation with some jitter
        pct = (idx - steps / 2) / max(1, steps) * 0.03
        jitter = random.uniform(-0.005, 0.005)
        trade_price = price * (1 + pct + jitter)
        trade_price = max(0.0001, trade_price)
        amount = max(0.01, 50.0 / trade_price)
        side = "buy" if idx % 2 == 0 else "sell"
        ts = now - datetime.timedelta(seconds=idx * 20)

        portfolio.record_prices({canonical_token: trade_price})
        update_price_cache(canonical_token, trade_price)

        if memory is not None:
            log_trade = getattr(memory, "log_trade", None)
            payload = {
                "token": canonical_token,
                "direction": side,
                "amount": float(amount),
                "price": float(trade_price),
                "reason": "synthetic",
                "timestamp": ts,
                "created_at": ts,
                "_broadcast": False,
            }
            try:
                if asyncio.iscoroutinefunction(log_trade):
                    await log_trade(**payload)
                elif callable(log_trade):
                    log_trade(**payload)  # type: ignore[misc]
            except Exception:
                # Best effort; continue seeding prices even if logging fails
                continue

    return context


__all__ = ["ensure_synthetic_baseline"]
