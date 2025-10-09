from __future__ import annotations

import asyncio
import datetime as _dt
import logging
from typing import Dict

from ..agents.price_utils import resolve_price
from ..prices import update_price_cache
from ..token_aliases import canonical_mint

logger = logging.getLogger(__name__)


async def hydrate_from_trades(mint: str, portfolio, memory_agent) -> Dict[str, float]:
    """Approximate recent trade activity using Helius price metadata.

    The Helius REST API does not expose the older ``/v1/dex/trades`` endpoint
    any more.  Instead of attempting to replay full transactions we use the
    live price metadata to seed synthetic fills so agents have recent ROI and
    volatility context.  This keeps the pipeline responsive while still
    reflecting Helius pricing.
    """

    mint = canonical_mint(mint)
    price, context = await resolve_price(mint, portfolio)
    price_info = context.get("helius_price_info") or {}

    memory = getattr(memory_agent, "memory", None) if memory_agent else None
    if memory is not None:
        try:
            latest = await memory.latest_trade_time(mint)
        except Exception:
            latest = None
        else:
            if latest is not None:
                delta = _dt.datetime.utcnow() - latest
                if delta.total_seconds() < 90:
                    return context

    price = float(
        price
        or price_info.get("pricePerToken")
        or price_info.get("priceUSD")
        or 0.0
    )
    if price <= 0:
        return context

    change_pct = 0.0
    for key in ("priceChangePercent1h", "priceChangePercent24h", "priceChange24h"):
        raw = price_info.get(key)
        try:
            if raw is None:
                continue
            change_pct = float(raw)
            break
        except (TypeError, ValueError):
            continue
    change_pct /= 100.0

    volume_usd = 0.0
    for key in ("volumeUsd24h", "volume24h", "volume"):
        raw = price_info.get(key)
        try:
            if raw is None:
                continue
            volume_usd = float(raw)
            break
        except (TypeError, ValueError):
            continue

    base_price = price / max(1e-6, 1.0 + change_pct) if abs(change_pct) > 1e-6 else price * 0.97
    amount = 1.0
    if volume_usd > 0 and price > 0:
        amount = max(0.01, min(volume_usd / price * 0.05, 100.0))

    ts_now = _dt.datetime.utcnow()
    fills = (
        (base_price, "buy", ts_now - _dt.timedelta(seconds=45)),
        (price, "buy", ts_now - _dt.timedelta(seconds=20)),
        (price * (1.0 + change_pct * 0.2), "buy", ts_now),
    )

    for fill_price, direction, ts in fills:
        portfolio.record_prices({mint: fill_price})
        update_price_cache(mint, fill_price)
        if memory is None:
            continue
        payload = {
            "token": mint,
            "direction": direction,
            "amount": amount,
            "price": float(fill_price),
            "timestamp": ts,
            "created_at": ts,
            "reason": "helius_price_proxy",
            "_broadcast": False,
        }
        try:
            log_trade = getattr(memory, "log_trade", None)
            if asyncio.iscoroutinefunction(log_trade):
                await log_trade(**payload)
            elif callable(log_trade):
                log_trade(**payload)  # type: ignore[misc]
        except Exception:
            logger.debug("Failed to inject synthetic trade for %s", mint, exc_info=True)

    context["helius_trade_count"] = len(fills)
    context["helius_volume_usd"] = volume_usd
    context["helius_price_change_pct"] = change_pct * 100
    return context


__all__ = ["hydrate_from_trades"]
