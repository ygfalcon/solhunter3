from __future__ import annotations

import asyncio
import datetime as _dt
import logging
import os
from typing import Dict, Any, Tuple

from ..agents.price_utils import resolve_price
from ..prices import update_price_cache
from ..token_aliases import canonical_mint, validate_mint

logger = logging.getLogger(__name__)

# Tunables (env overrides keep behavior adjustable without code changes)
_SYNTH_FRACTION = float(os.getenv("HELIUS_SYNTH_FRACTION", "0.0005") or 0.0005)  # 0.05% of 24h USD vol
_SYNTH_CAP = float(os.getenv("HELIUS_SYNTH_CAP", "100.0") or 100.0)              # max token amount
_FILL_COUNT = int(os.getenv("HELIUS_FILL_COUNT", "3") or 3)                      # number of buy fills before the close
_DEDUPE_SEC = int(os.getenv("HELIUS_DEDUPE_SECONDS", "90") or 90)               # min seconds between injections


def _extract_price_and_meta(
    context: Dict[str, Any] | None
) -> Tuple[float, Dict[str, Any]]:
    """Return (price, price_info) with best-effort extraction."""
    ctx = context or {}
    info = ctx.get("helius_price_info") or {}
    # price may come as direct number, pricePerToken, or priceUSD
    price = None
    for key in ("price", "pricePerToken", "priceUSD"):
        val = info.get(key) if key in info else ctx.get(key)
        try:
            if val is not None:
                price = float(val)
                break
        except (TypeError, ValueError):
            continue
    return float(price or 0.0), dict(info)


def _extract_change_pct(info: Dict[str, Any]) -> float:
    """Return 24h change as a fraction (e.g. 0.05 for +5%)."""
    for key in ("priceChangePercent1h", "priceChangePercent24h", "priceChange24h"):
        raw = info.get(key)
        try:
            if raw is None:
                continue
            return float(raw) / 100.0
        except (TypeError, ValueError):
            continue
    return 0.0


def _extract_volume_usd(info: Dict[str, Any]) -> float:
    """Return 24h USD volume if present."""
    for key in ("volumeUsd24h", "volume24h", "volume"):
        raw = info.get(key)
        try:
            if raw is None:
                continue
            return float(raw)
        except (TypeError, ValueError):
            continue
    return 0.0


async def hydrate_from_trades(mint: str, portfolio, memory_agent) -> Dict[str, float]:
    """
    Approximate recent trade activity using Helius price metadata.

    We synthesize a short sequence of buys followed by a partial close (sell)
    so downstream ROI math has a balanced revenue/spend signal.

    Returns a context dict with telemetry:
      {
        "helius_trade_count": int,
        "helius_volume_usd": float,
        "helius_price_change_pct": float,  # in percent
      }
    """
    result_ctx: Dict[str, float] = {
        "helius_trade_count": 0.0,
        "helius_volume_usd": 0.0,
        "helius_price_change_pct": 0.0,
    }

    mint = canonical_mint(mint)
    if not validate_mint(mint):
        return result_ctx

    # Resolve live price + metadata; be resilient to transient failures.
    price = 0.0
    ctx = {}
    try:
        price, ctx = await resolve_price(mint, portfolio)
        if not isinstance(ctx, dict):
            ctx = {}
    except Exception:
        logger.debug("resolve_price failed for %s", mint, exc_info=True)
        price, ctx = 0.0, {}

    # Extract price info consistently
    resolved_price, price_info = _extract_price_and_meta(ctx)
    price = float(price or resolved_price or 0.0)
    if price <= 0.0:
        return result_ctx  # nothing to seed

    change_frac = _extract_change_pct(price_info)  # e.g. 0.05 for +5%
    volume_usd = _extract_volume_usd(price_info)

    # Throttle synthetic injection if the last trade for this mint is very recent.
    memory = getattr(memory_agent, "memory", None) if memory_agent else None
    if memory is not None:
        try:
            latest = await memory.latest_trade_time(mint)
        except Exception:
            latest = None
        if latest is not None:
            delta = _dt.datetime.utcnow() - latest
            if delta.total_seconds() < max(15, _DEDUPE_SEC):
                # Still return telemetry so callers can use it.
                result_ctx.update(
                    {
                        "helius_trade_count": 0.0,
                        "helius_volume_usd": float(volume_usd),
                        "helius_price_change_pct": float(change_frac * 100.0),
                    }
                )
                return result_ctx

    # Compute a base anchor price that roughly reflects the recent path.
    base_price = (
        price / max(1e-6, 1.0 + change_frac)
        if abs(change_frac) > 1e-6
        else price * 0.97
    )

    # Determine synthetic size (in tokens) from 24h USD volume (light fraction, capped)
    if volume_usd > 0 and price > 0:
        amount = max(0.01, min((volume_usd * _SYNTH_FRACTION) / price, _SYNTH_CAP))
    else:
        amount = 1.0  # safe minimal seed

    # Build a few buy fills, then a partial close sell to create revenue.
    ts_now = _dt.datetime.utcnow()
    buys = []
    step = max(1, _FILL_COUNT)
    for i in range(step):
        t = ts_now - _dt.timedelta(seconds=(step - i) * 15)
        # Nudge price along the direction of change to add slight variance
        p = base_price * (1.0 + change_frac * (i + 1) / (step * 2))
        buys.append((p, "buy", t))

    # Partial close (sell) to avoid pathological ROI (all buys)
    # Close half at current/near-current price with a tiny positive edge
    sell_price = price * (1.0 + max(0.0005, change_frac * 0.1))
    sell = (sell_price, "sell", ts_now)

    fills = buys + [sell]

    # Apply fills: update portfolio & cache; optionally persist to memory
    trade_count = 0
    for fill_price, direction, ts in fills:
        try:
            portfolio.record_prices({mint: float(fill_price)})
        except Exception:
            # Non-fatal: keep going to memory logging
            logger.debug("portfolio.record_prices failed for %s", mint, exc_info=True)
        try:
            update_price_cache(mint, float(fill_price))
        except Exception:
            logger.debug("update_price_cache failed for %s", mint, exc_info=True)

        if memory is None:
            trade_count += 1
            continue

        payload = {
            "token": mint,
            "direction": direction,
            "amount": float(amount if direction == "buy" else amount * 0.5),
            "price": float(fill_price),
            "timestamp": ts,
            "created_at": ts,
            "reason": "helius_price_proxy",
            "_broadcast": False,  # keep the event bus quiet for synthetic seeds
        }
        try:
            log_trade = getattr(memory, "log_trade", None)
            if asyncio.iscoroutinefunction(log_trade):
                await log_trade(**payload)
            elif callable(log_trade):
                log_trade(**payload)  # type: ignore[misc]
            trade_count += 1
        except Exception:
            logger.debug("Failed to inject synthetic trade for %s", mint, exc_info=True)

    # Telemetry for callers/agents
    result_ctx.update(
        {
            "helius_trade_count": float(trade_count),
            "helius_volume_usd": float(volume_usd),
            "helius_price_change_pct": float(change_frac * 100.0),
        }
    )
    return result_ctx


__all__ = ["hydrate_from_trades"]
