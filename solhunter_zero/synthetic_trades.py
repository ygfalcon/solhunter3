from __future__ import annotations

import asyncio
import datetime
import os
import random
from typing import Any, Dict

from .agents.price_utils import resolve_price
from .prices import update_price_cache
from .token_aliases import canonical_mint

def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


_SYNTH_DEDUPE_SECONDS = max(0, _env_int("SYNTH_DEDUPE_SECONDS", 180))
_SYNTH_DEDUPE_SAMPLE_SIZE = max(1, _env_int("SYNTH_DEDUPE_SAMPLE_SIZE", 5))
_SYNTH_USD_BUDGET = max(0.0, _env_float("SYNTH_USD_BUDGET", 25.0))
_SYNTH_MAX_AMOUNT = max(0.0, _env_float("SYNTH_MAX_AMOUNT", 100.0))
_SYNTH_MIN_AMOUNT = max(0.0, _env_float("SYNTH_MIN_AMOUNT", 0.01))
_SYNTH_PRICE_FLOOR = max(0.0, _env_float("SYNTH_PRICE_FLOOR", 0.0001))
_SYNTH_PCT_RANGE = max(0.0, _env_float("SYNTH_PCT_RANGE", 0.03))
_SYNTH_JITTER_BPS = max(0.0, _env_float("SYNTH_JITTER_BPS", 50.0))
_SYNTH_STEP_SECONDS = max(1.0, _env_float("SYNTH_STEP_SECONDS", 20.0))

_SYNTHETIC_REASONS = {"synthetic", "helius_price_proxy"}


def _update_with_price_context(base: Dict[str, Any], price_context: Any) -> None:
    if isinstance(price_context, dict):
        for key, value in price_context.items():
            if key not in base:
                base[key] = value
    elif price_context is not None:
        base.setdefault("price_context", price_context)


async def ensure_synthetic_baseline(token: str, portfolio, memory_agent, *, trades: int = 3) -> Dict[str, Any]:
    """
    Ensure ``token`` has recent price/trade context by inserting synthetic fills.

    - Uses resolve_price() (with Helius fallback) to get a base USD price.
    - If the portfolio already has history and memory has trades in the last 5 minutes, it returns.
    - Otherwise logs a small sequence of alternating buy/sell trades via the memory agent,
      records prices into the portfolio, and updates the price cache.

    Returns the pricing context used (useful for logging/diagnostics).
    """

    canonical_token = canonical_mint(token)

    result_context: Dict[str, Any] = {
        "synthetic_trade_count": 0,
        "synthetic_window_seconds": _SYNTH_DEDUPE_SECONDS,
        "synthetic_token": canonical_token,
    }

    history = portfolio.price_history.get(canonical_token, [])
    if history and len(history) >= max(1, trades):
        result_context["skipped_reason"] = "portfolio_history"
        return result_context

    memory = getattr(memory_agent, "memory", None) if memory_agent else None
    now = datetime.datetime.utcnow()

    if memory is not None and _SYNTH_DEDUPE_SECONDS > 0:
        list_trades_fn = getattr(memory, "list_trades", None)
        recent_reason = None
        recent_timestamp = None
        if callable(list_trades_fn):
            try:
                maybe_trades = list_trades_fn(
                    token=canonical_token,
                    limit=max(1, _SYNTH_DEDUPE_SAMPLE_SIZE),
                )
                trades_result = (
                    await maybe_trades
                    if asyncio.iscoroutine(maybe_trades)
                    else maybe_trades
                )
            except Exception:
                trades_result = []
        else:
            trades_result = []

        for trade in reversed(trades_result or []):
            reason = getattr(trade, "reason", None)
            created_at = getattr(trade, "created_at", None) or getattr(trade, "timestamp", None)
            if isinstance(trade, dict):
                reason = trade.get("reason", reason)
                created_at = trade.get("created_at") or trade.get("timestamp") or created_at
            if reason in _SYNTHETIC_REASONS and isinstance(created_at, datetime.datetime):
                delta = now - created_at
                if delta.total_seconds() < _SYNTH_DEDUPE_SECONDS:
                    recent_reason = reason
                    recent_timestamp = created_at
                    break

        if recent_timestamp is not None:
            result_context["skipped_reason"] = "recent_synthetic_trade"
            result_context["last_synthetic_reason"] = recent_reason
            result_context["last_synthetic_at"] = recent_timestamp.isoformat()
            return result_context

    try:
        price, price_context = await resolve_price(canonical_token, portfolio)
    except Exception as exc:
        price = 0.0
        price_context = {
            "resolve_error": str(exc),
            "resolve_error_type": exc.__class__.__name__,
        }

    _update_with_price_context(result_context, price_context)
    result_context["price_resolved"] = bool(price and price > 0)
    result_context["synthetic_base_price"] = price

    if not price or price <= 0:
        result_context["skipped_reason"] = "price_unavailable"
        return result_context

    try:
        update_price_cache(canonical_token, price)
    except Exception:
        result_context.setdefault("cache_update_errors", 0)
        result_context["cache_update_errors"] += 1

    # Build a few synthetic fills around the base price
    trades_to_create = max(1, trades)
    steps = max(1, trades_to_create - 1)
    jitter_range = abs(_SYNTH_JITTER_BPS) / 10_000

    trades_attempted = 0
    trades_logged = 0
    log_failures = 0
    record_failures = 0
    cache_failures = result_context.pop("cache_update_errors", 0)

    for idx in range(trades_to_create):
        pct = (idx - steps / 2) / max(1, steps) * _SYNTH_PCT_RANGE
        jitter = random.uniform(-jitter_range, jitter_range)
        trade_price = price * (1 + pct + jitter)
        trade_price = max(_SYNTH_PRICE_FLOOR, trade_price)
        amount = _SYNTH_USD_BUDGET / trade_price if trade_price > 0 else _SYNTH_MAX_AMOUNT
        amount = max(_SYNTH_MIN_AMOUNT, min(_SYNTH_MAX_AMOUNT, amount))
        side = "buy" if idx % 2 == 0 else "sell"
        ts = now - datetime.timedelta(seconds=idx * _SYNTH_STEP_SECONDS)

        trades_attempted += 1

        try:
            portfolio.record_prices({canonical_token: trade_price})
        except Exception:
            record_failures += 1

        try:
            update_price_cache(canonical_token, trade_price)
        except Exception:
            cache_failures += 1

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
                    trades_logged += 1
                elif callable(log_trade):
                    result = log_trade(**payload)  # type: ignore[misc]
                    if asyncio.iscoroutine(result):
                        await result
                    trades_logged += 1
            except Exception:
                log_failures += 1

    result_context["synthetic_trade_count"] = trades_attempted
    result_context["synthetic_trades_logged"] = trades_logged
    if log_failures:
        result_context["synthetic_log_failures"] = log_failures
    if record_failures:
        result_context["price_record_errors"] = record_failures
    if cache_failures:
        result_context["cache_update_errors"] = cache_failures

    return result_context


__all__ = ["ensure_synthetic_baseline"]
