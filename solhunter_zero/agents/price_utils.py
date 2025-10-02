"""Shared helpers for resolving token prices for agents."""

from __future__ import annotations

from typing import Dict, Any

import logging

from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async, get_cached_price

logger = logging.getLogger(__name__)


async def resolve_price(token: str, portfolio: Portfolio) -> tuple[float, Dict[str, Any]]:
    """Return the best available USD price for ``token`` and debug context.

    Agents frequently need to evaluate a position using the most recent price
    available.  This helper consolidates the common fallbacks used across the
    codebase:

    1.  The latest entry in the portfolio's recorded price history.
    2.  A cached quote from :mod:`solhunter_zero.prices`.
    3.  A live fetch via :func:`fetch_token_prices_async` when other sources are
        unavailable.

    The returned tuple contains the resolved price (``0.0`` when unavailable)
    and a context dictionary with the intermediate values for observability.
    """

    context: Dict[str, Any] = {}
    price = 0.0

    history = portfolio.price_history.get(token, [])
    if history:
        try:
            hist_price = float(history[-1])
        except Exception:
            hist_price = 0.0
        else:
            context["history_price"] = hist_price
            if hist_price > 0:
                price = hist_price

    if price <= 0:
        cached = get_cached_price(token)
        if cached is not None:
            try:
                cached_price = float(cached)
            except Exception:
                cached_price = 0.0
            else:
                context["cached_price"] = cached_price
                if cached_price > 0:
                    price = cached_price

    if price <= 0:
        fetched_price = 0.0
        try:
            prices = await fetch_token_prices_async({token})
        except Exception as exc:  # pragma: no cover - network/runtime failures
            context["fetch_error"] = str(exc)
            logger.debug("price fetch failed for token %%s", token, exc_info=True)
        else:
            fetched_price = float(prices.get(token, 0.0) or 0.0)
            context["fetched_price"] = fetched_price
        if fetched_price > 0:
            price = fetched_price

    return price, context


__all__ = ["resolve_price"]

