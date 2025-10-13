"""Shared helpers for resolving token prices for agents."""

from __future__ import annotations

from typing import Dict, Any

import aiohttp
import os
from urllib.parse import parse_qs, urlparse

import logging

from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async, get_cached_price
from ..token_aliases import canonical_mint

_HELIUS_BASE_URL = os.getenv("HELIUS_PRICE_BASE_URL", "https://api.helius.xyz")
_HELIUS_PRICE_PATH = os.getenv("HELIUS_PRICE_METADATA_PATH", "/v0/token-metadata")
_HELIUS_TIMEOUT = float(os.getenv("HELIUS_PRICE_TIMEOUT", "2.5"))

logger = logging.getLogger(__name__)


async def resolve_price(token: str, portfolio: Portfolio) -> tuple[float, Dict[str, Any]]:
    """Return the best available USD price for ``token`` and debug context.

    Agents frequently need to evaluate a position using the most recent price
    available.  The live quote fetch can legitimately fail when upstream REST
    or websocket providers are unreachable, credentials such as the Birdeye API
    key are missing, or a token simply is not listed by any of the configured
    feeds (Helius, Birdeye, Dexscreener).  This helper consolidates the common
    fallbacks used across the codebase so agents can continue operating in
    those scenarios:

    1.  The latest entry in the portfolio's recorded price history.
    2.  A cached quote from :mod:`solhunter_zero.prices`.
    3.  A live fetch via :func:`fetch_token_prices_async` when other sources are
        unavailable.

    The returned tuple contains the resolved price (``0.0`` when unavailable)
    and a context dictionary with the intermediate values for observability.
    """

    token = canonical_mint(token)
    context: Dict[str, Any] = {}
    price = 0.0
    source: str | None = None

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
                source = "history"
                context["source"] = source

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
                    source = "cache"
                    context["source"] = source

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
            source = "fetch"
            context["source"] = source

    if price <= 0:
        helius_price, helius_context = await _fetch_helius_price(token)
        context.update(helius_context)
        if helius_price > 0:
            price = helius_price
            source = "helius"
            context["source"] = source

    return price, context


def _helius_api_key() -> str | None:
    explicit = os.getenv("HELIUS_API_KEY")
    if explicit and explicit.strip():
        return explicit.strip()
    rpc_url = os.getenv("SOLANA_RPC_URL")
    if not rpc_url:
        return None
    try:
        parsed = urlparse(rpc_url)
    except Exception:
        return None
    query = parse_qs(parsed.query)
    for key in ("api-key", "apiKey"):
        values = query.get(key)
        if values:
            return values[0]
    return None


async def _fetch_helius_price(token: str) -> tuple[float, Dict[str, Any]]:
    api_key = _helius_api_key()
    details: Dict[str, Any] = {}
    if not api_key:
        details["helius_error"] = "missing_api_key"
        return 0.0, details

    url = _HELIUS_BASE_URL.rstrip("/") + _HELIUS_PRICE_PATH
    params = {"api-key": api_key, "mintAccounts": token}
    timeout = aiohttp.ClientTimeout(total=_HELIUS_TIMEOUT)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as resp:
                resp.raise_for_status()
                payload = await resp.json()
    except Exception as exc:  # pragma: no cover - network failures best effort
        details["helius_error"] = str(exc)
        return 0.0, details

    price = 0.0
    if isinstance(payload, list) and payload:
        first = payload[0]
        if isinstance(first, dict):
            price_info = first.get("priceInfo") or {}
            value = price_info.get("pricePerToken") or price_info.get("priceUSD")
            try:
                price = float(value)
            except (TypeError, ValueError):
                price = 0.0
            details["helius_price_info"] = price_info
    elif isinstance(payload, dict):
        price_info = payload.get("priceInfo") or {}
        value = price_info.get("pricePerToken") or price_info.get("priceUSD")
        try:
            price = float(value)
        except (TypeError, ValueError):
            price = 0.0
        details["helius_price_info"] = price_info
    else:
        details["helius_error"] = "unexpected_payload"

    details["helius_price"] = price
    return price, details


__all__ = ["resolve_price"]
