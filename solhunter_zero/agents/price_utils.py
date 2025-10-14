"""Shared helpers for resolving token prices for agents."""

from __future__ import annotations

from typing import Dict, Any

import aiohttp
import os

import logging

from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async, get_cached_price
from ..token_aliases import canonical_mint, validate_mint
from ..runtime_settings import (
    RuntimeSettings,
    SettingsError,
    refresh_runtime_settings,
    runtime_settings,
)

logger = logging.getLogger(__name__)


def _settings() -> RuntimeSettings:
    loader = refresh_runtime_settings if os.getenv("PYTEST_CURRENT_TEST") else runtime_settings
    try:
        return loader()
    except SettingsError:
        if os.getenv("PYTEST_CURRENT_TEST") is not None:
            os.environ.setdefault(
                "HELIUS_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=test"
            )
            os.environ.setdefault(
                "HELIUS_WS_URL", "wss://mainnet.helius-rpc.com/?api-key=test"
            )
            os.environ.setdefault("HELIUS_API_KEY", "test-helius-key")
            os.environ.setdefault("HELIUS_API_KEYS", "test-helius-key")
            return refresh_runtime_settings()
        return refresh_runtime_settings()


async def resolve_price(token: str, portfolio: Portfolio) -> tuple[float, Dict[str, Any]]:
    """Return the best available USD price for ``token`` and debug context.

    Agents frequently need to evaluate a position using the most recent price
    available.  The live quote fetch can legitimately fail when upstream REST
    or websocket providers are unreachable, credentials such as the Birdeye API
    key are missing, or a token simply is not listed by any of the configured
    feeds (Helius, Birdeye, Jupiter, Pyth).  This helper consolidates the common
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
    if not validate_mint(token):
        return 0.0, {"error": "invalid_mint"}
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
    try:
        return _settings().helius_api_key
    except SettingsError:
        return None


async def _fetch_helius_price(token: str) -> tuple[float, Dict[str, Any]]:
    api_key = _helius_api_key()
    details: Dict[str, Any] = {}
    if not api_key:
        details["helius_error"] = "missing_api_key"
        return 0.0, details

    settings = _settings()
    url = settings.helius_price_base_url.rstrip("/") + settings.helius_price_metadata_path
    params = {"api-key": api_key, "mintAccounts": token}
    timeout = aiohttp.ClientTimeout(total=settings.helius_price_timeout)
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
