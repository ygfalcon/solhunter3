import os
import logging
import aiohttp
import asyncio

from typing import Iterable, Dict, Any, Sequence

from solhunter_zero.lru import TTLCache
from .http import get_session
from .async_utils import run_async

logger = logging.getLogger(__name__)

HELIUS_PRICE_URL = os.getenv("HELIUS_PRICE_URL", "https://api.helius.xyz/v0/price")
HELIUS_API_KEY = os.getenv(
    "HELIUS_API_KEY", "af30888b-b79f-4b12-b3fd-c5375d5bad2d"
)

BIRDEYE_PRICE_URL = os.getenv("BIRDEYE_PRICE_URL", "https://public-api.birdeye.so")
DEXSCREENER_PRICE_URL = os.getenv(
    "DEXSCREENER_PRICE_URL", "https://api.dexscreener.com/latest/dex/tokens"
)

PRICE_RETRY_ATTEMPTS = max(1, int(os.getenv("PRICE_RETRY_ATTEMPTS", "3") or 3))
PRICE_RETRY_BACKOFF = float(os.getenv("PRICE_RETRY_BACKOFF", "0.5") or 0.5)

# module level price cache
PRICE_CACHE_TTL = float(os.getenv("PRICE_CACHE_TTL", "30") or 30)
PRICE_CACHE = TTLCache(maxsize=256, ttl=PRICE_CACHE_TTL)


def _tokens_key(tokens: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted(set(tokens)))


def get_cached_price(token: str) -> float | None:
    """Return cached price for ``token`` if available."""
    return PRICE_CACHE.get(token)


def update_price_cache(token: str, price: float) -> None:
    """Store ``price`` in the module cache."""
    if isinstance(price, (int, float)):
        PRICE_CACHE.set(token, float(price))


def _extract_price(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if isinstance(value, dict):
        for key in ("price", "value", "priceUsd", "usd", "price_usd"):
            price = value.get(key)
            if isinstance(price, (int, float)):
                return float(price)
            if isinstance(price, str):
                try:
                    return float(price)
                except (TypeError, ValueError):
                    continue
        # Some providers wrap the value in another dict layer
        if "data" in value and isinstance(value["data"], dict):
            return _extract_price(value["data"])
    return None


async def _request_json(
    session: aiohttp.ClientSession,
    url: str,
    provider: str,
    *,
    params: Dict[str, Any] | None = None,
    headers: Dict[str, str] | None = None,
    json: Any | None = None,
    method: str = "GET",
) -> Any:
    for attempt in range(PRICE_RETRY_ATTEMPTS):
        try:
            async with session.request(
                method,
                url,
                params=params,
                headers=headers,
                json=json,
                timeout=10,
            ) as resp:
                resp.raise_for_status()
                return await resp.json()
        except aiohttp.ClientError as exc:
            if attempt == PRICE_RETRY_ATTEMPTS - 1:
                logger.warning("Failed to fetch prices from %s: %s", provider, exc)
            else:
                logger.debug(
                    "%s price fetch attempt %d failed: %s",
                    provider,
                    attempt + 1,
                    exc,
                )
                await asyncio.sleep(PRICE_RETRY_BACKOFF * (2 ** attempt))
        except Exception as exc:  # pragma: no cover - safety net
            if attempt == PRICE_RETRY_ATTEMPTS - 1:
                logger.warning("Failed to fetch prices from %s: %s", provider, exc)
            else:
                logger.debug(
                    "%s price fetch attempt %d failed: %s",
                    provider,
                    attempt + 1,
                    exc,
                )
                await asyncio.sleep(PRICE_RETRY_BACKOFF * (2 ** attempt))
    return None


async def _fetch_prices_helius(
    session: aiohttp.ClientSession, token_list: Sequence[str]
) -> Dict[str, float]:
    if not token_list:
        return {}

    params: Dict[str, Any] | None = None
    if HELIUS_API_KEY:
        params = {"api-key": HELIUS_API_KEY}

    payload = await _request_json(
        session,
        HELIUS_PRICE_URL,
        "Helius",
        params=params,
        json={"ids": list(token_list)},
        method="POST",
    )
    if not isinstance(payload, dict):
        return {}

    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

    prices: Dict[str, float] = {}
    if isinstance(data, dict):
        for token in token_list:
            entry = data.get(token)
            price = _extract_price(entry)
            if price is not None:
                prices[token] = price
                update_price_cache(token, price)

    return prices


async def _fetch_prices_birdeye(
    session: aiohttp.ClientSession, token_list: Sequence[str]
) -> Dict[str, float]:
    if not token_list:
        return {}

    api_key = os.getenv("BIRDEYE_API_KEY", "")
    if not api_key:
        return {}

    url = f"{BIRDEYE_PRICE_URL.rstrip('/')}/defi/price"
    headers = {"X-API-KEY": api_key}
    prices: Dict[str, float] = {}

    for token in token_list:
        payload = await _request_json(
            session,
            url,
            "Birdeye",
            params={"address": token},
            headers=headers,
        )
        if not isinstance(payload, dict):
            continue

        price = None
        data = payload.get("data")
        if isinstance(data, dict):
            price = _extract_price(data)
            if price is None:
                price = _extract_price(data.get("value"))
        if price is None:
            price = _extract_price(payload)

        if price is not None:
            prices[token] = price
            update_price_cache(token, price)

    return prices


async def _fetch_prices_dexscreener(
    session: aiohttp.ClientSession, token_list: Sequence[str]
) -> Dict[str, float]:
    if not token_list:
        return {}

    prices: Dict[str, float] = {}

    for token in token_list:
        url = f"{DEXSCREENER_PRICE_URL.rstrip('/')}/{token}"
        payload = await _request_json(session, url, "Dexscreener")
        if not isinstance(payload, dict):
            continue

        value = None
        pairs = payload.get("pairs")
        if isinstance(pairs, list):
            for pair in pairs:
                value = _extract_price(pair.get("priceUsd"))
                if value is None and isinstance(pair, dict):
                    value = _extract_price(pair.get("price"))
                if value is not None:
                    break
        if value is None:
            value = _extract_price(payload)

        if value is not None:
            prices[token] = value
            update_price_cache(token, value)

    return prices


async def _fetch_prices(token_list: Iterable[str]) -> Dict[str, float]:
    tokens = list(token_list)
    if not tokens:
        return {}

    session = await get_session()

    resolved: Dict[str, float] = {}
    missing = list(tokens)

    providers = (
        ("Helius", _fetch_prices_helius),
        ("Birdeye", _fetch_prices_birdeye),
        ("Dexscreener", _fetch_prices_dexscreener),
    )

    for name, provider in providers:
        if not missing:
            break
        try:
            fetched = await provider(session, tuple(missing))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to fetch prices from %s: %s", name, exc)
            fetched = {}
        if fetched:
            for token, price in fetched.items():
                resolved[token] = price
            missing = [tok for tok in missing if tok not in fetched]

    if missing:
        default = os.getenv("OFFLINE_PRICE_DEFAULT")
        if default is not None:
            try:
                val = float(default)
                for token in missing:
                    resolved[token] = val
                    update_price_cache(token, val)
            except Exception:
                pass

    return resolved


def fetch_token_prices(tokens: Iterable[str]) -> Dict[str, float]:
    """Retrieve USD prices for multiple tokens from the configured API."""
    return run_async(lambda: fetch_token_prices_async(tokens))


def warm_cache(tokens: Iterable[str]) -> None:
    """Fetch prices for ``tokens`` and populate ``PRICE_CACHE``.

    This is a convenience wrapper around :func:`fetch_token_prices_async` that
    runs the asynchronous fetcher and discards the return value.  It is useful
    when callers only need the cache to be primed without immediately using the
    fetched prices.
    """
    run_async(lambda: fetch_token_prices_async(tokens))


async def fetch_token_prices_async(tokens: Iterable[str]) -> Dict[str, float]:
    """Asynchronously retrieve USD prices for multiple tokens."""
    token_list = _tokens_key(tokens)
    if not token_list:
        return {}

    result: Dict[str, float] = {}
    missing: list[str] = []
    for tok in token_list:
        val = get_cached_price(tok)
        if val is not None:
            result[tok] = val
        else:
            missing.append(tok)

    if missing:
        fetched = await _fetch_prices(missing)
        for t, v in fetched.items():
            update_price_cache(t, v)
            result[t] = v

    return result
