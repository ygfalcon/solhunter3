import os
import logging
import aiohttp
import asyncio

from typing import Iterable, Dict

from solhunter_zero.lru import TTLCache
from .http import get_session
from .async_utils import run_async

logger = logging.getLogger(__name__)

PRICE_API_BASE_URL = os.getenv("PRICE_API_URL", "https://price.jup.ag")
PRICE_API_PATH = "/v4/price"

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




async def _fetch_prices(token_list: Iterable[str]) -> Dict[str, float]:
    ids_list = list(token_list)
    ids = ",".join(ids_list)
    url = f"{PRICE_API_BASE_URL}{PRICE_API_PATH}?ids={ids}"

    session = await get_session()
    # retry with simple backoff to reduce transient failures and warning noise
    data = {}
    attempts = int(os.getenv("PRICE_RETRY_ATTEMPTS", "3") or 3)
    backoff = float(os.getenv("PRICE_RETRY_BACKOFF", "0.5") or 0.5)
    for i in range(max(1, attempts)):
        try:
            async with session.get(url, timeout=10) as resp:
                resp.raise_for_status()
                data = (await resp.json()).get("data", {})
            break
        except aiohttp.ClientError as exc:
            if i == attempts - 1:
                logger.warning("Failed to fetch token prices: %s", exc)
            else:
                logger.debug("Price fetch attempt %d failed: %s", i + 1, exc)
                await asyncio.sleep(backoff * (2 ** i))
                continue
        except Exception as exc:
            if i == attempts - 1:
                logger.warning("Price fetch failed: %s", exc)
            else:
                logger.debug("Price fetch attempt %d failed: %s", i + 1, exc)
                await asyncio.sleep(backoff * (2 ** i))
                continue
    if not data:
        # Secondary fallback: Coingecko token_price API when enabled
        fallback = os.getenv("PRICE_FALLBACK", "").lower()
        if fallback == "coingecko":
            try:
                base = os.getenv("COINGECKO_URL", "https://api.coingecko.com")
                cg_url = (
                    f"{base}/api/v3/simple/token_price/solana?contract_addresses="
                    + ",".join(ids_list)
                    + "&vs_currencies=usd"
                )
                async with session.get(cg_url, timeout=10) as resp2:
                    resp2.raise_for_status()
                    payload = await resp2.json()
                    prices = {}
                    for addr in ids_list:
                        info = payload.get(addr.lower()) or payload.get(addr)
                        if isinstance(info, dict):
                            usd = info.get("usd")
                            if isinstance(usd, (int, float)):
                                prices[addr] = float(usd)
                    if prices:
                        return prices
            except Exception as e:  # pragma: no cover - network fallback
                logger.warning("Coingecko fallback failed: %s", e)
        elif fallback == "quote_jup":
            try:
                # Derive USD prices using Jupiter quote API for known tokens.
                # USDC/USDT ~1.0; WSOL queried via quote to USDC.
                dex_base = (os.getenv("DEX_BASE_URL", "https://quote-api.jup.ag") or "").rstrip("/")
                usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9"
                usdt = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
                wsol = "So11111111111111111111111111111111111111112"
                out_mint = os.getenv("QUOTE_JUP_OUT_MINT", usdc)
                # Parse optional env QUOTE_JUP_TOKENS to add more tokens: JSON dict {mint:decimals}
                # or CSV "mint:decimals,..."
                token_decimals: Dict[str, int] = {}
                env_tokens = os.getenv("QUOTE_JUP_TOKENS")
                if env_tokens:
                    import json as _json
                    parsed = None
                    try:
                        parsed = _json.loads(env_tokens)
                    except Exception:
                        parsed = None
                    if isinstance(parsed, dict):
                        for k, v in parsed.items():
                            try:
                                token_decimals[str(k)] = int(v)
                            except Exception:
                                continue
                    else:
                        # CSV fallback
                        for entry in env_tokens.split(","):
                            entry = entry.strip()
                            if not entry or ":" not in entry:
                                continue
                            try:
                                m, d = entry.split(":", 1)
                                token_decimals[str(m.strip())] = int(d.strip())
                            except Exception:
                                continue
                prices: Dict[str, float] = {}
                for addr in ids_list:
                    if addr == usdc or addr == usdt:
                        prices[addr] = 1.0
                    elif addr == wsol:
                        # 1 SOL -> USDC; WSOL has 9 decimals; amount=1e9
                        quote_url = f"{dex_base}/v6/quote?inputMint={wsol}&outputMint={out_mint}&amount=1000000000&slippageBps=50"
                        async with session.get(quote_url, timeout=10) as resp3:
                            resp3.raise_for_status()
                            payload = await resp3.json()
                            out_amt = 0.0
                            try:
                                out_amt = float(payload.get("outAmount") or payload.get("data", {}).get("outAmount") or 0.0)
                            except Exception:
                                out_amt = 0.0
                            if out_amt > 0:
                                prices[addr] = out_amt / 1_000_000.0
                    elif addr in token_decimals:
                        dec = max(0, int(token_decimals.get(addr, 0)))
                        try:
                            amount_units = 10 ** dec
                        except Exception:
                            amount_units = 1
                        quote_url = f"{dex_base}/v6/quote?inputMint={addr}&outputMint={out_mint}&amount={amount_units}&slippageBps=50"
                        try:
                            async with session.get(quote_url, timeout=10) as resp4:
                                resp4.raise_for_status()
                                payload = await resp4.json()
                                out_amt = 0.0
                                try:
                                    out_amt = float(payload.get("outAmount") or payload.get("data", {}).get("outAmount") or 0.0)
                                except Exception:
                                    out_amt = 0.0
                                if out_amt > 0:
                                    prices[addr] = out_amt / 1_000_000.0
                        except Exception:
                            continue
                    # Others skipped; OFFLINE_PRICE_DEFAULT may handle them
                if prices:
                    return prices
            except Exception as e:  # pragma: no cover - network fallback
                logger.warning("Jupiter quote fallback failed: %s", e)
        # Optional offline fallback for environments without network/DNS
        default = os.getenv("OFFLINE_PRICE_DEFAULT")
        if default is not None:
            try:
                val = float(default)
                return {tok: val for tok in ids_list}
            except Exception:
                pass
        return {}

    prices: Dict[str, float] = {}
    for token, info in data.items():
        price = info.get("price")
        if isinstance(price, (int, float)):
            prices[token] = float(price)
    return prices


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
