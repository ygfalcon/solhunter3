import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, MutableMapping, Sequence, Tuple, Union

import aiohttp

from solhunter_zero.lru import TTLCache
from .async_utils import run_async
from .http import get_session

logger = logging.getLogger(__name__)

PRICE_RETRY_ATTEMPTS = max(1, int(os.getenv("PRICE_RETRY_ATTEMPTS", "3") or 3))
PRICE_RETRY_BACKOFF = float(os.getenv("PRICE_RETRY_BACKOFF", "0.5") or 0.5)
PRICE_CACHE_TTL = float(os.getenv("PRICE_CACHE_TTL", "30") or 30)

PRICE_CACHE: TTLCache = TTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
QUOTE_CACHE: TTLCache = TTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)

JUPITER_PRICE_URL = os.getenv("JUPITER_PRICE_URL", "https://price.jup.ag/v3/price")
JUPITER_BATCH_SIZE = max(1, int(os.getenv("JUPITER_BATCH_SIZE", "100") or 100))

DEXSCREENER_PRICE_URL = os.getenv(
    "DEXSCREENER_PRICE_URL", "https://api.dexscreener.com/latest/dex/tokens"
)

BIRDEYE_PRICE_URL = os.getenv("BIRDEYE_PRICE_URL", "https://public-api.birdeye.so")
BIRDEYE_CHAIN = os.getenv("BIRDEYE_CHAIN", "solana")
BIRDEYE_MAX_BATCH = max(1, int(os.getenv("BIRDEYE_BATCH_SIZE", "100") or 100))

PYTH_PRICE_URL = os.getenv("PYTH_PRICE_URL", "https://hermes.pyth.network/v2/updates/price/latest")

SYNTHETIC_HINTS_ENV = "SYNTHETIC_PRICE_HINTS"
OFFLINE_PRICE_DEFAULT = os.getenv("OFFLINE_PRICE_DEFAULT")

_LAST_BIRDEYE_KEY: str | None = None


def _monotonic() -> float:
    return time.monotonic()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _chunked(tokens: Sequence[str], size: int) -> List[Sequence[str]]:
    return [tokens[i : i + size] for i in range(0, len(tokens), size)]


@dataclass(slots=True)
class PriceQuote:
    price_usd: float
    source: str
    asof: int
    quality: str
    liquidity_hint: float | None = None


@dataclass(slots=True)
class ProviderHealth:
    name: str
    cooldown_until: float = 0.0
    consecutive_failures: int = 0
    last_status: int | None = None
    healthy: bool = True

    def in_cooldown(self) -> bool:
        return _monotonic() < self.cooldown_until

    def record_success(self) -> None:
        self.cooldown_until = 0.0
        self.consecutive_failures = 0
        self.last_status = None
        self.healthy = True

    def record_failure(self, status: int | None, *, cooldown: float | None = None) -> None:
        self.consecutive_failures += 1
        self.last_status = status
        self.healthy = False
        base = min(8.0, 0.5 * (2 ** (self.consecutive_failures - 1)))
        duration = cooldown if cooldown is not None else base
        self.cooldown_until = max(self.cooldown_until, _monotonic() + duration)

    def clear(self) -> None:
        self.cooldown_until = 0.0
        self.consecutive_failures = 0
        self.last_status = None
        self.healthy = True


@dataclass(slots=True)
class ProviderStats:
    name: str
    successes: int = 0
    failures: int = 0
    last_latency_ms: float | None = None
    last_error_status: int | None = None
    last_error: str | None = None

    def record_success(self, latency_ms: float) -> None:
        self.successes += 1
        self.last_latency_ms = latency_ms
        self.last_error_status = None
        self.last_error = None

    def record_failure(self, status: int | None, latency_ms: float, error: BaseException) -> None:
        self.failures += 1
        self.last_latency_ms = latency_ms
        self.last_error_status = status
        self.last_error = str(error)


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    fetcher: str
    label: str
    overrides: bool = False
    requires_key: Callable[[], str | None] | None = None


_PROVIDER_NAMES = ["birdeye", "jupiter", "dexscreener", "pyth", "synthetic"]


def _init_provider_health() -> Dict[str, ProviderHealth]:
    return {name: ProviderHealth(name=name) for name in _PROVIDER_NAMES}


def _init_provider_stats() -> Dict[str, ProviderStats]:
    return {name: ProviderStats(name=name) for name in _PROVIDER_NAMES}


PROVIDER_HEALTH: Dict[str, ProviderHealth] = _init_provider_health()
PROVIDER_STATS: Dict[str, ProviderStats] = _init_provider_stats()


PROVIDER_CONFIGS: Dict[str, ProviderConfig] = {
    "birdeye": ProviderConfig(
        name="birdeye",
        fetcher="_fetch_quotes_birdeye",
        label="Birdeye",
        overrides=False,
        requires_key=lambda: _get_birdeye_api_key(),
    ),
    "jupiter": ProviderConfig(
        name="jupiter",
        fetcher="_fetch_quotes_jupiter",
        label="Jupiter",
    ),
    "dexscreener": ProviderConfig(
        name="dexscreener",
        fetcher="_fetch_quotes_dexscreener",
        label="Dexscreener",
    ),
    "pyth": ProviderConfig(
        name="pyth",
        fetcher="_fetch_quotes_pyth",
        label="Pyth",
        overrides=True,
    ),
    "synthetic": ProviderConfig(
        name="synthetic",
        fetcher="_fetch_quotes_synthetic",
        label="Synthetic",
    ),
}


def _get_birdeye_api_key() -> str | None:
    global _LAST_BIRDEYE_KEY
    key = os.getenv("BIRDEYE_API_KEY") or None
    if key != _LAST_BIRDEYE_KEY:
        _LAST_BIRDEYE_KEY = key
        if key:
            PROVIDER_HEALTH["birdeye"].clear()
    return key


def reset_provider_health() -> None:
    global PROVIDER_HEALTH, PROVIDER_STATS
    PROVIDER_HEALTH = _init_provider_health()
    PROVIDER_STATS = _init_provider_stats()


def get_provider_health_snapshot() -> Dict[str, Dict[str, Any]]:
    snapshot: Dict[str, Dict[str, Any]] = {}
    now = _monotonic()
    for name, state in PROVIDER_HEALTH.items():
        stats = PROVIDER_STATS[name]
        snapshot[name] = {
            "cooldown_remaining": max(0.0, state.cooldown_until - now),
            "consecutive_failures": state.consecutive_failures,
            "last_status": state.last_status,
            "healthy": state.healthy and not state.in_cooldown(),
            "successes": stats.successes,
            "failures": stats.failures,
            "last_latency_ms": stats.last_latency_ms,
            "last_error_status": stats.last_error_status,
            "last_error": stats.last_error,
        }
    return snapshot


def _tokens_key(tokens: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(tok for tok in tokens if isinstance(tok, str) and tok))


def get_cached_quote(token: str) -> PriceQuote | None:
    quote = QUOTE_CACHE.get(token)
    if isinstance(quote, PriceQuote):
        return quote
    return None


def get_cached_price(token: str) -> float | None:
    quote = get_cached_quote(token)
    if quote is not None:
        return quote.price_usd
    value = PRICE_CACHE.get(token)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def update_price_cache(token: str, price: float | PriceQuote) -> None:
    if isinstance(price, PriceQuote):
        QUOTE_CACHE.set(token, price)
        PRICE_CACHE.set(token, float(price.price_usd))
        return
    if isinstance(price, (int, float)):
        quote = PriceQuote(
            price_usd=float(price),
            source="manual",
            asof=_now_ms(),
            quality="synthetic",
        )
        QUOTE_CACHE.set(token, quote)
        PRICE_CACHE.set(token, float(price))


def store_quote(token: str, quote: PriceQuote) -> None:
    QUOTE_CACHE.set(token, quote)
    PRICE_CACHE.set(token, quote.price_usd)


async def _request_json(
    session: aiohttp.ClientSession,
    url: str,
    provider: str,
    *,
    params: Union[Dict[str, Any], Sequence[Tuple[str, Any]]] | None = None,
    headers: Dict[str, str] | None = None,
    json: Any | None = None,
    method: str = "GET",
) -> Any:
    last_error: BaseException | None = None
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
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            last_error = exc
            if attempt == PRICE_RETRY_ATTEMPTS - 1:
                break
            delay = PRICE_RETRY_BACKOFF * (2 ** attempt)
            await asyncio.sleep(delay)
    if last_error:
        raise last_error
    return None


def _extract_price(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    if isinstance(value, dict):
        for key in (
            "price",
            "value",
            "priceUsd",
            "price_usd",
            "usd",
            "price_per_token",
        ):
            if key in value:
                price = _extract_price(value[key])
                if price is not None:
                    return price
        data = value.get("data")
        if isinstance(data, dict):
            return _extract_price(data)
    return None


async def _fetch_quotes_jupiter(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    if not tokens:
        return {}
    url = JUPITER_PRICE_URL.strip()
    if not url:
        return {}
    quotes: Dict[str, PriceQuote] = {}
    for chunk in _chunked(tokens, JUPITER_BATCH_SIZE):
        params = {"ids": ",".join(chunk)}
        payload = await _request_json(session, url, "Jupiter", params=params)
        if not isinstance(payload, MutableMapping):
            continue
        data = payload.get("data") if isinstance(payload.get("data"), MutableMapping) else payload
        if not isinstance(data, MutableMapping):
            continue
        timestamp = int(payload.get("timestamp", _now_ms()))
        asof = timestamp if timestamp > 1e12 else _now_ms()
        for token in chunk:
            entry = data.get(token)
            price = _extract_price(entry)
            if price is not None:
                quotes[token] = PriceQuote(
                    price_usd=price,
                    source="jupiter",
                    asof=asof,
                    quality="aggregate",
                )
    return quotes


async def _fetch_quotes_dexscreener(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    quotes: Dict[str, PriceQuote] = {}
    for token in tokens:
        url = f"{DEXSCREENER_PRICE_URL.rstrip('/')}/{token}"
        payload = await _request_json(session, url, "Dexscreener")
        if not isinstance(payload, MutableMapping):
            continue
        pairs = payload.get("pairs")
        best_pair: MutableMapping[str, Any] | None = None
        if isinstance(pairs, list):
            for candidate in pairs:
                if isinstance(candidate, MutableMapping) and _extract_price(candidate.get("priceUsd")):
                    best_pair = candidate
                    break
                if isinstance(candidate, MutableMapping) and best_pair is None:
                    best_pair = candidate
        liquidity_hint = None
        if isinstance(best_pair, MutableMapping):
            price = _extract_price(best_pair.get("priceUsd")) or _extract_price(
                best_pair.get("price")
            )
            liquidity = best_pair.get("liquidity")
            if isinstance(liquidity, MutableMapping):
                liquidity_hint = _extract_price(liquidity.get("usd"))
        else:
            price = _extract_price(payload)
        if price is None:
            continue
        quotes[token] = PriceQuote(
            price_usd=price,
            source="dexscreener",
            asof=_now_ms(),
            quality="aggregate",
            liquidity_hint=liquidity_hint,
        )
    return quotes


async def _fetch_quotes_birdeye(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    api_key = _get_birdeye_api_key()
    if not api_key or not tokens:
        return {}
    url = f"{BIRDEYE_PRICE_URL.rstrip('/')}/defi/multi_price"
    headers = {
        "X-API-KEY": api_key,
        "accept": "application/json",
        "x-chain": BIRDEYE_CHAIN,
    }
    quotes: Dict[str, PriceQuote] = {}
    chunks = _chunked(tokens, min(BIRDEYE_MAX_BATCH, 100))
    for idx, chunk in enumerate(chunks):
        params = {"list_address": ",".join(chunk), "chain": BIRDEYE_CHAIN}
        payload = await _request_json(session, url, "Birdeye", params=params, headers=headers)
        if not isinstance(payload, MutableMapping):
            continue
        data = payload.get("data")
        if not isinstance(data, MutableMapping):
            data = payload
        asof = int(payload.get("timestamp", _now_ms()))
        if asof < 1_000_000_000_000:
            asof = _now_ms()
        for token in chunk:
            entry = data.get(token)
            if entry is None:
                continue
            price = None
            if isinstance(entry, MutableMapping):
                price = _extract_price(entry.get("price")) or _extract_price(entry.get("value"))
                if price is None:
                    price = _extract_price(entry)
            else:
                price = _extract_price(entry)
            if price is not None:
                quotes[token] = PriceQuote(
                    price_usd=price,
                    source="birdeye",
                    asof=asof,
                    quality="aggregate",
                )
        if idx + 1 < len(chunks):
            await asyncio.sleep(1.0)
    return quotes


def _parse_pyth_mapping() -> Dict[str, str]:
    env_value = os.getenv("PYTH_PRICE_IDS")
    mapping: Dict[str, str] = {}
    if env_value:
        try:
            loaded = json.loads(env_value)
            if isinstance(loaded, MutableMapping):
                mapping.update({str(k): str(v) for k, v in loaded.items()})
        except json.JSONDecodeError:
            logger.warning("Failed to decode PYTH_PRICE_IDS env; ignoring")
    default_mapping = {
        "So11111111111111111111111111111111111111112": "J83JdAq8FDeC8v2WFE2QyXkJhtCmvYzu3d6PvMfo4WwS",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9": "GkzKf5qcF6edCbnMD4HzyBbs6k8ZZrVSu2Ce279b9EcT",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "7sxNXmAf6oMzFxLpyR4V6kRDeo63HgNbUsVTNff7kX2Z",
    }
    mapping.update({k: v for k, v in default_mapping.items() if k not in mapping})
    return mapping


async def _fetch_quotes_pyth(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    mapping = _parse_pyth_mapping()
    reverse = {v: k for k, v in mapping.items()}
    ids = [mapping[token] for token in tokens if token in mapping]
    if not ids:
        return {}
    params = {"ids": ",".join(ids)}
    payload = await _request_json(session, PYTH_PRICE_URL, "Pyth", params=params)
    if not isinstance(payload, MutableMapping):
        return {}
    records: List[MutableMapping[str, Any]] = []
    if isinstance(payload.get("parsed"), list):
        records.extend([item for item in payload["parsed"] if isinstance(item, MutableMapping)])
    if isinstance(payload.get("data"), list):
        records.extend([item for item in payload["data"] if isinstance(item, MutableMapping)])
    if isinstance(payload.get("prices"), list):
        records.extend([item for item in payload["prices"] if isinstance(item, MutableMapping)])
    if not records and isinstance(payload.get("result"), list):
        records.extend([item for item in payload["result"] if isinstance(item, MutableMapping)])
    quotes: Dict[str, PriceQuote] = {}
    for entry in records:
        price_info = entry.get("price")
        token_id = entry.get("id") or entry.get("price_id")
        if isinstance(price_info, MutableMapping):
            raw_price = price_info.get("price") or price_info.get("aggregate_price")
            expo = price_info.get("expo") or price_info.get("exponent")
            if isinstance(raw_price, (int, float)) and isinstance(expo, (int, float)):
                price = float(raw_price) * (10 ** float(expo))
            else:
                price = _extract_price(price_info)
        else:
            price = _extract_price(price_info)
        if price is None and isinstance(entry.get("ema_price"), MutableMapping):
            ema = entry["ema_price"]
            raw_price = ema.get("price")
            expo = ema.get("expo")
            if isinstance(raw_price, (int, float)) and isinstance(expo, (int, float)):
                price = float(raw_price) * (10 ** float(expo))
        if price is None:
            continue
        token = reverse.get(str(token_id))
        if not token:
            continue
        publish_time = entry.get("publish_time") or entry.get("publishTime") or entry.get("timestamp")
        if isinstance(publish_time, (int, float)):
            asof = int(publish_time * 1000 if publish_time < 1e12 else publish_time)
        else:
            asof = _now_ms()
        quotes[token] = PriceQuote(
            price_usd=price,
            source="pyth",
            asof=asof,
            quality="authoritative",
        )
    return quotes


async def _fetch_quotes_synthetic(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    quotes: Dict[str, PriceQuote] = {}
    hints_raw = os.getenv(SYNTHETIC_HINTS_ENV)
    hints: Dict[str, Any] = {}
    if hints_raw:
        try:
            loaded = json.loads(hints_raw)
            if isinstance(loaded, MutableMapping):
                hints = loaded
        except json.JSONDecodeError:
            logger.debug("Invalid JSON in %s; ignoring", SYNTHETIC_HINTS_ENV)
    fallback_value = None
    if OFFLINE_PRICE_DEFAULT is not None:
        try:
            fallback_value = float(OFFLINE_PRICE_DEFAULT)
        except ValueError:
            fallback_value = None
    now = _now_ms()
    for token in tokens:
        if isinstance(hints.get(token), (int, float)):
            price = float(hints[token])
        elif fallback_value is not None:
            price = fallback_value
        else:
            continue
        quotes[token] = PriceQuote(
            price_usd=price,
            source="synthetic",
            asof=now,
            quality="synthetic",
        )
    return quotes


def _provider_priority() -> List[str]:
    order = ["jupiter", "dexscreener", "birdeye", "pyth", "synthetic"]
    state = PROVIDER_HEALTH.get("birdeye")
    key = _get_birdeye_api_key() if state else None
    if state and key and not state.in_cooldown() and state.healthy:
        order.remove("birdeye")
        order.insert(0, "birdeye")
    return order


def _record_provider_success(name: str, latency_ms: float) -> None:
    state = PROVIDER_HEALTH[name]
    state.record_success()
    PROVIDER_STATS[name].record_success(latency_ms)


def _retry_after_seconds(headers: MutableMapping[str, str] | None) -> float | None:
    if not headers:
        return None
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    if not retry_after:
        return None
    try:
        return float(retry_after)
    except ValueError:
        return None


def _record_provider_failure(name: str, exc: BaseException, latency_ms: float) -> None:
    status: int | None = None
    cooldown: float | None = None
    headers: MutableMapping[str, str] | None = None
    if isinstance(exc, aiohttp.ClientResponseError):
        status = exc.status
        headers = exc.headers or {}
        if status in (401, 403):
            cooldown = 300.0
        elif status == 429:
            cooldown = _retry_after_seconds(headers) or 10.0
        elif status and status >= 500:
            cooldown = None
    elif isinstance(exc, asyncio.TimeoutError):
        cooldown = 2.0
    PROVIDER_HEALTH[name].record_failure(status, cooldown=cooldown)
    PROVIDER_STATS[name].record_failure(status, latency_ms, exc)
    if isinstance(exc, aiohttp.ClientResponseError):
        logger.warning(
            "Prices: %s HTTP error %s", name, exc.status,
        )
    else:
        logger.warning("Prices: %s failure %s", name, exc)


async def _fetch_price_quotes(tokens: Sequence[str]) -> Dict[str, PriceQuote]:
    if not tokens:
        return {}
    session = await get_session()
    resolved: Dict[str, PriceQuote] = {}
    order = _provider_priority()
    for idx, provider_name in enumerate(order):
        config = PROVIDER_CONFIGS[provider_name]
        state = PROVIDER_HEALTH[provider_name]
        required_key = config.requires_key() if config.requires_key else None
        if config.requires_key and not required_key:
            logger.debug("Prices: skipping %s provider due to missing key", config.label)
            continue
        if state.in_cooldown():
            logger.debug("Prices: skipping %s provider due to cooldown", config.label)
            continue
        pending = tokens if config.overrides else [tok for tok in tokens if tok not in resolved]
        if not pending and not config.overrides:
            continue
        fetcher = globals()[config.fetcher]
        start = _monotonic()
        try:
            quotes = await fetcher(session, tuple(pending))
        except Exception as exc:  # noqa: BLE001 - deliberate broad catch
            latency_ms = (_monotonic() - start) * 1000.0
            _record_provider_failure(provider_name, exc, latency_ms)
            continue
        latency_ms = (_monotonic() - start) * 1000.0
        _record_provider_success(provider_name, latency_ms)
        if not quotes:
            continue
        if config.overrides:
            for token, quote in quotes.items():
                resolved[token] = quote
        else:
            for token, quote in quotes.items():
                if token not in resolved:
                    resolved[token] = quote
        if len(resolved) == len(tokens):
            override_ahead = any(
                PROVIDER_CONFIGS[name].overrides for name in order[idx + 1 :]
            )
            if not override_ahead:
                break
    if len(resolved) < len(tokens):
        missing = [tok for tok in tokens if tok not in resolved]
        if missing:
            logger.warning(
                "Prices: unresolved token(s) after provider loop: %s", ", ".join(missing)
            )
    return {token: resolved[token] for token in tokens if token in resolved}


async def fetch_price_quotes_async(tokens: Iterable[str]) -> Dict[str, PriceQuote]:
    token_list = _tokens_key(tokens)
    if not token_list:
        return {}
    result: Dict[str, PriceQuote] = {}
    missing: List[str] = []
    for token in token_list:
        cached = get_cached_quote(token)
        if cached is not None:
            result[token] = cached
        else:
            missing.append(token)
    if result:
        logger.info(
            "Prices: cache satisfied %d token(s); %d to fetch", len(result), len(missing)
        )
    if missing:
        fetched = await _fetch_price_quotes(tuple(missing))
        for token, quote in fetched.items():
            store_quote(token, quote)
            result[token] = quote
        logger.info("Prices: fetched %d token(s) via providers", len(fetched))
    return result


async def fetch_token_prices_async(tokens: Iterable[str]) -> Dict[str, float]:
    quotes = await fetch_price_quotes_async(tokens)
    return {token: quote.price_usd for token, quote in quotes.items()}


def fetch_token_prices(tokens: Iterable[str]) -> Dict[str, float]:
    return run_async(lambda: fetch_token_prices_async(tokens))


def warm_cache(tokens: Iterable[str]) -> None:
    run_async(lambda: fetch_price_quotes_async(tokens))
