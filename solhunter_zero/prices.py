import asyncio
import base64
import binascii
import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple, Union

import aiohttp
import base58

from solhunter_zero.lru import TTLCache

from .async_utils import run_async
from .http import get_session
from .logging_utils import warn_once_per
from .token_aliases import canonical_mint, validate_mint

logger = logging.getLogger(__name__)

PRICE_RETRY_ATTEMPTS = max(1, int(os.getenv("PRICE_RETRY_ATTEMPTS", "3") or 3))
PRICE_RETRY_BACKOFF = float(os.getenv("PRICE_RETRY_BACKOFF", "0.5") or 0.5)
PRICE_CACHE_TTL = float(os.getenv("PRICE_CACHE_TTL", "30") or 30)

PRICE_CACHE: TTLCache = TTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
QUOTE_CACHE: TTLCache = TTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
BATCH_CACHE: TTLCache = TTLCache(maxsize=256, ttl=PRICE_CACHE_TTL)

JUPITER_PRICE_URL = os.getenv("JUPITER_PRICE_URL", "https://price.jup.ag/v3/price")
JUPITER_BATCH_SIZE = max(1, int(os.getenv("JUPITER_BATCH_SIZE", "64") or 64))

DEXSCREENER_PRICE_URL = os.getenv(
    "DEXSCREENER_PRICE_URL", "https://api.dexscreener.com/latest/dex/tokens"
)

BIRDEYE_PRICE_URL = os.getenv("BIRDEYE_PRICE_URL", "https://public-api.birdeye.so")
BIRDEYE_CHAIN = os.getenv("BIRDEYE_CHAIN", "solana")
BIRDEYE_MAX_BATCH = max(1, int(os.getenv("BIRDEYE_BATCH_SIZE", "50") or 50))

PYTH_PRICE_URL = os.getenv("PYTH_PRICE_URL", "https://hermes.pyth.network/v2/price_feeds")

SYNTHETIC_HINTS_ENV = "SYNTHETIC_PRICE_HINTS"
OFFLINE_PRICE_DEFAULT = os.getenv("OFFLINE_PRICE_DEFAULT")

_LAST_BIRDEYE_KEY: str | None = None
_LAST_BIRDEYE_FAILURE_KEY: str | None = None
_BIRDEYE_KEY_FINGERPRINT: str | None = None

_PYTH_BOOT_TIMEOUT = max(0.5, float(os.getenv("PYTH_BOOT_TIMEOUT", "3.0") or 3.0))
_PYTH_VALIDATE_FLAG = (os.getenv("PYTH_VALIDATE_ON_BOOT") or "1").strip().lower()
_PYTH_VALIDATE_ENABLED = _PYTH_VALIDATE_FLAG not in {"0", "false", "no", "off"}


DEFAULT_PYTH_PRICE_IDS: Dict[str, str] = {
    "So11111111111111111111111111111111111111112": "J83JdAq8FDeC8v2WFE2QyXkJhtCmvYzu3d6PvMfo4WwS",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9": "GkzKf5qcF6edCbnMD4HzyBbs6k8ZZrVSu2Ce279b9EcT",
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "7sxNXmAf6oMzFxLpyR4V6kRDeo63HgNbUsVTNff7kX2Z",
}


def _mask_identifier(value: str, *, prefix: int = 6, suffix: int = 4) -> str:
    candidate = (value or "").strip()
    if len(candidate) <= prefix + suffix + 1:
        return candidate
    return f"{candidate[:prefix]}…{candidate[-suffix:]}"


def _normalize_pyth_id(value: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        raise ValueError("empty price id")

    errors: List[str] = []

    def _format_length_error(codec: str, decoded: bytes) -> str:
        return f"{codec} decoded length {len(decoded)} bytes (expected 32)"

    # Hex (with or without 0x prefix)
    hex_candidate = candidate[2:] if candidate.lower().startswith("0x") else candidate
    try:
        if hex_candidate:
            decoded = bytes.fromhex(hex_candidate)
        else:
            decoded = b""
    except ValueError as exc:
        errors.append(f"hex decode failed: {exc}")
    else:
        if len(decoded) == 32:
            return "0x" + decoded.hex()
        if decoded:
            errors.append(_format_length_error("hex", decoded))
        else:
            errors.append("hex decode failed: empty input")

    # Base58
    try:
        decoded = base58.b58decode(candidate)
    except ValueError as exc:
        errors.append(f"base58 decode failed: {exc}")
    else:
        if len(decoded) == 32:
            return "0x" + decoded.hex()
        errors.append(_format_length_error("base58", decoded))

    # Base64
    try:
        decoded = base64.b64decode(candidate, validate=True)
    except (binascii.Error, ValueError) as exc:
        errors.append(f"base64 decode failed: {exc}")
    else:
        if len(decoded) == 32:
            return "0x" + decoded.hex()
        errors.append(_format_length_error("base64", decoded))

    raise ValueError("; ".join(errors) if errors else "invalid price id format")


def _normalize_pyth_mapping(
    mapping: Mapping[str, str], extras: Iterable[str]
) -> Tuple[Dict[str, str], List[str], List[Tuple[str | None, str, str]]]:
    normalized_mapping: Dict[str, str] = {}
    normalized_extras: List[str] = []
    issues: List[Tuple[str | None, str, str]] = []

    for mint, raw_id in mapping.items():
        try:
            normalized_mapping[mint] = _normalize_pyth_id(raw_id)
        except ValueError as exc:
            issues.append((mint, raw_id, str(exc)))

    for raw_id in extras:
        try:
            normalized_extras.append(_normalize_pyth_id(raw_id))
        except ValueError as exc:
            issues.append((None, raw_id, str(exc)))

    return normalized_mapping, normalized_extras, issues


def _log_pyth_issues(issues: Iterable[Tuple[str | None, str, str]]) -> None:
    for mint, raw_id, reason in issues:
        masked_id = _mask_identifier(raw_id)
        if mint:
            logger.error(
                "Invalid Pyth price id for %s (%s): %s",
                _mask_identifier(mint),
                masked_id,
                reason,
            )
        else:
            logger.error("Invalid extra Pyth price id %s: %s", masked_id, reason)


def _build_pyth_boot_url(base_url: str, ids: Sequence[str]) -> str:
    parsed = urllib.parse.urlsplit(base_url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    for value in ids:
        query_items.append(("ids[]", value))
    encoded_query = urllib.parse.urlencode(query_items)
    return urllib.parse.urlunsplit(parsed._replace(query=encoded_query))


def _mark_pyth_provider_unhealthy(status: int | None, detail: str) -> None:
    state = PROVIDER_HEALTH.get("pyth")
    if state:
        cooldown = 60.0 if status and 400 <= status < 500 else None
        state.record_failure(status, cooldown=cooldown)
    stats = PROVIDER_STATS.get("pyth")
    if stats:
        stats.record_failure(status, 0.0, RuntimeError(detail))


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

# Treat Jupiter as unverified until a quote succeeds.
if "jupiter" in PROVIDER_HEALTH:
    PROVIDER_HEALTH["jupiter"].healthy = False


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


def _is_valid_birdeye_key(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if len(candidate) <= 20:
        return None
    return candidate


def _log_birdeye_key_fingerprint(key: str) -> None:
    global _BIRDEYE_KEY_FINGERPRINT
    fingerprint = key[-4:] if len(key) >= 4 else key
    if fingerprint == _BIRDEYE_KEY_FINGERPRINT:
        return
    _BIRDEYE_KEY_FINGERPRINT = fingerprint
    logger.info("Birdeye API key configured (…%s)", fingerprint)


def _get_birdeye_api_key() -> str | None:
    global _LAST_BIRDEYE_KEY, _LAST_BIRDEYE_FAILURE_KEY, _BIRDEYE_KEY_FINGERPRINT
    key = _is_valid_birdeye_key(os.getenv("BIRDEYE_API_KEY"))
    if key != _LAST_BIRDEYE_KEY:
        _LAST_BIRDEYE_KEY = key
        if key:
            _log_birdeye_key_fingerprint(key)
        else:
            _BIRDEYE_KEY_FINGERPRINT = None
        if key and key != _LAST_BIRDEYE_FAILURE_KEY:
            PROVIDER_HEALTH["birdeye"].clear()
            _LAST_BIRDEYE_FAILURE_KEY = None
    return key


def reset_provider_health() -> None:
    global PROVIDER_HEALTH, PROVIDER_STATS, _LAST_BIRDEYE_KEY, _LAST_BIRDEYE_FAILURE_KEY
    PROVIDER_HEALTH = _init_provider_health()
    PROVIDER_STATS = _init_provider_stats()
    _LAST_BIRDEYE_KEY = None
    _LAST_BIRDEYE_FAILURE_KEY = None


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


def _canonical_mint_from_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    if not candidate:
        return None
    canonical = canonical_mint(candidate)
    if not validate_mint(canonical):
        return None
    return canonical


def _normalise_token(value: Any) -> str | None:
    if isinstance(value, str):
        candidate = value.strip()
    elif value is None:
        return None
    else:
        candidate = str(value).strip()
    if not candidate:
        return None
    canonical = _canonical_mint_from_value(candidate)
    if not canonical:
        warn_once_per(
            300.0,
            f"prices-invalid:{candidate}",
            "Prices: ignoring invalid mint '%s'",
            candidate,
            logger=logger,
        )
        return None
    return canonical


def _tokens_key(tokens: Iterable[str]) -> tuple[str, ...]:
    seen: Dict[str, None] = {}
    ordered: List[str] = []
    for token in tokens:
        normalised = _normalise_token(token)
        if not normalised or normalised in seen:
            continue
        seen[normalised] = None
        ordered.append(normalised)
    return tuple(ordered)


def _tokens_cache_key(tokens: Iterable[str]) -> tuple[str, ...]:
    if isinstance(tokens, (list, tuple)):
        return tuple(sorted(dict.fromkeys(tokens)))
    return tuple(sorted(_tokens_key(tokens)))


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


def _pyth_decimal_price(raw_price: Any, expo: Any) -> float | None:
    if raw_price is None or expo is None:
        return None
    try:
        expo_int = int(expo)
    except (TypeError, ValueError):
        return None
    try:
        raw_decimal = Decimal(str(raw_price))
    except (InvalidOperation, ValueError, TypeError):
        return None
    try:
        shifted = raw_decimal.scaleb(expo_int)
    except InvalidOperation:
        return None
    return float(shifted)


def _pyth_price_from_info(info: Any) -> float | None:
    if isinstance(info, MutableMapping):
        price = _pyth_decimal_price(info.get("price"), info.get("expo") or info.get("exponent"))
        if price is not None:
            return price
        agg = info.get("aggregate_price")
        if agg is not None:
            price = _pyth_decimal_price(agg, info.get("expo") or info.get("exponent"))
            if price is not None:
                return price
        nested = info.get("price")
        if isinstance(nested, MutableMapping):
            price = _pyth_price_from_info(nested)
            if price is not None:
                return price
        nested = info.get("price_info")
        if isinstance(nested, MutableMapping):
            price = _pyth_price_from_info(nested)
            if price is not None:
                return price
    return _extract_price(info)


def _infer_pyth_mint(
    entry: MutableMapping[str, Any], price_id: str, allowed: set[str]
) -> str | None:
    def search_value(value: Any) -> str | None:
        if isinstance(value, str):
            candidate = _canonical_mint_from_value(value)
            if candidate and candidate != price_id and (not allowed or candidate in allowed):
                return candidate
        elif isinstance(value, MutableMapping):
            return search_mapping(value)
        elif isinstance(value, list):
            for item in value:
                candidate = search_value(item)
                if candidate:
                    return candidate
        return None

    def search_mapping(mapping: MutableMapping[str, Any]) -> str | None:
        prioritised: List[Tuple[str, Any]] = []
        fallback: List[Tuple[str, Any]] = []
        for key, value in mapping.items():
            lowered = key.lower()
            target = "mint" in lowered or "token" in lowered or "address" in lowered or "base" in lowered
            if target:
                prioritised.append((key, value))
            else:
                fallback.append((key, value))
        for _, value in prioritised + fallback:
            candidate = search_value(value)
            if candidate:
                return candidate
        return None

    product = entry.get("product")
    if isinstance(product, MutableMapping):
        candidate = search_mapping(product)
        if candidate:
            return candidate
    return search_mapping(entry)


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
    for chunk in _chunked(tokens, min(BIRDEYE_MAX_BATCH, 100)):
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
    return quotes


def _load_custom_pyth_ids() -> Tuple[Dict[str, str], List[str]]:
    env_value = os.getenv("PYTH_PRICE_IDS")
    mapping: Dict[str, str] = {}
    extras: List[str] = []
    if not env_value:
        return mapping, extras
    try:
        loaded = json.loads(env_value)
    except json.JSONDecodeError:
        logger.warning("Failed to decode PYTH_PRICE_IDS env; ignoring")
        return mapping, extras
    if isinstance(loaded, MutableMapping):
        for raw_mint, raw_id in loaded.items():
            if not isinstance(raw_mint, str) or not isinstance(raw_id, str):
                continue
            mint = canonical_mint(raw_mint)
            if validate_mint(mint):
                mapping[mint] = raw_id
    elif isinstance(loaded, list):
        for item in loaded:
            if isinstance(item, str):
                extras.append(item)
            elif isinstance(item, MutableMapping):
                mint = item.get("mint") or item.get("token") or item.get("address")
                price_id = item.get("price_id") or item.get("id")
                if isinstance(mint, str) and isinstance(price_id, str):
                    canonical = canonical_mint(mint)
                    if validate_mint(canonical):
                        mapping[canonical] = price_id
    else:
        logger.warning(
            "Unexpected PYTH_PRICE_IDS type %s; ignoring", type(loaded).__name__
        )
    return mapping, extras


def _parse_pyth_mapping() -> Tuple[Dict[str, str], List[str]]:
    mapping, extras = _load_custom_pyth_ids()
    for mint, price_id in DEFAULT_PYTH_PRICE_IDS.items():
        mapping.setdefault(mint, price_id)
    normalized_mapping, normalized_extras, issues = _normalize_pyth_mapping(mapping, extras)
    if issues:
        _log_pyth_issues(issues)
    return normalized_mapping, normalized_extras


@lru_cache(maxsize=1)
def validate_pyth_overrides_on_boot(*, network_required: bool = True) -> None:
    if not _PYTH_VALIDATE_ENABLED or not network_required:
        return
    raw_mapping, extras = _load_custom_pyth_ids()
    normalized_mapping, _, issues = _normalize_pyth_mapping(raw_mapping, extras)
    if issues:
        _log_pyth_issues(issues)
    id_sources: Dict[str, List[Tuple[str | None, str]]] = {}
    for mint, normalized in normalized_mapping.items():
        raw_value = raw_mapping.get(mint, "")
        id_sources.setdefault(normalized, []).append((mint, raw_value))
    for raw_value in extras:
        try:
            normalized = _normalize_pyth_id(raw_value)
        except ValueError:
            continue
        id_sources.setdefault(normalized, []).append((None, raw_value))
    ids = sorted(id_sources)
    if not ids:
        return
    base_url = PYTH_PRICE_URL.strip() or "https://hermes.pyth.network/v2/price_feeds"
    url = _build_pyth_boot_url(base_url, ids)
    headers = {"Accept": "application/json"}
    try:
        req = urllib.request.Request(url, method="GET", headers=headers)
        with urllib.request.urlopen(req, timeout=_PYTH_BOOT_TIMEOUT) as resp:
            resp.read(1)
    except urllib.error.HTTPError as exc:
        detail = exc.read() if hasattr(exc, "read") else b""
        preview = detail.decode("utf-8", errors="replace").strip()[:200]
        status = exc.code
        if status and 400 <= status < 500:
            entries: List[str] = []
            for normalized in ids:
                for mint, raw_value in id_sources.get(normalized, []):
                    masked_value = _mask_identifier(raw_value)
                    if mint:
                        entries.append(
                            f"{_mask_identifier(mint)}→{masked_value or _mask_identifier(normalized)}"
                        )
                    else:
                        entries.append(f"extra:{masked_value or _mask_identifier(normalized)}")
            detail_text = preview or exc.reason or "client error"
            logger.error(
                "PYTH_BOOT_VALIDATION_FAILED status=%s count=%d detail=%s entries=%s",
                status,
                len(entries),
                detail_text,
                ", ".join(entries) if entries else "<unknown>",
            )
            _mark_pyth_provider_unhealthy(status, detail_text)
            return
        logger.debug(
            "Pyth override validation received HTTP %s for ids=%s", status, ",".join(ids)
        )
    except Exception as exc:  # pragma: no cover - network best effort
        logger.debug(
            "Pyth override validation encountered %s for ids=%s", exc, ",".join(ids)
        )


async def _fetch_quotes_pyth(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    mapping, extras = _parse_pyth_mapping()
    reverse: Dict[str, str] = {}
    for mint, price_id in mapping.items():
        reverse[price_id] = mint
    requested_tokens = set(tokens)
    ids = {mapping[token] for token in tokens if token in mapping}
    ids.update(extras)
    if not ids:
        return {}
    params: List[Tuple[str, str]] = [("ids[]", value) for value in sorted(ids)]
    headers = {"Accept": "application/json"}
    try:
        payload = await _request_json(
            session, PYTH_PRICE_URL, "Pyth", params=params, headers=headers
        )
    except aiohttp.ClientResponseError as exc:
        if exc.status == 400:
            logger.error(
                "Pyth price request returned HTTP 400 for ids=%s (tokens=%s)",
                ",".join(sorted(ids)),
                ",".join(sorted(requested_tokens)),
            )
        raise
    records: List[MutableMapping[str, Any]] = []
    if isinstance(payload, list):
        records.extend([item for item in payload if isinstance(item, MutableMapping)])
    if isinstance(payload, MutableMapping):
        if isinstance(payload.get("parsed"), list):
            records.extend([item for item in payload["parsed"] if isinstance(item, MutableMapping)])
        if isinstance(payload.get("data"), list):
            records.extend([item for item in payload["data"] if isinstance(item, MutableMapping)])
        if isinstance(payload.get("prices"), list):
            records.extend([item for item in payload["prices"] if isinstance(item, MutableMapping)])
        if not records and isinstance(payload.get("result"), list):
            records.extend([item for item in payload["result"] if isinstance(item, MutableMapping)])
    if not records:
        return {}
    quotes: Dict[str, PriceQuote] = {}
    for entry in records:
        price = _pyth_price_from_info(entry.get("price"))
        if price is None and isinstance(entry.get("ema_price"), MutableMapping):
            price = _pyth_price_from_info(entry.get("ema_price"))
        if price is None and isinstance(entry.get("aggregate_price"), MutableMapping):
            price = _pyth_price_from_info(entry.get("aggregate_price"))
        if price is None:
            continue
        token_id = entry.get("id") or entry.get("price_id")
        if not isinstance(token_id, str):
            continue
        try:
            normalized_id = _normalize_pyth_id(token_id)
        except ValueError:
            normalized_id = str(token_id)
        token = reverse.get(normalized_id)
        if not token:
            inferred = _infer_pyth_mint(entry, str(token_id), requested_tokens)
            if inferred:
                reverse[normalized_id] = inferred
                token = inferred
        if not token or token not in requested_tokens:
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
    elif isinstance(exc, aiohttp.ClientConnectorError):
        cooldown = max(cooldown or 0.0, 15.0)
    if name == "birdeye" and isinstance(exc, aiohttp.ClientResponseError) and exc.status in (401, 403):
        global _LAST_BIRDEYE_FAILURE_KEY
        _LAST_BIRDEYE_FAILURE_KEY = _LAST_BIRDEYE_KEY
    PROVIDER_HEALTH[name].record_failure(status, cooldown=cooldown)
    PROVIDER_STATS[name].record_failure(status, latency_ms, exc)
    if isinstance(exc, aiohttp.ClientResponseError):
        warn_once_per(
            1.0,
            f"prices-http:{name}:{exc.status}",
            "Prices: %s HTTP error %s",
            name,
            exc.status,
            logger=logger,
        )
    else:
        warn_once_per(
            1.0,
            f"prices-failure:{name}:{exc.__class__.__name__}",
            "Prices: %s failure %s",
            name,
            exc,
            logger=logger,
        )


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
        if quotes:
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
    cache_key = _tokens_cache_key(token_list)
    result: Dict[str, PriceQuote] = {}
    if cache_key:
        cached_batch = BATCH_CACHE.get(cache_key)
        if isinstance(cached_batch, dict):
            for token in token_list:
                quote = cached_batch.get(token)
                if isinstance(quote, PriceQuote):
                    result[token] = quote
                    store_quote(token, quote)
    missing: List[str] = []
    for token in token_list:
        if token in result:
            continue
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
    if cache_key and result:
        payload = {token: result[token] for token in token_list if token in result}
        if payload:
            BATCH_CACHE.set(cache_key, payload)
    return result


async def fetch_token_prices_async(tokens: Iterable[str]) -> Dict[str, float]:
    quotes = await fetch_price_quotes_async(tokens)
    return {token: quote.price_usd for token, quote in quotes.items()}


def fetch_token_prices(tokens: Iterable[str]) -> Dict[str, float]:
    return run_async(lambda: fetch_token_prices_async(tokens))


def warm_cache(tokens: Iterable[str]) -> None:
    run_async(lambda: fetch_price_quotes_async(tokens))
