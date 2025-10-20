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
import random
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Sequence, Tuple, Union

import aiohttp
try:
    import base58
except ImportError:  # pragma: no cover - lightweight fallback
    class _Base58Fallback:
        _alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
        _index = {ch: idx for idx, ch in enumerate(_alphabet)}

        @classmethod
        def b58decode(cls, value: str) -> bytes:
            num = 0
            for char in value:
                if char not in cls._index:
                    raise ValueError(f"Invalid base58 character {char!r}")
                num = num * 58 + cls._index[char]
            result = num.to_bytes((num.bit_length() + 7) // 8, "big") if num else b""
            leading = len(value) - len(value.lstrip("1"))
            return b"\x00" * leading + result

    base58 = _Base58Fallback()  # type: ignore[assignment]

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

JUPITER_PRICE_URL = os.getenv("JUPITER_PRICE_URL", "https://price.jup.ag/v4/price")
JUPITER_BATCH_SIZE = max(1, int(os.getenv("JUPITER_BATCH_SIZE", "64") or 64))

DEXSCREENER_PRICE_URL = os.getenv(
    "DEXSCREENER_PRICE_URL", "https://api.dexscreener.com/latest/dex/tokens"
)

BIRDEYE_PRICE_URL = os.getenv("BIRDEYE_PRICE_URL", "https://api.birdeye.so")
BIRDEYE_CHAIN = os.getenv("BIRDEYE_CHAIN", "solana")
BIRDEYE_MAX_BATCH = max(1, int(os.getenv("BIRDEYE_BATCH_SIZE", "20") or 20))

if not os.getenv("HTTP_FORCE_IPV4"):
    os.environ.setdefault("HTTP_FORCE_IPV4", "1")

PYTH_PRICE_URL = os.getenv("PYTH_PRICE_URL", "https://hermes.pyth.network/v2/price_feeds")

SYNTHETIC_HINTS_ENV = "SYNTHETIC_PRICE_HINTS"
OFFLINE_PRICE_DEFAULT = os.getenv("OFFLINE_PRICE_DEFAULT")

_LAST_BIRDEYE_KEY: str | None = None
_LAST_BIRDEYE_FAILURE_KEY: str | None = None
_BIRDEYE_KEY_FINGERPRINT: str | None = None

_PYTH_BOOT_TIMEOUT = max(0.5, float(os.getenv("PYTH_BOOT_TIMEOUT", "3.0") or 3.0))
_PYTH_VALIDATE_FLAG = (os.getenv("PYTH_VALIDATE_ON_BOOT") or "1").strip().lower()
_PYTH_VALIDATE_ENABLED = _PYTH_VALIDATE_FLAG not in {"0", "false", "no", "off"}


# ---------------------------------------------------------------------------
# Pyth identifier handling
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PythIdentifier:
    """Normalized view of a Pyth feed identifier or price account."""

    feed_id: str
    account: str | None
    raw: str

    @property
    def kind(self) -> str:
        return "account" if self.account else "feed"

    @property
    def request_value(self) -> str:
        return self.account or self.feed_id


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


def _normalize_pyth_identifier(value: str) -> PythIdentifier:
    candidate = (value or "").strip()
    if not candidate:
        raise ValueError("empty price id")

    errors: List[str] = []

    def _format_length_error(codec: str, decoded: bytes) -> str:
        return f"{codec} decoded length {len(decoded)} bytes (expected 32)"

    # Hex (with or without 0x prefix)
    hex_candidate = candidate[2:] if candidate.lower().startswith("0x") else candidate
    try:
        decoded_hex = bytes.fromhex(hex_candidate) if hex_candidate else b""
    except ValueError as exc:
        errors.append(f"hex decode failed: {exc}")
    else:
        if len(decoded_hex) == 32:
            normalized = "0x" + decoded_hex.hex()
            return PythIdentifier(feed_id=normalized, account=None, raw=candidate)
        if decoded_hex:
            errors.append(_format_length_error("hex", decoded_hex))
        else:
            errors.append("hex decode failed: empty input")

    # Base58
    try:
        decoded_b58 = base58.b58decode(candidate)
    except ValueError as exc:
        errors.append(f"base58 decode failed: {exc}")
    else:
        if len(decoded_b58) == 32:
            normalized = "0x" + decoded_b58.hex()
            return PythIdentifier(feed_id=normalized, account=candidate, raw=candidate)
        errors.append(_format_length_error("base58", decoded_b58))

    # Base64
    try:
        decoded_b64 = base64.b64decode(candidate, validate=True)
    except (binascii.Error, ValueError) as exc:
        errors.append(f"base64 decode failed: {exc}")
    else:
        if len(decoded_b64) == 32:
            normalized = "0x" + decoded_b64.hex()
            return PythIdentifier(feed_id=normalized, account=None, raw=candidate)
        errors.append(_format_length_error("base64", decoded_b64))

    raise ValueError("; ".join(errors) if errors else "invalid price id format")


def _normalize_pyth_id(value: str) -> str:
    return _normalize_pyth_identifier(value).feed_id


def _normalize_pyth_mapping(
    mapping: Mapping[str, str], extras: Iterable[str]
) -> Tuple[Dict[str, PythIdentifier], List[PythIdentifier], List[Tuple[str | None, str, str]]]:
    normalized_mapping: Dict[str, PythIdentifier] = {}
    normalized_extras: List[PythIdentifier] = []
    issues: List[Tuple[str | None, str, str]] = []

    for mint, raw_id in mapping.items():
        try:
            normalized_mapping[mint] = _normalize_pyth_identifier(raw_id)
        except ValueError as exc:
            issues.append((mint, raw_id, str(exc)))

    for raw_id in extras:
        try:
            normalized_extras.append(_normalize_pyth_identifier(raw_id))
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


def _build_pyth_boot_url(
    base_url: str, *, feed_ids: Sequence[str], accounts: Sequence[str]
) -> str:
    parsed = urllib.parse.urlsplit(base_url)
    query_items = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    for value in feed_ids:
        query_items.append(("ids[]", value))
    for account in accounts:
        query_items.append(("accounts[]", account))
    encoded_query = urllib.parse.urlencode(query_items, doseq=True, safe="[]")
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
    last_cooldown_log: float = 0.0

    def in_cooldown(self) -> bool:
        return _monotonic() < self.cooldown_until

    def record_success(self) -> None:
        self.cooldown_until = 0.0
        self.consecutive_failures = 0
        self.last_status = None
        self.healthy = True
        self.last_cooldown_log = 0.0

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
        self.last_cooldown_log = 0.0


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


_DEFAULT_PROVIDER_ORDER = ["pyth", "helius", "birdeye", "jupiter", "dexscreener", "synthetic"]


def _parse_provider_roster(raw: str | None) -> List[str]:
    if not raw:
        return list(_DEFAULT_PROVIDER_ORDER)
    tokens = [token.strip().lower() for token in raw.replace(";", ",").split(",")]
    resolved: List[str] = []
    for token in tokens:
        if not token:
            continue
        if token not in _ALL_PROVIDER_CONFIGS:
            logger.warning("Unknown price provider '%s' ignored", token)
            continue
        if token not in resolved:
            resolved.append(token)
    return resolved or list(_DEFAULT_PROVIDER_ORDER)


_ACTIVE_PROVIDER_NAMES: List[str] = []


def _init_provider_health() -> Dict[str, ProviderHealth]:
    return {name: ProviderHealth(name=name) for name in _ACTIVE_PROVIDER_NAMES}


def _init_provider_stats() -> Dict[str, ProviderStats]:
    return {name: ProviderStats(name=name) for name in _ACTIVE_PROVIDER_NAMES}


_ALL_PROVIDER_CONFIGS: Dict[str, ProviderConfig] = {
    "helius": ProviderConfig(
        name="helius",
        fetcher="_fetch_quotes_helius",
        label="Helius",
    ),
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

PROVIDER_CONFIGS: Dict[str, ProviderConfig] = {}
PROVIDER_HEALTH: Dict[str, ProviderHealth] = {}
PROVIDER_STATS: Dict[str, ProviderStats] = {}


def _rebuild_provider_tables() -> None:
    global _ACTIVE_PROVIDER_NAMES, PROVIDER_CONFIGS, PROVIDER_HEALTH, PROVIDER_STATS
    _ACTIVE_PROVIDER_NAMES = _parse_provider_roster(os.getenv("PRICE_PROVIDERS"))
    PROVIDER_CONFIGS = {
        name: _ALL_PROVIDER_CONFIGS[name] for name in _ACTIVE_PROVIDER_NAMES
    }
    PROVIDER_HEALTH = _init_provider_health()
    PROVIDER_STATS = _init_provider_stats()
    if "jupiter" in PROVIDER_HEALTH:
        PROVIDER_HEALTH["jupiter"].healthy = False


_rebuild_provider_tables()


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
        if (
            key
            and key != _LAST_BIRDEYE_FAILURE_KEY
            and "birdeye" in PROVIDER_HEALTH
        ):
            PROVIDER_HEALTH["birdeye"].clear()
            _LAST_BIRDEYE_FAILURE_KEY = None
    return key


def reset_provider_health() -> None:
    global PROVIDER_HEALTH, PROVIDER_STATS, _LAST_BIRDEYE_KEY, _LAST_BIRDEYE_FAILURE_KEY
    _rebuild_provider_tables()
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
            if isinstance(exc, aiohttp.ClientConnectorError):
                delay = max(delay, 0.2)
            jitter = random.uniform(0.05, 0.25)
            await asyncio.sleep(delay + jitter)
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
        "x-api-key": api_key,
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


async def _fetch_quotes_helius(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    api_url = (os.getenv("HELIUS_PRICE_REST_URL") or "").strip()
    api_key = (os.getenv("HELIUS_API_KEY") or os.getenv("HELIUS_API_TOKEN") or "").strip()
    if not tokens or not api_url or not api_key:
        return {}
    url = api_url.rstrip("/")
    headers = {
        "x-helius-api-key": api_key,
    }
    quotes: Dict[str, PriceQuote] = {}
    for chunk in _chunked(tokens, 50):
        params = {"ids": ",".join(chunk), "api-key": api_key}
        start = _monotonic()
        try:
            payload = await _request_json(
                session,
                url,
                "HeliusPrice",
                headers=headers,
                params=params,
                method="GET",
            )
        except Exception as exc:
            _record_provider_failure("helius", exc, 0.0)
            continue
        else:
            latency_ms = (_monotonic() - start) * 1000.0
            _record_provider_success("helius", latency_ms)
        if not isinstance(payload, MutableMapping):
            continue
        data = payload.get("data")
        if not isinstance(data, MutableMapping):
            data = payload
        timestamp = int(payload.get("timestamp", _now_ms()))
        if timestamp < 1_000_000_000_000:
            timestamp = _now_ms()
        for token in chunk:
            entry = data.get(token)
            price = _extract_price(entry)
            if price is None and isinstance(entry, MutableMapping):
                price = _extract_price(entry.get("prices"))
            if price is None:
                continue
            quotes[token] = PriceQuote(
                price_usd=price,
                source="helius",
                asof=timestamp,
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


def _parse_pyth_mapping() -> Tuple[Dict[str, PythIdentifier], List[PythIdentifier]]:
    mapping, extras = _load_custom_pyth_ids()
    for mint, price_id in DEFAULT_PYTH_PRICE_IDS.items():
        mapping.setdefault(mint, price_id)
    normalized_mapping, normalized_extras, issues = _normalize_pyth_mapping(mapping, extras)
    if issues:
        _log_pyth_issues(issues)
    return normalized_mapping, normalized_extras


@lru_cache(maxsize=1)
def validate_pyth_overrides_on_boot(*, network_required: bool = True) -> None:
    if not _PYTH_VALIDATE_ENABLED:
        return
    raw_mapping, extras = _load_custom_pyth_ids()
    normalized_mapping, _, issues = _normalize_pyth_mapping(raw_mapping, extras)
    if issues:
        _log_pyth_issues(issues)
    id_sources: Dict[Tuple[str, str], List[Tuple[str | None, str]]] = {}
    feed_ids: List[str] = []
    accounts: List[str] = []
    for mint, identifier in normalized_mapping.items():
        raw_value = raw_mapping.get(mint, "")
        key = (identifier.kind, identifier.request_value)
        id_sources.setdefault(key, []).append((mint, raw_value))
        if identifier.account:
            accounts.append(identifier.account)
        feed_ids.append(identifier.feed_id)
    for identifier in extras:
        key = (identifier.kind, identifier.request_value)
        id_sources.setdefault(key, []).append((None, identifier.raw))
        if identifier.account:
            accounts.append(identifier.account)
        feed_ids.append(identifier.feed_id)
    unique_feed_ids = sorted({fid for fid in feed_ids if fid})
    unique_accounts = sorted({acc for acc in accounts if acc})
    if not unique_feed_ids and not unique_accounts:
        return
    base_url = PYTH_PRICE_URL.strip() or "https://hermes.pyth.network/v2/price_feeds"
    url = _build_pyth_boot_url(base_url, feed_ids=unique_feed_ids, accounts=unique_accounts)
    headers = {"Accept": "application/json"}
    logger.debug(
        "Validating custom Pyth overrides via %s", url
    )
    strict = bool(network_required)
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
            for key, sources in id_sources.items():
                _, normalized_value = key
                for mint, raw_value in sources:
                    masked_value = _mask_identifier(raw_value)
                    if mint:
                        entries.append(
                            f"{_mask_identifier(mint)}→{masked_value or _mask_identifier(normalized_value)}"
                        )
                    else:
                        entries.append(f"extra:{masked_value or _mask_identifier(normalized_value)}")
            detail_text = preview or exc.reason or "client error"
            log_fn = logger.error if strict else logger.warning
            log_fn(
                "PYTH_BOOT_VALIDATION_FAILED status=%s count=%d detail=%s entries=%s url=%s",
                status,
                len(entries),
                detail_text,
                ", ".join(entries) if entries else "<unknown>",
                url,
            )
            _mark_pyth_provider_unhealthy(status, detail_text)
            if strict:
                raise RuntimeError(
                    f"Pyth override validation failed (status={status}): {detail_text}"
                )
            return
        logger.debug(
            "Pyth override validation received HTTP %s from %s", status, url
        )
    except Exception as exc:  # pragma: no cover - network best effort
        if strict:
            logger.debug(
                "Pyth override validation encountered %s via %s", exc, url
            )
        else:
            logger.warning(
                "Pyth override validation skipped due to %s (url=%s)", exc, url
            )


async def _fetch_quotes_pyth(
    session: aiohttp.ClientSession, tokens: Sequence[str]
) -> Dict[str, PriceQuote]:
    mapping, extras = _parse_pyth_mapping()
    reverse: Dict[str, str] = {}
    account_reverse: Dict[str, str] = {}
    for mint, identifier in mapping.items():
        reverse[identifier.feed_id] = mint
        if identifier.account:
            account_reverse[identifier.account] = mint
    requested_tokens = set(tokens)
    feed_ids = {mapping[token].feed_id for token in tokens if token in mapping}
    feed_ids.update(identifier.feed_id for identifier in extras)
    account_ids = {
        mapping[token].account
        for token in tokens
        if token in mapping and mapping[token].account
    }
    account_ids.update(identifier.account for identifier in extras if identifier.account)
    feed_ids = {fid for fid in feed_ids if fid}
    account_ids = {acc for acc in account_ids if acc}
    if not feed_ids and not account_ids:
        return {}
    sorted_feeds = sorted(feed_ids)
    sorted_accounts = sorted(account_ids)
    params: List[Tuple[str, str]] = []
    params.extend(("ids[]", value) for value in sorted_feeds)
    params.extend(("accounts[]", value) for value in sorted_accounts)
    headers = {"Accept": "application/json"}
    request_url = _build_pyth_boot_url(
        PYTH_PRICE_URL, feed_ids=sorted_feeds, accounts=sorted_accounts
    )
    try:
        payload = await _request_json(
            session, PYTH_PRICE_URL, "Pyth", params=params, headers=headers
        )
    except aiohttp.ClientResponseError as exc:
        if exc.status == 400:
            logger.error(
                "Pyth price request returned HTTP 400 via %s (tokens=%s)",
                request_url,
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
        token_id = (
            entry.get("id")
            or entry.get("price_id")
            or entry.get("priceAccount")
            or entry.get("price_account")
        )
        if not isinstance(token_id, str):
            continue
        try:
            normalized_id = _normalize_pyth_id(token_id)
        except ValueError:
            normalized_id = str(token_id)
        token = reverse.get(normalized_id)
        if not token:
            token = account_reverse.get(token_id)
        if not token:
            inferred = _infer_pyth_mint(entry, str(token_id), requested_tokens)
            if inferred:
                reverse[normalized_id] = inferred
                if token_id:
                    account_reverse[token_id] = inferred
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
    order = [name for name in _DEFAULT_PROVIDER_ORDER if name in PROVIDER_CONFIGS]
    state = PROVIDER_HEALTH.get("birdeye")
    key = _get_birdeye_api_key() if state else None
    if state and key and not state.in_cooldown() and state.healthy and "birdeye" in order:
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
    state = PROVIDER_HEALTH[name]
    previous_cooldown = state.cooldown_until
    state.record_failure(status, cooldown=cooldown)
    PROVIDER_STATS[name].record_failure(status, latency_ms, exc)
    if cooldown:
        now = _monotonic()
        if previous_cooldown <= now and (now - state.last_cooldown_log) > 0.1:
            logger.warning(
                "Prices: %s entering cooldown for %.1fs (status=%s, error=%s)",
                name,
                cooldown,
                status,
                exc,
            )
            state.last_cooldown_log = now
        return
    key = (
        f"prices-http:{name}:{getattr(exc, 'status', 'unknown')}"
        if isinstance(exc, aiohttp.ClientResponseError)
        else f"prices-failure:{name}:{exc.__class__.__name__}"
    )
    warn_once_per(
        1.0,
        key,
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

    if "pyth" in order:
        mapping, _ = _parse_pyth_mapping()
        override_tokens = [token for token in tokens if token in mapping]
        if override_tokens:
            start = _monotonic()
            try:
                quotes = await _fetch_quotes_pyth(session, tuple(override_tokens))
            except Exception as exc:  # noqa: BLE001 - deliberate broad catch
                latency_ms = (_monotonic() - start) * 1000.0
                _record_provider_failure("pyth", exc, latency_ms)
            else:
                latency_ms = (_monotonic() - start) * 1000.0
                if quotes:
                    _record_provider_success("pyth", latency_ms)
                    resolved.update(quotes)
                else:
                    _record_provider_failure(
                        "pyth",
                        RuntimeError("no quotes returned"),
                        latency_ms,
                    )
            order = [name for name in order if name != "pyth"]
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
