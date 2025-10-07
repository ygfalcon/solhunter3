# solhunter_zero/scanner_onchain.py
from __future__ import annotations

import asyncio
import logging
import os
from typing import List, Dict, Any, Iterable, Tuple, Sequence

import requests

from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient
from solana.rpc.core import RPCException
from solders.pubkey import Pubkey

# Backwards-compatibility alias for tests that patch ``PublicKey`` directly.
PublicKey = Pubkey

from solhunter_zero.lru import TTLCache

from .rpc_helpers import (
    extract_signature_entries,
    extract_token_accounts,
    extract_program_accounts,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants & caches
# ---------------------------------------------------------------------------

# SPL Token Program (SPL-Token v2)
TOKEN_PROGRAM_ID: Pubkey = Pubkey.from_string(
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
)

# Module-level caches for frequently polled metrics
_METRIC_TTL = float(os.getenv("ONCHAIN_METRIC_TTL", "30") or 30.0)
MEMPOOL_RATE_CACHE: TTLCache[Tuple[str, str], float] = TTLCache(maxsize=512, ttl=_METRIC_TTL)
WHALE_ACTIVITY_CACHE: TTLCache[Tuple[str, str], float] = TTLCache(maxsize=512, ttl=_METRIC_TTL)
AVG_SWAP_SIZE_CACHE: TTLCache[Tuple[str, str], float] = TTLCache(maxsize=512, ttl=_METRIC_TTL)

# Backoff window when a provider (currently Helius) rejects heavy GPA scans. Using a TTL cache
# keeps the behaviour stateless between runs but prevents hammering the RPC on every discovery
# cycle when we already know the request will be refused.
_HEAVY_SCAN_BACKOFF_TTL = float(os.getenv("PROGRAM_SCAN_BACKOFF", "600") or 600.0)
_HEAVY_SCAN_BACKOFF: TTLCache[str, bool] = TTLCache(
    maxsize=16,
    ttl=_HEAVY_SCAN_BACKOFF_TTL,
)

# History buffer used for optional forecasting in fetch_mempool_tx_rate
MEMPOOL_FEATURE_HISTORY: Dict[Tuple[str, str], list[list[float]]] = {}

# Cap for safety when doing a raw program scan (can be heavy on public RPC)
MAX_PROGRAM_SCAN_ACCOUNTS = int(os.getenv("MAX_PROGRAM_SCAN_ACCOUNTS", "2000") or 2000)

# Token accounts (SPL Token program) are 165 bytes. We perform a secondary scan using this
# size to retain the historical "token account" discovery path while still filtering the
# dataset aggressively enough to keep the RPC payload bounded.
TOKEN_ACCOUNT_DATA_SIZE = int(os.getenv("TOKEN_ACCOUNT_DATA_SIZE", "165") or 165)

# Mint accounts in the SPL Token program have a fixed data size of 82 bytes. Fetching only
# accounts of this size dramatically reduces the volume of data returned by
# ``getProgramAccounts`` and makes the results relevant for discovery (previously the scan was
# dominated by regular token accounts, most of which were long-lived and unrelated to recent
# launches).
MINT_ACCOUNT_DATA_SIZE = int(os.getenv("MINT_ACCOUNT_DATA_SIZE", "82") or 82)
_MINT_ACCOUNT_FILTERS: list[dict[str, int]] = [
    {"dataSize": MINT_ACCOUNT_DATA_SIZE},
]
_TOKEN_ACCOUNT_FILTERS: list[dict[str, int]] = [
    {"dataSize": TOKEN_ACCOUNT_DATA_SIZE},
]

# Helius-specific pagination settings. Requests default to 1k accounts per page which keeps
# memory bounded while avoiding the "Too many accounts requested" RPC errors.
_HELIUS_GPA_PAGE_LIMIT = int(os.getenv("HELIUS_GPA_PAGE_LIMIT", "1000") or 1000)
_HELIUS_GPA_TIMEOUT = float(os.getenv("HELIUS_GPA_TIMEOUT", "20") or 20.0)


# ---------------------------------------------------------------------------
# Small helpers: safe numeric coercions (squelch “an integer is required”)
# ---------------------------------------------------------------------------

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            s = x.strip()
            if s.lower() in {"", "nan", "none", "null"}:
                return default
            return float(s)
    except Exception:
        pass
    return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        if isinstance(x, bool):
            return int(x)
        if isinstance(x, int):
            return x
        if isinstance(x, float):
            return int(x)
        if isinstance(x, str):
            s = x.strip()
            if s.lower() in {"", "nan", "none", "null"}:
                return default
            return int(float(s))
    except Exception:
        pass
    return default


def _parse_int_sequence(raw: str | None, default: Sequence[int]) -> tuple[int, ...]:
    """Parse a comma-delimited list of ints from ``raw``.

    Keeps ordering, ignores invalid/empty entries, and falls back to ``default``
    when nothing valid is provided.
    """

    values: list[int] = []
    for part in (raw or "").split(","):
        chunk = part.strip()
        if not chunk:
            continue
        try:
            num = int(chunk)
        except Exception:
            continue
        if num > 0 and num not in values:
            values.append(num)
    if values:
        return tuple(values)
    return tuple(default)


_DEFAULT_SIGNATURE_LIMITS: tuple[int, ...] = (250, 120, 60, 30, 15, 8)
_SIGNATURE_SCAN_LIMITS: tuple[int, ...] = _parse_int_sequence(
    os.getenv("SIGNATURE_SCAN_LIMITS"), _DEFAULT_SIGNATURE_LIMITS
)

_RETRYABLE_SIGNATURE_ERROR_SNIPPETS: tuple[str, ...] = (
    "429",
    "rate limit",
    "too many",
    "too many requests",
    "heavy",
    "improve",
    "still failing",
    "timeout",
    "timed out",
    "deadline",
    "gateway",
    "overloaded",
    "busy",
    "temporarily unavailable",
)


def _signature_attempt_limits(initial: int | None = None) -> tuple[int, ...]:
    """Return the list of limits to try when fetching signatures."""

    limits = list(_SIGNATURE_SCAN_LIMITS)
    if initial is not None:
        try:
            init = int(initial)
        except Exception:
            init = 0
        if init > 0:
            if init in limits:
                limits.remove(init)
            limits.insert(0, init)
    return tuple(limits)


def _extract_error_message(exc: Exception) -> str:
    """Human-friendly error message from ``exc`` for logging/retry decisions."""

    if isinstance(exc, RPCException):
        payload = exc.args[0] if exc.args else None
        if isinstance(payload, dict):
            message = payload.get("message")
            if not message:
                data = payload.get("data")
                if isinstance(data, dict):
                    message = data.get("message") or data.get("error")
            if message:
                return str(message)
        return str(exc)
    return str(exc)


def _is_retryable_signature_error(exc: Exception) -> bool:
    if isinstance(exc, requests.RequestException):
        return True
    message = _extract_error_message(exc).lower()
    return any(snippet in message for snippet in _RETRYABLE_SIGNATURE_ERROR_SNIPPETS)


async def _get_signatures_for_address_async(
    client: AsyncClient,
    pubkey: Pubkey,
    *,
    limits: Sequence[int] | None = None,
):
    """Fetch signatures with graceful backoff on provider errors."""

    attempt_limits = tuple(limits or _SIGNATURE_SCAN_LIMITS)
    last_exc: Exception | None = None
    for idx, limit in enumerate(attempt_limits):
        try:
            return await client.get_signatures_for_address(pubkey, limit=limit)
        except Exception as exc:  # pragma: no cover - network variability
            last_exc = exc
            should_retry = _is_retryable_signature_error(exc)
            if should_retry and idx < len(attempt_limits) - 1:
                next_limit = attempt_limits[idx + 1]
                logger.debug(
                    "getSignaturesForAddress(%s) failed with limit=%s: %s; retrying with %s",
                    pubkey,
                    limit,
                    _extract_error_message(exc),
                    next_limit,
                )
                await asyncio.sleep(0)
                continue
            raise
    if last_exc is not None:
        raise last_exc


def _get_signatures_for_address(
    client: Client,
    pubkey: Pubkey,
    *,
    limits: Sequence[int] | None = None,
):
    """Sync variant of ``_get_signatures_for_address_async``."""

    attempt_limits = tuple(limits or _SIGNATURE_SCAN_LIMITS)
    last_exc: Exception | None = None
    for idx, limit in enumerate(attempt_limits):
        try:
            return client.get_signatures_for_address(pubkey, limit=limit)
        except Exception as exc:  # pragma: no cover - network variability
            last_exc = exc
            should_retry = _is_retryable_signature_error(exc)
            if should_retry and idx < len(attempt_limits) - 1:
                next_limit = attempt_limits[idx + 1]
                logger.debug(
                    "getSignaturesForAddress(%s) failed with limit=%s: %s; retrying with %s",
                    pubkey,
                    limit,
                    _extract_error_message(exc),
                    next_limit,
                )
                continue
            raise
    if last_exc is not None:
        raise last_exc
def _to_pubkey(token: str | Pubkey) -> Pubkey:
    if isinstance(token, Pubkey):
        return token
    if isinstance(token, bytes):
        token = token.decode("utf-8", "ignore")
    text = str(token).strip()
    if not text:
        raise ValueError("token is required")
    return Pubkey.from_string(text)


def _is_helius_url(rpc_url: str) -> bool:
    return "helius" in (rpc_url or "").lower()


def _should_backoff_heavy_scan(rpc_url: str) -> bool:
    if not _is_helius_url(rpc_url):
        return False
    if _HEAVY_SCAN_BACKOFF.get(rpc_url):
        logger.debug(
            "Skipping getProgramAccounts on %s due to recent provider backoff", rpc_url
        )
        return True
    return False


def _is_provider_plan_error(exc: Exception) -> bool:
    message = str(exc).lower()
    plan_markers = (
        "compute units usage limit exceeded",
        "compute units limit exceeded",
        "plan requires upgrade",
        "access denied",
        "forbidden",
        "not permitted",
        "429",
        "too many requests",
        "heavy request",
    )
    return any(marker in message for marker in plan_markers)


async def _perform_program_scan(
    rpc_url: str,
    *,
    filters: Iterable[Dict[str, Any]] | None,
    data_size: int | None,
    limit: int,
) -> tuple[Dict[str, Any] | None, bool]:
    """Run a capped ``getProgramAccounts`` request with defensive error handling."""

    delays = [0.0, 1.0, 2.0]
    last_exc: Exception | None = None

    for attempt in range(len(delays)):
        try:
            if _is_helius_url(rpc_url):
                resp = await asyncio.to_thread(
                    _fetch_program_accounts_v2_helius,
                    rpc_url,
                    TOKEN_PROGRAM_ID,
                    limit,
                    filters=filters,
                )
                _HEAVY_SCAN_BACKOFF.pop(rpc_url, None)
            else:
                client = Client(rpc_url)
                kwargs: Dict[str, Any] = {"encoding": "jsonParsed"}
                if data_size is not None:
                    kwargs["data_size"] = data_size
                if filters:
                    kwargs["filters"] = list(filters)
                resp = await asyncio.to_thread(
                    client.get_program_accounts,
                    TOKEN_PROGRAM_ID,
                    **kwargs,
                )
            return resp, False
        except Exception as exc:
            if _is_helius_url(rpc_url) and _is_provider_plan_error(exc):
                _HEAVY_SCAN_BACKOFF.set(rpc_url, True)
                logger.warning(
                    "Helius rejected getProgramAccounts (%s); backing off for %.0fs",
                    exc,
                    _HEAVY_SCAN_BACKOFF_TTL,
                )
                return None, True
            last_exc = exc
            if attempt < len(delays) - 1:
                await asyncio.sleep(delays[attempt + 1])

    if last_exc is not None:
        logger.warning("Program scan failed: %s", last_exc)
    return None, False


def _collect_mint_candidates(resp: Dict[str, Any] | None, limit: int) -> list[str]:
    if not resp or limit <= 0:
        return []

    mints: list[str] = []
    for acc in extract_program_accounts(resp):
        if len(mints) >= limit:
            break
        account = acc.get("account", {})
        data = account.get("data", {}) if isinstance(account, dict) else {}
        parsed = data.get("parsed", {}) if isinstance(data, dict) else {}
        parsed_type = parsed.get("type") if isinstance(parsed, dict) else None
        candidate: str | None = None

        if (
            isinstance(parsed_type, str)
            and parsed_type.lower() == "mint"
            and isinstance(acc.get("pubkey"), str)
        ):
            candidate = acc.get("pubkey")
        else:
            info = parsed.get("info", {}) if isinstance(parsed, dict) else {}
            mint = info.get("mint") if isinstance(info, dict) else None
            if isinstance(mint, str):
                candidate = mint

        if isinstance(candidate, str) and len(candidate) >= 32:
            mints.append(candidate)
            if len(mints) >= limit:
                break

    return mints


# ---------------------------------------------------------------------------
# Optional raw on-chain *discovery* (conservative, numeric-only, capped)
# ---------------------------------------------------------------------------

async def scan_tokens_onchain(
    rpc_url: str,
    *,
    return_metrics: bool = False,
) -> List[str] | List[Dict[str, Any]]:
    """
    Light-weight, *best-effort* discovery of SPL-Token mints using `getProgramAccounts`.
    This is capped and should not be your primary discovery source (BirdEye/WS/mempool
    are preferred). We DO NOT use any name/suffix heuristics here.

    When `return_metrics=True`, basic per-mint liquidity/volume are fetched (still safe).
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    if _should_backoff_heavy_scan(rpc_url):
        return []

    resp_mints, backoff = await _perform_program_scan(
        rpc_url,
        filters=_MINT_ACCOUNT_FILTERS,
        data_size=MINT_ACCOUNT_DATA_SIZE,
        limit=MAX_PROGRAM_SCAN_ACCOUNTS,
    )
    if backoff:
        return []

    mint_candidates = _collect_mint_candidates(resp_mints, MAX_PROGRAM_SCAN_ACCOUNTS)

    remaining = max(0, MAX_PROGRAM_SCAN_ACCOUNTS - len(mint_candidates))
    combined_candidates = list(mint_candidates)

    if remaining > 0:
        resp_tokens, backoff_tokens = await _perform_program_scan(
            rpc_url,
            filters=_TOKEN_ACCOUNT_FILTERS,
            data_size=TOKEN_ACCOUNT_DATA_SIZE,
            limit=remaining,
        )
        if not backoff_tokens:
            token_candidates = _collect_mint_candidates(resp_tokens, remaining)
            combined_candidates.extend(token_candidates)

    # De-dup while preserving order
    seen = set()
    uniq_mints = []
    for m in combined_candidates:
        if m not in seen:
            uniq_mints.append(m)
            seen.add(m)

    if not return_metrics:
        return uniq_mints

    # If metrics were requested, fetch basic liquidity / recent volume safely
    results: List[Dict[str, Any]] = []
    for mint in uniq_mints:
        try:
            liq = fetch_liquidity_onchain(mint, rpc_url)
        except Exception as exc:  # pragma: no cover
            logger.warning("Liquidity fetch failed for %s: %s", mint, exc)
            liq = 0.0
        try:
            vol = fetch_volume_onchain(mint, rpc_url)
        except Exception as exc:  # pragma: no cover
            logger.warning("Volume fetch failed for %s: %s", mint, exc)
            vol = 0.0
        results.append(
            {
                "address": mint,
                "liquidity": _safe_float(liq),
                "volume": _safe_float(vol),
            }
        )
    return results


def _fetch_program_accounts_v2_helius(
    rpc_url: str,
    program_id: Pubkey,
    max_accounts: int,
    *,
    encoding: str = "jsonParsed",
    filters: Iterable[Dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    """Fetch program accounts from Helius using getProgramAccountsV2 pagination."""

    per_page = max(1, min(_HELIUS_GPA_PAGE_LIMIT, max_accounts))
    aggregated: list[Dict[str, Any]] = []
    pagination_key: str | None = None
    previous_key: str | None = None

    with requests.Session() as session:
        while len(aggregated) < max_accounts:
            remaining = max_accounts - len(aggregated)
            limit = max(1, min(per_page, remaining))
            config: Dict[str, Any] = {"encoding": encoding, "limit": limit}
            if filters:
                config["filters"] = list(filters)
            if pagination_key:
                config["paginationKey"] = pagination_key

            payload = {
                "jsonrpc": "2.0",
                "id": "solhunter-program-scan",
                "method": "getProgramAccountsV2",
                "params": [str(program_id), config],
            }

            try:
                response = session.post(rpc_url, json=payload, timeout=_HELIUS_GPA_TIMEOUT)
                response.raise_for_status()
            except requests.RequestException as exc:  # pragma: no cover - network issues
                raise RuntimeError(f"Helius getProgramAccountsV2 failed: {exc}") from exc

            try:
                data = response.json()
            except ValueError as exc:  # pragma: no cover - unexpected provider payload
                raise RuntimeError("Helius getProgramAccountsV2 returned non-JSON response") from exc

            if not isinstance(data, dict):
                raise RuntimeError("Helius getProgramAccountsV2 returned unexpected payload")

            if "error" in data:
                error = data["error"]
                message = None
                if isinstance(error, dict):
                    message = error.get("message") or error.get("data")
                raise RuntimeError(f"Helius getProgramAccountsV2 error: {message or error}")

            result = data.get("result")
            if not isinstance(result, dict):
                break

            accounts = result.get("accounts")
            if not isinstance(accounts, list):
                accounts = []

            for entry in accounts:
                if isinstance(entry, dict):
                    aggregated.append(entry)
                    if len(aggregated) >= max_accounts:
                        break

            next_key = result.get("paginationKey")
            if not isinstance(next_key, str) or not accounts:
                break

            if next_key == previous_key:
                break

            previous_key = pagination_key = next_key

    return {"result": {"value": aggregated}}


def scan_tokens_onchain_sync(
    rpc_url: str,
    *,
    return_metrics: bool = False,
) -> List[str] | List[Dict[str, Any]]:
    """Synchronous wrapper."""
    return asyncio.run(scan_tokens_onchain(rpc_url, return_metrics=return_metrics))


# ---------------------------------------------------------------------------
# Liquidity / volume metrics (defensive parsing)
# ---------------------------------------------------------------------------

async def fetch_liquidity_onchain_async(token: str, rpc_url: str) -> float:
    """Sum balances from `getTokenLargestAccounts` as a proxy for liquidity."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    async with AsyncClient(rpc_url) as client:
        try:
            resp = await client.get_token_largest_accounts(_to_pubkey(token))
            accounts = extract_token_accounts(resp)
            total = 0.0
            for acc in accounts:
                # Prefer uiAmount, fall back to amount
                val = acc.get("uiAmount", acc.get("amount", 0))
                total += _safe_float(val, 0.0)
            return total
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to fetch liquidity for %s: %s", token, exc)
            return 0.0


def fetch_liquidity_onchain(token: str, rpc_url: str) -> float:
    """Sync wrapper for liquidity."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    client = Client(rpc_url)
    try:
        resp = client.get_token_largest_accounts(_to_pubkey(token))
        accounts = extract_token_accounts(resp)
        total = 0.0
        for acc in accounts:
            val = acc.get("uiAmount", acc.get("amount", 0))
            total += _safe_float(val, 0.0)
        return total
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch liquidity for %s: %s", token, exc)
        return 0.0


async def fetch_volume_onchain_async(token: str, rpc_url: str) -> float:
    """Approximate recent tx volume from signature entries (best-effort)."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    async with AsyncClient(rpc_url) as client:
        try:
            pubkey = _to_pubkey(token)
            resp = await _get_signatures_for_address_async(client, pubkey)
            entries = extract_signature_entries(resp)
            return _tx_volume(entries)
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to fetch volume for %s: %s", token, exc)
            return 0.0


def fetch_volume_onchain(token: str, rpc_url: str) -> float:
    """Sync wrapper for recent tx volume."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    client = Client(rpc_url)
    try:
        pubkey = _to_pubkey(token)
        resp = _get_signatures_for_address(client, pubkey)
        entries = extract_signature_entries(resp)
        return _tx_volume(entries)
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch volume for %s: %s", token, exc)
        return 0.0


def _tx_volume(entries: Iterable[dict]) -> float:
    """Sum any available 'amount' fields in signature entries safely."""
    total = 0.0
    for e in entries:
        total += _safe_float(e.get("amount"), 0.0)
    return total


# ---------------------------------------------------------------------------
# Mempool / whale / swap metrics (defensive, cached)
# ---------------------------------------------------------------------------

def fetch_mempool_tx_rate(token: str, rpc_url: str, limit: int = 20) -> float:
    """
    Approximate mempool tx *rate* (tx/sec) for `token` from signature timestamps.
    Safe against malformed timestamps; cached to avoid hammering the RPC.
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    cache_key = (token, rpc_url)
    cached = MEMPOOL_RATE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = Client(rpc_url)
    try:
        pubkey = _to_pubkey(token)
        limits = _signature_attempt_limits(limit)
        resp = _get_signatures_for_address(client, pubkey, limits=limits)
        entries = extract_signature_entries(resp)
        times = [_safe_int(e.get("blockTime")) for e in entries if e.get("blockTime") is not None]
        times = [t for t in times if t > 0]
        if len(times) >= 2:
            duration = max(times) - min(times)
            rate = (len(times) / float(duration)) if duration > 0 else float(len(times))
        else:
            rate = float(len(times))
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch mempool rate for %s: %s", token, exc)
        rate = 0.0

    # Optional feature collection for forecasting
    features = []
    try:
        depth_change = 0.0
        try:
            from . import onchain_metrics  # late import to avoid cycles
            depth_change = onchain_metrics.order_book_depth_change(token)
        except Exception:
            depth_change = 0.0

        whale = fetch_whale_wallet_activity(token, rpc_url)
        avg_swap = fetch_average_swap_size(token, rpc_url)
        features = [_safe_float(depth_change), _safe_float(rate), _safe_float(whale), _safe_float(avg_swap)]
    except Exception:
        features = [0.0, _safe_float(rate), 0.0, 0.0]

    hist = MEMPOOL_FEATURE_HISTORY.setdefault(cache_key, [])
    hist.append(features)

    # Optional tiny forecaster hook
    model_path = os.getenv("ONCHAIN_MODEL_PATH")
    if model_path:
        try:
            from .models.onchain_forecaster import get_model  # type: ignore
            model = get_model(model_path)
            if model is not None:
                seq_len = getattr(model, "seq_len", 30)
                if len(hist) >= seq_len:
                    seq = hist[-seq_len:]
                    try:
                        rate = _safe_float(model.predict(seq), _safe_float(rate))
                    except Exception as exc:  # pragma: no cover
                        logger.warning("Forecast failed: %s", exc)
        except Exception:
            # forecasting is optional; ignore entirely if anything is off
            pass

    # Keep history bounded
    try:
        hist[:] = hist[-30:]
    except Exception:
        pass

    MEMPOOL_RATE_CACHE.set(cache_key, _safe_float(rate))
    return _safe_float(rate)


def fetch_whale_wallet_activity(
    token: str,
    rpc_url: str,
    threshold: float = 1_000_000.0,
) -> float:
    """
    Fraction of supply held by large accounts (>= threshold).
    Uses `getTokenLargestAccounts`; fully defensive/coerced.
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    cache_key = (token, rpc_url)
    cached = WHALE_ACTIVITY_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = Client(rpc_url)
    try:
        resp = client.get_token_largest_accounts(_to_pubkey(token))
        accounts = extract_token_accounts(resp)
        total = 0.0
        whales = 0.0
        thr = _safe_float(threshold, 0.0)
        for acc in accounts:
            bal = _safe_float(acc.get("uiAmount", acc.get("amount", 0.0)), 0.0)
            total += bal
            if bal >= thr:
                whales += bal
        activity = (whales / total) if total > 0 else 0.0
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch whale activity for %s: %s", token, exc)
        activity = 0.0

    WHALE_ACTIVITY_CACHE.set(cache_key, _safe_float(activity))
    return _safe_float(activity)


def fetch_average_swap_size(token: str, rpc_url: str, limit: int = 20) -> float:
    """
    Average swap 'amount' inferred from recent signatures (best-effort).
    Graceful on missing/dirty fields; cached.
    """
    if not rpc_url:
        raise ValueError("rpc_url is required")

    cache_key = (token, rpc_url)
    cached = AVG_SWAP_SIZE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    client = Client(rpc_url)
    try:
        pubkey = _to_pubkey(token)
        limits = _signature_attempt_limits(limit)
        resp = _get_signatures_for_address(client, pubkey, limits=limits)
        entries = extract_signature_entries(resp)
        total = 0.0
        count = 0
        for e in entries:
            amt = _safe_float(e.get("amount"), 0.0)
            if amt > 0:
                total += amt
                count += 1
        size = (total / float(count)) if count else 0.0
    except Exception as exc:  # pragma: no cover
        logger.warning("Failed to fetch swap size for %s: %s", token, exc)
        size = 0.0

    AVG_SWAP_SIZE_CACHE.set(cache_key, _safe_float(size))
    return _safe_float(size)
