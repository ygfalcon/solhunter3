import asyncio
import os
import time
from statistics import median
from typing import Callable, Optional

from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient

RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY")
RPC_TESTNET_URL = os.getenv(
    "SOLANA_TESTNET_RPC_URL",
    "https://devnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY",
)

LAMPORTS_PER_SOL = 1_000_000_000

_FALLBACK_LAMPORTS = int(os.getenv("FALLBACK_LAMPORTS_PER_SIG", "5000") or 5000)
_MIN_CU_PRICE = int(os.getenv("MIN_CU_PRICE_LAMPORTS", "0") or 0)
_MAX_CU_PRICE = int(os.getenv("MAX_CU_PRICE_LAMPORTS", "10000") or 10000)
_CU_PRICE_SLOPE = float(os.getenv("CU_PRICE_SLOPE", "1000") or 1000.0)
_FEE_CACHE_TTL = float(os.getenv("FEE_CACHE_TTL_SECONDS", "5") or 5.0)
_PRIORITY_CACHE_TTL = float(os.getenv("PRIORITY_FEE_CACHE_TTL_SECONDS", "3") or 3.0)

_fee_cache = {}
_priority_cache = {}


def _extract_lamports(resp: object) -> int:
    """Return lamports per signature from an RPC response."""

    try:
        value = resp["value"]  # type: ignore[index]
    except Exception:
        value = getattr(resp, "value", None)

    if isinstance(value, dict):
        calc = value.get("feeCalculator") or value.get("fee_calculator") or {}
        lamports = calc.get("lamportsPerSignature") or calc.get(
            "lamports_per_signature"
        )
        if isinstance(lamports, (int, float)) and lamports >= 0:
            return int(lamports)

    try:
        return int(value.fee_calculator.lamports_per_signature)  # type: ignore[attr-defined]
    except Exception:
        return 0


def _best_effort_base_fee(client: Client) -> int:
    """Try multiple RPCs to obtain the current base fee."""

    try:
        resp = client.get_fees()  # type: ignore[attr-defined]
        lamports = _extract_lamports(resp)
        if lamports > 0:
            return lamports
    except Exception:
        pass

    try:
        resp = client.get_recent_blockhash()  # type: ignore[attr-defined]
        lamports = _extract_lamports(resp)
        if lamports > 0:
            return lamports
    except Exception:
        pass

    try:
        resp = client.get_latest_blockhash()
        lamports = _extract_lamports(resp)
        if lamports > 0:
            return lamports
    except Exception:
        pass

    return _FALLBACK_LAMPORTS


def _cache_get(cache: dict, key: tuple, ttl: float) -> Optional[float]:
    entry = cache.get(key)
    if not entry:
        return None
    value, timestamp = entry
    if time.monotonic() - timestamp > ttl:
        cache.pop(key, None)
        return None
    return value


def _cache_set(cache: dict, key: tuple, value: float) -> None:
    cache[key] = (value, time.monotonic())


def get_current_fee(testnet: bool = False) -> float:
    """Return current fee per signature in SOL."""

    cache_key = ("current_fee", testnet)
    cached = _cache_get(_fee_cache, cache_key, _FEE_CACHE_TTL)
    if cached is not None:
        return cached

    client = Client(RPC_TESTNET_URL if testnet else RPC_URL)
    lamports = _best_effort_base_fee(client)
    fee = lamports / LAMPORTS_PER_SOL
    _cache_set(_fee_cache, cache_key, fee)
    return fee


async def get_current_fee_async(testnet: bool = False) -> float:
    """Asynchronously return current fee per signature in SOL."""

    cache_key = ("current_fee", testnet)
    cached = _cache_get(_fee_cache, cache_key, _FEE_CACHE_TTL)
    if cached is not None:
        return cached

    async with AsyncClient(RPC_TESTNET_URL if testnet else RPC_URL) as client:
        try:
            resp = await client._provider.make_request("getFees", [])  # type: ignore[attr-defined]
            lamports = _extract_lamports({"value": resp.get("result", {}).get("value", {})})
        except Exception:
            lamports = 0

        if lamports <= 0:
            try:
                rb = await client.get_recent_blockhash()  # type: ignore[attr-defined]
                lamports = _extract_lamports(rb)
            except Exception:
                lamports = 0

        if lamports <= 0:
            try:
                lb = await client.get_latest_blockhash()
                lamports = _extract_lamports(lb)
            except Exception:
                lamports = 0

    if lamports <= 0:
        lamports = _FALLBACK_LAMPORTS

    fee = lamports / LAMPORTS_PER_SOL
    _cache_set(_fee_cache, cache_key, fee)
    return fee


async def get_priority_fee_async(rpc_url: str, *, percentile: float = 0.5) -> float:
    """Return a percentile of prioritization fee in SOL from ``rpc_url``."""

    async with AsyncClient(rpc_url) as client:
        try:
            resp = await client._provider.make_request("getRecentPrioritizationFees", [])
            arr = resp.get("result") or []
            if not isinstance(arr, list) or not arr:
                return 0.0

            vals = [int(x.get("prioritizationFee", 0)) for x in arr if isinstance(x, dict)]
            vals = [v for v in vals if v > 0]
            if not vals:
                return 0.0

            vals.sort()
            idx = min(len(vals) - 1, max(0, int(round(percentile * (len(vals) - 1)))))
            lamports = vals[idx]
        except Exception:
            lamports = 0
    return lamports / LAMPORTS_PER_SOL


async def get_priority_fee_estimate(
    rpc_urls: list[str],
    *,
    percentile: float = 0.5,
    reduce: Optional[Callable[[list[float]], float]] = None,
) -> float:
    """Query all URLs concurrently and return an aggregate estimate."""

    if not rpc_urls:
        return 0.0

    use_cache = reduce is None
    cache_key = (tuple(sorted(rpc_urls)), percentile)
    if use_cache:
        cached = _cache_get(_priority_cache, cache_key, _PRIORITY_CACHE_TTL)
        if cached is not None:
            return cached

    tasks = [get_priority_fee_async(u, percentile=percentile) for u in rpc_urls]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    fees = [r for r in results if isinstance(r, (int, float)) and r > 0]
    if not fees:
        return 0.0

    estimate = float(reduce(fees)) if reduce else float(median(fees))
    if use_cache:
        _cache_set(_priority_cache, cache_key, estimate)
    return estimate


def adjust_priority_fee(tx_rate: float) -> int:
    """Compute-unit price (lamports) from tx/s, clamped by env-configured bounds."""

    if tx_rate <= 0:
        return 0

    price = int(tx_rate * _CU_PRICE_SLOPE)
    return max(_MIN_CU_PRICE, min(price, _MAX_CU_PRICE))
