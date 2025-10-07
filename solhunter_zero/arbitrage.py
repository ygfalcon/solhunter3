"""Arbitrage utilities and helpers.

Latency measurements now run concurrently using :func:`asyncio.gather`. With
dynamic concurrency enabled this refreshes endpoint latency around 30-50% faster
when multiple URLs are checked.

The module historically doubled as both the low-level arbitrage helper library
and the concrete :class:`~solhunter_zero.agents.arbitrage.ArbitrageAgent`
implementation.  Some environments still resolve the agent class through this
module, so we lazily re-export it via :func:`__getattr__` below to keep those
imports working.
"""

import asyncio
import importlib
import logging
import os

from .system import detect_cpu_count, set_rayon_threads

# Configure Rayon parallelism for the Rust FFI
set_rayon_threads()
from .http import get_session, loads, dumps
import heapq
import time
from typing import List
import numpy as np
import contextlib

from .dynamic_limit import _target_concurrency, _step_limit
from . import resource_monitor
from .util import parse_bool_env
from .onchain_metrics import _helius_price_overview, _birdeye_price_overview

try:  # pragma: no cover - optional dependency
    from numba import njit as _numba_njit

    _HAS_NUMBA = True
except Exception:  # pragma: no cover - numba not available
    _HAS_NUMBA = False

    def njit(*args, **kwargs):  # type: ignore
        if args and callable(args[0]):
            return args[0]

        def wrapper(func):
            return func

        return wrapper

else:  # pragma: no cover - numba available

    def njit(*args, **kwargs):  # type: ignore
        def decorator(func):
            try:
                return _numba_njit(*args, **kwargs)(func)
            except Exception:
                return func

        if args and callable(args[0]):
            return decorator(args[0])

        return decorator


from typing import (
    Callable,
    Awaitable,
    Sequence,
    Tuple,
    Optional,
    AsyncGenerator,
    Mapping,
)

logger = logging.getLogger(__name__)

try:  # optional rust ffi
    from . import routeffi as _routeffi

    _HAS_ROUTEFFI = _routeffi.is_routeffi_available()
    _HAS_PARALLEL = _routeffi.parallel_enabled()
    if not _HAS_ROUTEFFI:
        logger.warning(
            "Route FFI library not loaded; falling back to Python routing."
            " Set ROUTE_FFI_LIB or place the library alongside the package to enable it."
        )
except Exception:  # pragma: no cover - ffi unavailable
    _HAS_ROUTEFFI = False
    _HAS_PARALLEL = False
    logger.warning("Failed to import routeffi; using Python fallback.")

import aiohttp
from .scanner_common import JUPITER_WS_URL

from .exchange import (
    place_order_async,
    ORCA_DEX_URL,
    RAYDIUM_DEX_URL,
    DEX_BASE_URL,
    VENUE_URLS,
    resolve_swap_endpoint,
)
from .config import load_dex_config
from . import order_book_ws
from . import depth_client
from .depth_client import stream_depth, prepare_signed_tx
from .execution import EventExecutor
from .flash_loans import borrow_flash, repay_flash
from solders.instruction import Instruction
from solders.pubkey import Pubkey
from solders.keypair import Keypair

# rate limit for depth streams (seconds between updates)
DEPTH_RATE_LIMIT = 0.1

DEPTH_SERVICE_SOCKET = os.getenv("DEPTH_SERVICE_SOCKET", "/tmp/depth_service.sock")

PriceFeed = Callable[[str], Awaitable[float]]


def _load_arbitrage_agent():
    """Return the :class:`ArbitrageAgent` without import-time recursion."""

    module = importlib.import_module(".agents.arbitrage", __package__)
    return getattr(module, "ArbitrageAgent")


# Default API endpoints for direct price queries
ORCA_API_URL = os.getenv("ORCA_API_URL", "https://api.orca.so")
RAYDIUM_API_URL = os.getenv("RAYDIUM_API_URL", "https://api.raydium.io")
PHOENIX_API_URL = os.getenv("PHOENIX_API_URL", "https://api.phoenix.trade")
METEORA_API_URL = os.getenv("METEORA_API_URL", "https://api.meteora.ag")
ORCA_WS_URL = os.getenv("ORCA_WS_URL", "")
RAYDIUM_WS_URL = os.getenv("RAYDIUM_WS_URL", "")
PHOENIX_WS_URL = os.getenv("PHOENIX_WS_URL", "")
METEORA_WS_URL = os.getenv("METEORA_WS_URL", "")
DEX_PRIORITIES = [
    n.strip()
    for n in os.getenv(
        "DEX_PRIORITIES",
        "service,phoenix,meteora,orca,raydium,jupiter",
    )
    .replace(";", ",")
    .split(",")
    if n.strip()
]

USE_DEPTH_STREAM = parse_bool_env("USE_DEPTH_STREAM", True)
USE_SERVICE_EXEC = parse_bool_env("USE_SERVICE_EXEC", True)
USE_SERVICE_ROUTE = parse_bool_env("USE_SERVICE_ROUTE", True)

_ffi_env = os.getenv("USE_FFI_ROUTE")
if _ffi_env is not None:
    USE_FFI_ROUTE = _ffi_env.strip().lower() not in {"0", "false", "no"}
else:
    try:  # prefer the FFI path when available
        USE_FFI_ROUTE = _routeffi.is_routeffi_available()
    except Exception:
        USE_FFI_ROUTE = False


def __getattr__(name: str):
    if name == "ArbitrageAgent":  # pragma: no cover - import guard
        return _load_arbitrage_agent()
    raise AttributeError(name)


def __dir__() -> List[str]:  # pragma: no cover - convenience for REPLs
    return sorted({*globals(), "ArbitrageAgent"})


def _parse_mapping_env(env: str) -> dict:
    """Return dictionary from ``env`` or an empty mapping."""
    val = os.getenv(env)
    if not val:
        return {}
    try:
        data = loads(val)
    except Exception:
        try:
            import ast

            data = ast.literal_eval(val)
        except Exception:
            return {}
    if isinstance(data, dict):
        return data
    return {}


_DEX_CFG = load_dex_config()
DEX_FEES = _DEX_CFG.fees
DEX_GAS = _DEX_CFG.gas
DEX_LATENCY = _DEX_CFG.latency
EXTRA_API_URLS = {str(k): str(v) for k, v in _parse_mapping_env("DEX_API_URLS").items()}
EXTRA_WS_URLS = {str(k): str(v) for k, v in _parse_mapping_env("DEX_WS_URLS").items()}

# Measure DEX latency on import unless disabled via environment variable
MEASURE_DEX_LATENCY = parse_bool_env("MEASURE_DEX_LATENCY", True)

# Interval in seconds between latency refreshes
DEX_LATENCY_REFRESH_INTERVAL = float(
    os.getenv("DEX_LATENCY_REFRESH_INTERVAL", "60") or 60
)

# Interval in seconds between dynamic concurrency adjustments
_DYN_INTERVAL: float = 2.0


async def _ping_url(
    session: aiohttp.ClientSession, url: str, attempts: int = 3
) -> float:
    """Return the average latency for ``url`` in seconds."""

    async def _once() -> float | None:
        start = time.perf_counter()
        try:
            if url.startswith("ws"):
                async with session.ws_connect(url, timeout=5):
                    pass
            else:
                async with session.get(url, timeout=5) as resp:
                    await resp.read()
        except Exception:  # pragma: no cover - network failures
            return None
        return time.perf_counter() - start

    coros = [_once() for _ in range(max(1, attempts))]
    results = await asyncio.gather(*coros, return_exceptions=True)
    times = [t for t in results if isinstance(t, (int, float))]
    if times:
        return sum(times) / len(times)
    return 0.0


async def measure_dex_latency_async(
    urls: Mapping[str, str],
    attempts: int = 3,
    *,
    max_concurrency: int | None = None,
    dynamic_concurrency: bool = False,
) -> dict[str, float]:
    """Asynchronously measure latency for each URL in ``urls``."""

    cache_key = (tuple(sorted(urls.items())), attempts)
    cached = DEX_LATENCY_CACHE.get(cache_key)
    if cached is not None:
        return cached

    if _routeffi.is_routeffi_available():
        func = getattr(_routeffi, "measure_latency", None)
        if callable(func):
            try:
                loop = asyncio.get_running_loop()
                res = await loop.run_in_executor(
                    None, lambda: func(dict(urls), attempts)
                )
                if res:
                    DEX_LATENCY_CACHE.set(cache_key, res)
                    return res
            except Exception as exc:  # pragma: no cover - optional ffi failures
                logger.debug("ffi.measure_latency failed: %s", exc)

    if max_concurrency is None or max_concurrency <= 0:
        max_concurrency = max(detect_cpu_count(), len(urls))

    class _Noop:
        async def __aenter__(self):
            return None

        async def __aexit__(self, exc_type, exc, tb):
            pass

    use_sem = dynamic_concurrency or max_concurrency < len(urls)
    sem: asyncio.Semaphore | _Noop
    if use_sem:
        sem = asyncio.Semaphore(max_concurrency)
    else:
        sem = _Noop()
    current_limit = max_concurrency
    _dyn_interval = float(
        os.getenv("DYNAMIC_CONCURRENCY_INTERVAL", str(_DYN_INTERVAL)) or _DYN_INTERVAL
    )
    ewm = float(os.getenv("CONCURRENCY_EWM_SMOOTHING", "0.15") or 0.15)
    kp = float(
        os.getenv("CONCURRENCY_SMOOTHING", os.getenv("CONCURRENCY_KP", "0.5")) or 0.5
    )
    ki = float(os.getenv("CONCURRENCY_KI", "0.0") or 0.0)
    high = float(os.getenv("CPU_HIGH_THRESHOLD", "80") or 80)
    low = float(os.getenv("CPU_LOW_THRESHOLD", "40") or 40)
    adjust_task: asyncio.Task | None = None

    async def _set_limit(new_limit: int) -> None:
        nonlocal current_limit
        diff = new_limit - current_limit
        if diff > 0:
            for _ in range(diff):
                sem.release()
        elif diff < 0:
            for _ in range(-diff):
                await sem.acquire()
        current_limit = new_limit

    if dynamic_concurrency:

        async def _adjust() -> None:
            try:
                while True:
                    await asyncio.sleep(_dyn_interval)
                    cpu = resource_monitor.get_cpu_usage()
                    target = _target_concurrency(
                        cpu, max_concurrency, low, high, smoothing=ewm
                    )
                    new_limit = _step_limit(
                        current_limit,
                        target,
                        max_concurrency,
                        smoothing=kp,
                        ki=ki,
                    )
                    if new_limit != current_limit:
                        await _set_limit(new_limit)
            except asyncio.CancelledError:
                pass

        adjust_task = asyncio.create_task(_adjust())
        await asyncio.sleep(0)

    session = await get_session()

    async def _measure(name: str, url: str) -> tuple[str, float]:
        async with sem:
            value = await _ping_url(session, url, attempts)
        return name, value

    coros = [_measure(n, u) for n, u in urls.items() if u]
    results = await asyncio.gather(*coros, return_exceptions=True)
    latency = {}
    for res in results:
        if isinstance(res, tuple):
            n, v = res
            latency[n] = v
    if adjust_task:
        adjust_task.cancel()
        with contextlib.suppress(Exception):
            await adjust_task
    DEX_LATENCY_CACHE.set(cache_key, latency)
    return latency


def measure_dex_latency(
    urls: Mapping[str, str] | None = None, attempts: int = 3
) -> dict[str, float]:
    """Synchronously measure latency for DEX endpoints.

    Safe to call from within an event loop: uses run_coroutine_threadsafe
    if a loop is already running, otherwise uses asyncio.run.
    """

    if urls is None:
        urls = {**VENUE_URLS, **EXTRA_API_URLS, **EXTRA_WS_URLS}
        ws_map = {
            "orca": ORCA_WS_URL,
            "raydium": RAYDIUM_WS_URL,
            "phoenix": PHOENIX_WS_URL,
            "meteora": METEORA_WS_URL,
            "jupiter": JUPITER_WS_URL,
        }
        for name, url in ws_map.items():
            if url:
                urls.setdefault(name, url)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop and loop.is_running():
        fut = asyncio.run_coroutine_threadsafe(
            measure_dex_latency_async(urls, attempts), loop
        )
        try:
            return fut.result(timeout=10)
        except Exception:  # pragma: no cover - timeout or loop errors
            return {}
    return asyncio.run(measure_dex_latency_async(urls, attempts))


if MEASURE_DEX_LATENCY:
    try:
        DEX_LATENCY.update(measure_dex_latency())
    except Exception as exc:  # pragma: no cover - measurement failures
        logger.debug("DEX latency measurement failed: %s", exc)

_LATENCY_TASK: asyncio.Task | None = None


async def _latency_loop(interval: float) -> None:
    """Background latency measurement loop."""
    try:
        while True:
            res = await measure_dex_latency_async(VENUE_URLS, dynamic_concurrency=True)
            if res:
                DEX_LATENCY.update(res)
                publish("dex_latency_update", res)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:  # pragma: no cover - cancellation
        pass


def start_latency_refresh(
    interval: float = DEX_LATENCY_REFRESH_INTERVAL,
) -> asyncio.Task:
    """Start periodic DEX latency refresh task."""
    global _LATENCY_TASK
    if _LATENCY_TASK is None or _LATENCY_TASK.done():
        loop = asyncio.get_running_loop()
        _LATENCY_TASK = loop.create_task(_latency_loop(interval))
    return _LATENCY_TASK


def stop_latency_refresh() -> None:
    """Cancel the running latency refresh task, if any."""
    global _LATENCY_TASK
    if _LATENCY_TASK is not None:
        _LATENCY_TASK.cancel()
        _LATENCY_TASK = None


# Flash loan configuration
USE_FLASH_LOANS = parse_bool_env("USE_FLASH_LOANS", False)
MAX_FLASH_AMOUNT = float(os.getenv("MAX_FLASH_AMOUNT", "0") or 0)
FLASH_LOAN_RATIO = float(os.getenv("FLASH_LOAN_RATIO", "0") or 0)
USE_MEV_BUNDLES = parse_bool_env("USE_MEV_BUNDLES", False)

# Path search configuration
MAX_HOPS = int(os.getenv("MAX_HOPS", "3") or 3)
PATH_ALGORITHM = os.getenv("PATH_ALGORITHM", "graph")
USE_NUMBA_ROUTE = parse_bool_env("USE_NUMBA_ROUTE", False)
USE_GNN_ROUTING = parse_bool_env("USE_GNN_ROUTING", False)
GNN_MODEL_PATH = os.getenv("GNN_MODEL_PATH", "route_gnn.pt")
ROUTE_GENERATOR_PATH = os.getenv("ROUTE_GENERATOR_PATH", "route_generator.pt")

from solhunter_zero.lru import LRUCache, TTLCache
from .event_bus import subscribe, publish
from .prices import get_cached_price, update_price_cache

ROUTE_CACHE = LRUCache(maxsize=128)
EDGE_CACHE_TTL = float(os.getenv("EDGE_CACHE_TTL", "60") or 60)
_EDGE_CACHE = TTLCache(maxsize=1024, ttl=EDGE_CACHE_TTL)
_LAST_DEPTH: dict[str, float] = {}

# shared HTTP session and price cache
PRICE_CACHE_TTL = float(os.getenv("PRICE_CACHE_TTL", "30") or 30)
PRICE_CACHE = TTLCache(maxsize=256, ttl=PRICE_CACHE_TTL)
DEX_LATENCY_CACHE_TTL = float(os.getenv("DEX_LATENCY_CACHE_TTL", "30") or 30)
DEX_LATENCY_CACHE = TTLCache(maxsize=64, ttl=DEX_LATENCY_CACHE_TTL)
MEMPOOL_WEIGHT = float(os.getenv("MEMPOOL_WEIGHT", "0.0001") or 0.0001)


def _route_key(
    token: str,
    amount: float,
    fees: Mapping[str, float],
    gas: Mapping[str, float],
    latency: Mapping[str, float],
) -> tuple:
    def _norm(m: Mapping[str, float]) -> tuple:
        return tuple(sorted((k, float(v)) for k, v in m.items()))

    return (token, float(amount), _norm(fees), _norm(gas), _norm(latency))


def invalidate_route(token: str | None = None) -> None:
    """Remove cached paths for ``token`` or clear the cache."""
    if token is None:
        ROUTE_CACHE.clear()
        return
    keys = [k for k in ROUTE_CACHE.keys() if k[0] == token]
    for k in keys:
        ROUTE_CACHE.pop(k, None)


def invalidate_edges(token: str | None = None) -> None:
    """Remove cached adjacency data for ``token`` or clear the cache."""
    if token is None:
        _EDGE_CACHE.clear()
        return
    _EDGE_CACHE.pop(token, None)


def _on_depth_update(payload: Mapping[str, Mapping[str, float]]) -> None:
    for token, entry in payload.items():
        depth = float(entry.get("depth", 0.0))
        last = _LAST_DEPTH.get(token)
        if last is not None:
            base = max(depth, last, 1.0)
            if abs(depth - last) / base > 0.1:
                invalidate_route(token)
                invalidate_edges(token)
        _LAST_DEPTH[token] = depth


subscribe("depth_update", _on_depth_update)


def refresh_costs() -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    """Return updated fee, gas and latency mappings from the environment."""
    cfg = load_dex_config()
    latency = dict(cfg.latency)
    if MEASURE_DEX_LATENCY:
        try:
            latency.update(measure_dex_latency())
        except Exception as exc:  # pragma: no cover - measurement failures
            logger.debug("DEX latency measurement failed: %s", exc)
    global DEX_FEES, DEX_GAS, DEX_LATENCY
    DEX_FEES = cfg.fees
    DEX_GAS = cfg.gas
    DEX_LATENCY = latency
    return DEX_FEES, DEX_GAS, DEX_LATENCY


def _build_adjacency(
    prices: Mapping[str, float],
    trade_amount: float,
    fees: Mapping[str, float],
    gas: Mapping[str, float],
    latency: Mapping[str, float],
    depth: Mapping[str, Mapping[str, float]] | None,
    token: str | None,
    mempool_rate: float = 0.0,
) -> tuple[list[str], dict[str, dict[str, float]]]:
    """Return adjacency map for the search graph with caching."""

    venues = list(prices.keys())
    adj_key = token if token is not None else None
    adjacency: dict[str, dict[str, float]] | None = None
    if adj_key is not None:
        adjacency = _EDGE_CACHE.get(adj_key)
    if adjacency is None:
        service = depth_client.get_adjacency_matrix(token) if token else None
        if service and set(service[0]) == set(venues):
            venues = service[0]
            coeff = np.array(service[1], dtype=float)
            price_arr = np.array([prices[v] for v in venues], dtype=float)
            fee_arr = np.array([fees.get(v, 0.0) for v in venues], dtype=float)
            gas_arr = np.array([gas.get(v, 0.0) for v in venues], dtype=float)
            lat_arr = np.array([latency.get(v, 0.0) for v in venues], dtype=float)
            base_cost = (
                price_arr * trade_amount * fee_arr
                + gas_arr
                + lat_arr
                + mempool_rate * MEMPOOL_WEIGHT
            )
            step_matrix = base_cost[:, None] + base_cost[None, :]
            profit_matrix = coeff * trade_amount - step_matrix
            np.fill_diagonal(profit_matrix, float("-inf"))
            adjacency = {
                a: {
                    b: float(profit_matrix[i, j])
                    for j, b in enumerate(venues)
                    if i != j
                }
                for i, a in enumerate(venues)
            }
        else:
            price_arr = np.array([prices[v] for v in venues], dtype=float)
            fee_arr = np.array([fees.get(v, 0.0) for v in venues], dtype=float)
            gas_arr = np.array([gas.get(v, 0.0) for v in venues], dtype=float)
            lat_arr = np.array([latency.get(v, 0.0) for v in venues], dtype=float)

            base_cost = (
                price_arr * trade_amount * fee_arr
                + gas_arr
                + lat_arr
                + mempool_rate * MEMPOOL_WEIGHT
            )
            step_matrix = base_cost[:, None] + base_cost[None, :]

            if depth is not None:
                ask_arr = np.array(
                    [float((depth.get(v) or {}).get("asks", 0.0)) for v in venues],
                    dtype=float,
                )
                bid_arr = np.array(
                    [float((depth.get(v) or {}).get("bids", 0.0)) for v in venues],
                    dtype=float,
                )
                slip_a = np.where(ask_arr > 0, trade_amount / ask_arr, 0.0)
                slip_b = np.where(bid_arr > 0, trade_amount / bid_arr, 0.0)
                slip_matrix = (
                    price_arr[:, None] * trade_amount * slip_a[:, None]
                    + price_arr[None, :] * trade_amount * slip_b[None, :]
                )
            else:
                slip_matrix = 0.0

            profit_matrix = (
                (price_arr[None, :] - price_arr[:, None]) * trade_amount
                - step_matrix
                - slip_matrix
            )
            np.fill_diagonal(profit_matrix, float("-inf"))

            adjacency = {
                a: {
                    b: float(profit_matrix[i, j])
                    for j, b in enumerate(venues)
                    if i != j
                }
                for i, a in enumerate(venues)
            }
        if adj_key is not None:
            _EDGE_CACHE.set(adj_key, adjacency)

    return venues, adjacency


@njit
def _search_numba(matrix: "float[:, :]", max_hops: int) -> tuple[List[int], float]:
    n = matrix.shape[0]
    best_profit = -1e18
    best_len = 0
    best_path = [0] * max_hops
    path = [0] * max_hops
    visited = [0] * n

    def dfs(last_idx: int, depth: int, profit: float):
        nonlocal best_profit, best_len
        if depth > 1 and profit > best_profit:
            best_profit = profit
            best_len = depth
            for i in range(depth):
                best_path[i] = path[i]
        if depth >= max_hops:
            return
        for j in range(n):
            if visited[j] or j == last_idx:
                continue
            val = matrix[last_idx, j]
            if val == float("-inf"):
                continue
            visited[j] = 1
            path[depth] = j
            dfs(j, depth + 1, profit + val)
            visited[j] = 0

    for i in range(n):
        visited[i] = 1
        path[0] = i
        dfs(i, 1, 0.0)
        visited[i] = 0

    return best_path[:best_len], best_profit


def _list_paths(
    prices: Mapping[str, float],
    amount: float,
    *,
    token: str | None = None,
    fees: Mapping[str, float] | None = None,
    gas: Mapping[str, float] | None = None,
    latency: Mapping[str, float] | None = None,
    depth: Mapping[str, Mapping[str, float]] | None = None,
    mempool_rate: float = 0.0,
    use_flash_loans: bool | None = None,
    max_flash_amount: float | None = None,
    max_hops: int | None = None,
    top_k: int = 5,
) -> list[tuple[list[str], float]]:
    """Return top ``top_k`` paths ranked by computed profit."""

    fees = fees or {}
    gas = gas or {}
    latency = latency or {}
    for v in prices.keys():
        fees.setdefault(v, DEX_FEES.get(v, 0.0))
        gas.setdefault(v, DEX_GAS.get(v, 0.0))
        latency.setdefault(v, DEX_LATENCY.get(v, 0.0))
    if use_flash_loans is None:
        use_flash_loans = USE_FLASH_LOANS
    if max_flash_amount is None:
        max_flash_amount = MAX_FLASH_AMOUNT
    if max_hops is None:
        max_hops = MAX_HOPS

    trade_amount = (
        min(max_flash_amount or amount, amount) if use_flash_loans else amount
    )

    venues, adjacency = _build_adjacency(
        prices,
        trade_amount,
        fees,
        gas,
        latency,
        depth,
        token,
        mempool_rate,
    )

    heap: list[tuple[float, list[str], set[str]]] = []
    for v in venues:
        heapq.heappush(heap, (0.0, [v], {v}))

    paths: list[tuple[list[str], float]] = []
    while heap:
        neg_profit, path, visited = heapq.heappop(heap)
        profit = -neg_profit
        if len(path) > 1:
            paths.append((path, profit))
        if len(path) >= max_hops:
            continue
        last = path[-1]
        for nxt, val in adjacency.get(last, {}).items():
            if nxt in visited:
                continue
            new_profit = profit + val
            heapq.heappush(heap, (-new_profit, path + [nxt], visited | {nxt}))

    paths.sort(key=lambda p: p[1], reverse=True)
    return paths[:top_k]


async def _prepare_service_tx(
    token: str,
    side: str,
    amount: float,
    price: float,
    base_url: str,
) -> str | None:
    payload = {
        "token": token,
        "side": side,
        "amount": amount,
        "price": price,
        "cluster": "mainnet-beta",
    }
    session = await get_session()
    endpoint = resolve_swap_endpoint(base_url)
    try:
        async with session.post(endpoint, json=payload, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
    except aiohttp.ClientError:
        return None

    tx_b64 = data.get("swapTransaction")
    if not tx_b64:
        return None
    return await prepare_signed_tx(tx_b64)


async def fetch_orca_price_async(token: str) -> float:
    """Return the current price for ``token`` from the Orca API."""
    cached = get_cached_price(token)
    if cached is not None:
        PRICE_CACHE.set(("orca", token), cached)
        return cached

    cached = PRICE_CACHE.get(("orca", token))
    if cached is not None:
        update_price_cache(token, cached)
        return cached

    url = f"{ORCA_API_URL}/price?token={token}"
    session = await get_session()
    try:
        async with session.get(url, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            price = data.get("price")
            value = float(price) if isinstance(price, (int, float)) else 0.0
    except aiohttp.ClientError as exc:  # pragma: no cover - network errors
        logger.warning("Failed to fetch price from Orca: %s", exc)
        return 0.0

    PRICE_CACHE.set(("orca", token), value)
    update_price_cache(token, value)
    return value


async def fetch_raydium_price_async(token: str) -> float:
    """Return the current price for ``token`` from the Raydium API."""
    cached = get_cached_price(token)
    if cached is not None:
        PRICE_CACHE.set(("raydium", token), cached)
        return cached

    cached = PRICE_CACHE.get(("raydium", token))
    if cached is not None:
        update_price_cache(token, cached)
        return cached

    url = f"{RAYDIUM_API_URL}/price?token={token}"
    session = await get_session()
    try:
        async with session.get(url, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            price = data.get("price")
            value = float(price) if isinstance(price, (int, float)) else 0.0
    except aiohttp.ClientError as exc:  # pragma: no cover - network errors
        logger.warning("Failed to fetch price from Raydium: %s", exc)
        return 0.0

    PRICE_CACHE.set(("raydium", token), value)
    update_price_cache(token, value)
    return value


async def fetch_phoenix_price_async(token: str) -> float:
    """Return the current price for ``token`` from the Phoenix API."""
    cached = get_cached_price(token)
    if cached is not None:
        PRICE_CACHE.set(("phoenix", token), cached)
        return cached

    cached = PRICE_CACHE.get(("phoenix", token))
    if cached is not None:
        update_price_cache(token, cached)
        return cached

    url = f"{PHOENIX_API_URL}/price?token={token}"
    session = await get_session()
    try:
        async with session.get(url, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            price = data.get("price")
            value = float(price) if isinstance(price, (int, float)) else 0.0
    except aiohttp.ClientError as exc:  # pragma: no cover - network errors
        logger.warning("Failed to fetch price from Phoenix: %s", exc)
        return 0.0

    PRICE_CACHE.set(("phoenix", token), value)
    update_price_cache(token, value)
    return value


async def fetch_meteora_price_async(token: str) -> float:
    """Return the current price for ``token`` from the Meteora API."""
    cached = get_cached_price(token)
    if cached is not None:
        PRICE_CACHE.set(("meteora", token), cached)
        return cached

    cached = PRICE_CACHE.get(("meteora", token))
    if cached is not None:
        update_price_cache(token, cached)
        return cached

    url = f"{METEORA_API_URL}/price?token={token}"
    session = await get_session()
    try:
        async with session.get(url, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
            price = data.get("price")
            value = float(price) if isinstance(price, (int, float)) else 0.0
    except aiohttp.ClientError as exc:  # pragma: no cover - network errors
        logger.warning("Failed to fetch price from Meteora: %s", exc)
        return 0.0

    PRICE_CACHE.set(("meteora", token), value)
    update_price_cache(token, value)
    return value


ENABLE_BIRDEYE_FALLBACK = parse_bool_env("ENABLE_BIRDEYE_FALLBACK", True)
ENABLE_DEXSCREENER_FALLBACK = parse_bool_env("ENABLE_DEXSCREENER_FALLBACK", True)
DEXSCREENER_API_URL = os.getenv(
    "DEXSCREENER_API_URL", "https://api.dexscreener.com/latest/dex/tokens"
)


async def _fetch_price_from_helius(session: aiohttp.ClientSession, token: str) -> float:
    try:
        price, *_ = await _helius_price_overview(session, token)
    except Exception as exc:  # pragma: no cover - caching/optional failures
        logger.debug("Helius price helper failed for %s: %s", token, exc)
        return 0.0
    try:
        value = float(price)
    except Exception:
        value = 0.0
    return value if value > 0 else 0.0


async def _fetch_price_from_birdeye(session: aiohttp.ClientSession, token: str) -> float:
    try:
        price, *_ = await _birdeye_price_overview(session, token)
    except Exception as exc:  # pragma: no cover - optional dependency failures
        logger.debug("Birdeye price helper failed for %s: %s", token, exc)
        return 0.0
    try:
        value = float(price)
    except Exception:
        value = 0.0
    return value if value > 0 else 0.0


async def _fetch_price_from_dexscreener(
    session: aiohttp.ClientSession, token: str
) -> float:
    """Return price from DexScreener's token endpoint."""

    url = f"{DEXSCREENER_API_URL.rstrip('/')}/{token}"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status >= 400:
                return 0.0
            payload = await resp.json(content_type=None)
    except aiohttp.ClientError as exc:  # pragma: no cover - network failures
        logger.debug("DexScreener price request failed for %s: %s", token, exc)
        return 0.0
    except Exception:
        return 0.0

    best_price = 0.0
    best_liquidity = -1.0

    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if isinstance(pairs, list):
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            price_val = pair.get("priceUsd") or pair.get("price")
            try:
                price_float = float(price_val)
            except Exception:
                continue
            liquidity_info = pair.get("liquidity")
            liquidity = 0.0
            if isinstance(liquidity_info, dict):
                for key in ("usd", "base", "quote"):
                    cand = liquidity_info.get(key)
                    if isinstance(cand, (int, float)):
                        liquidity = max(liquidity, float(cand))
            elif isinstance(liquidity_info, (int, float)):
                liquidity = float(liquidity_info)
            if liquidity > best_liquidity and price_float > 0:
                best_price = price_float
                best_liquidity = liquidity

    return best_price if best_price > 0 else 0.0


async def fetch_jupiter_price_async(token: str) -> float:
    """Return the current price for ``token`` from the Jupiter price API."""

    cached = get_cached_price(token)
    if cached is not None:
        PRICE_CACHE.set(("jupiter", token), cached)
        return cached

    cached = PRICE_CACHE.get(("jupiter", token))
    if cached is not None:
        update_price_cache(token, cached)
        return cached

    session = await get_session()
    value = await _fetch_price_from_helius(session, token)
    if value <= 0 and ENABLE_BIRDEYE_FALLBACK:
        value = await _fetch_price_from_birdeye(session, token)
    if value <= 0 and ENABLE_DEXSCREENER_FALLBACK:
        value = await _fetch_price_from_dexscreener(session, token)

    PRICE_CACHE.set(("jupiter", token), value)
    update_price_cache(token, value)
    return value


async def fetch_helius_price_async(token: str) -> float:
    """Compatibility wrapper exposing the Helius-backed price helper."""

    return await fetch_jupiter_price_async(token)


fetch_helius_price_async.__name__ = "fetch_helius_price_async"


def make_api_price_fetch(url: str, name: str | None = None) -> PriceFeed:
    """Return a simple price fetcher for ``url``."""

    if not name:
        name = url

    async def _fetch(token: str, _name=name) -> float:
        cached = get_cached_price(token)
        if cached is not None:
            PRICE_CACHE.set((_name, token), cached)
            return cached

        cached = PRICE_CACHE.get((_name, token))
        if cached is not None:
            update_price_cache(token, cached)
            return cached

        req = f"{url.rstrip('/')}/price?token={token}"
        session = await get_session()
        try:
            async with session.get(req, timeout=10) as resp:
                resp.raise_for_status()
                data = await resp.json()
                price = data.get("price")
                value = float(price) if isinstance(price, (int, float)) else 0.0
        except aiohttp.ClientError as exc:  # pragma: no cover - network errors
            logger.warning("Failed to fetch price from %s: %s", url, exc)
            return 0.0

        PRICE_CACHE.set((_name, token), value)
        update_price_cache(token, value)
        return value

    _fetch.__name__ = str(name)
    return _fetch


async def stream_orca_prices(
    token: str, url: str = ORCA_WS_URL
) -> AsyncGenerator[float, None]:
    """Yield live prices for ``token`` from the Orca websocket feed."""

    if not url:
        return

    session = await get_session()
    while True:
        try:
            async with session.ws_connect(url) as ws:
                await ws.send_str(dumps({"token": token}).decode())
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = loads(msg.data)
                    except Exception:  # pragma: no cover - invalid message
                        continue
                    price = data.get("price")
                    if isinstance(price, (int, float)):
                        yield float(price)
        except Exception:  # pragma: no cover - connection failures
            logger.exception(
                "stream_orca_prices websocket error for %s; reconnecting", token
            )
            await asyncio.sleep(1)


async def stream_raydium_prices(
    token: str, url: str = RAYDIUM_WS_URL
) -> AsyncGenerator[float, None]:
    """Yield live prices for ``token`` from the Raydium websocket feed."""

    if not url:
        return

    session = await get_session()
    while True:
        try:
            async with session.ws_connect(url) as ws:
                await ws.send_str(dumps({"token": token}).decode())
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = loads(msg.data)
                    except Exception:  # pragma: no cover - invalid message
                        continue
                    price = data.get("price")
                    if isinstance(price, (int, float)):
                        yield float(price)
        except Exception:  # pragma: no cover - connection failures
            logger.exception(
                "stream_raydium_prices websocket error for %s; reconnecting", token
            )
            await asyncio.sleep(1)


async def stream_phoenix_prices(
    token: str, url: str = PHOENIX_WS_URL
) -> AsyncGenerator[float, None]:
    """Yield live prices for ``token`` from the Phoenix websocket feed."""

    if not url:
        return

    session = await get_session()
    while True:
        try:
            async with session.ws_connect(url) as ws:
                await ws.send_str(dumps({"token": token}).decode())
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = loads(msg.data)
                    except Exception:  # pragma: no cover - invalid message
                        continue
                    price = data.get("price")
                    if isinstance(price, (int, float)):
                        yield float(price)
        except Exception:  # pragma: no cover - connection failures
            logger.exception(
                "stream_phoenix_prices websocket error for %s; reconnecting", token
            )
            await asyncio.sleep(1)


async def stream_meteora_prices(
    token: str, url: str = METEORA_WS_URL
) -> AsyncGenerator[float, None]:
    """Yield live prices for ``token`` from the Meteora websocket feed."""

    if not url:
        return

    session = await get_session()
    while True:
        try:
            async with session.ws_connect(url) as ws:
                await ws.send_str(dumps({"token": token}).decode())
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = loads(msg.data)
                    except Exception:  # pragma: no cover - invalid message
                        continue
                    price = data.get("price")
                    if isinstance(price, (int, float)):
                        yield float(price)
        except Exception:  # pragma: no cover - connection failures
            logger.exception(
                "stream_meteora_prices websocket error for %s; reconnecting", token
            )
            await asyncio.sleep(1)


async def stream_jupiter_prices(
    token: str, url: str = JUPITER_WS_URL
) -> AsyncGenerator[float, None]:
    """Yield live prices for ``token`` from the Jupiter websocket feed."""

    if not url:
        return

    session = await get_session()
    while True:
        try:
            async with session.ws_connect(url) as ws:
                await ws.send_str(dumps({"token": token}).decode())
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    try:
                        data = loads(msg.data)
                    except Exception:  # pragma: no cover - invalid message
                        continue
                    price = data.get("price")
                    if isinstance(price, (int, float)):
                        yield float(price)
        except Exception:  # pragma: no cover - connection failures
            logger.exception(
                "stream_jupiter_prices websocket error for %s; reconnecting", token
            )
            await asyncio.sleep(1)


def make_ws_stream(url: str) -> Callable[[str], AsyncGenerator[float, None]]:
    """Return a simple websocket price stream factory."""

    async def _stream(token: str, url: str = url) -> AsyncGenerator[float, None]:
        if not url:
            return
        session = await get_session()
        while True:
            try:
                async with session.ws_connect(url) as ws:
                    await ws.send_str(dumps({"token": token}).decode())
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        try:
                            data = loads(msg.data)
                        except Exception:  # pragma: no cover - invalid message
                            continue
                        price = data.get("price")
                        if isinstance(price, (int, float)):
                            yield float(price)
            except Exception:  # pragma: no cover - connection failures
                logger.exception(
                    "websocket error for %s in make_ws_stream; reconnecting", token
                )
                await asyncio.sleep(1)

    return _stream


def _best_route_py(
    prices: Mapping[str, float],
    amount: float,
    *,
    token: str | None = None,
    fees: Mapping[str, float] | None = None,
    gas: Mapping[str, float] | None = None,
    latency: Mapping[str, float] | None = None,
    depth: Mapping[str, Mapping[str, float]] | None = None,
    mempool_rate: float = 0.0,
    use_flash_loans: bool | None = None,
    max_flash_amount: float | None = None,
    max_hops: int | None = None,
    path_algorithm: str | None = None,
) -> tuple[list[str], float]:
    """Return path with maximum profit and the expected profit."""

    paths = _list_paths(
        prices,
        amount,
        token=token,
        fees=fees,
        gas=gas,
        latency=latency,
        depth=depth,
        mempool_rate=mempool_rate,
        use_flash_loans=use_flash_loans,
        max_flash_amount=max_flash_amount,
        max_hops=max_hops,
    )
    return paths[0] if paths else ([], float("-inf"))


def _best_route_numba(
    prices: Mapping[str, float],
    amount: float,
    *,
    token: str | None = None,
    fees: Mapping[str, float] | None = None,
    gas: Mapping[str, float] | None = None,
    latency: Mapping[str, float] | None = None,
    depth: Mapping[str, Mapping[str, float]] | None = None,
    mempool_rate: float = 0.0,
    use_flash_loans: bool | None = None,
    max_flash_amount: float | None = None,
    max_hops: int | None = None,
    path_algorithm: str | None = None,
) -> tuple[list[str], float]:
    fees = fees or {}
    gas = gas or {}
    latency = latency or {}
    for v in prices.keys():
        if v not in fees:
            fees[v] = DEX_FEES.get(v, 0.0)
        if v not in gas:
            gas[v] = DEX_GAS.get(v, 0.0)
        if v not in latency:
            latency[v] = DEX_LATENCY.get(v, 0.0)
    if use_flash_loans is None:
        use_flash_loans = USE_FLASH_LOANS
    if max_flash_amount is None:
        max_flash_amount = MAX_FLASH_AMOUNT
    if max_hops is None:
        max_hops = MAX_HOPS
    trade_amount = (
        min(max_flash_amount or amount, amount) if use_flash_loans else amount
    )

    venues, adjacency = _build_adjacency(
        prices,
        trade_amount,
        fees,
        gas,
        latency,
        depth,
        token,
        mempool_rate,
    )
    index = {v: i for i, v in enumerate(venues)}
    import numpy as np

    n = len(venues)
    matrix = np.full((n, n), float("-inf"), dtype=float)
    for a, neigh in adjacency.items():
        i = index[a]
        for b, val in neigh.items():
            j = index[b]
            matrix[i, j] = float(val)

    idx_path, profit = _search_numba(matrix, int(max_hops))
    path = [venues[i] for i in idx_path]
    return path, float(profit)


def _best_route(
    prices: Mapping[str, float],
    amount: float,
    *,
    mempool_rate: float = 0.0,
    use_gnn_routing: bool | None = None,
    gnn_model_path: str | None = None,
    **kwargs,
) -> tuple[list[str], float]:
    """Return the best route using the Rust FFI when available."""

    if use_gnn_routing is None:
        use_gnn_routing = USE_GNN_ROUTING
    if gnn_model_path is None:
        gnn_model_path = GNN_MODEL_PATH

    if use_gnn_routing:
        try:
            from .models.gnn import load_route_gnn, rank_routes
            from .models.route_generator import load_route_generator

            model = load_route_gnn(gnn_model_path)
            generator = load_route_generator(ROUTE_GENERATOR_PATH)
        except Exception:
            model = None
            generator = None
        if model is not None:
            routes: list[list[str]] | None = None
            if generator is not None:
                try:
                    routes = generator.generate(
                        max_hops=kwargs.get("max_hops", MAX_HOPS)
                    )
                except Exception:
                    routes = None
            cand: list[tuple[list[str], float]]
            if routes:
                fees = kwargs.get("fees")
                gas = kwargs.get("gas")
                latency = kwargs.get("latency")
                depth = kwargs.get("depth")
                token = kwargs.get("token")
                use_flash_loans = kwargs.get("use_flash_loans")
                max_flash_amount = kwargs.get("max_flash_amount")
                trade_amount = (
                    min(max_flash_amount or amount, amount)
                    if (use_flash_loans or USE_FLASH_LOANS)
                    else amount
                )
                venues, adjacency = _build_adjacency(
                    prices,
                    trade_amount,
                    fees or {},
                    gas or {},
                    latency or {},
                    depth,
                    token,
                    mempool_rate,
                )
                cand = []
                for r in routes:
                    profit = 0.0
                    valid = True
                    for a, b in zip(r[:-1], r[1:]):
                        val = adjacency.get(a, {}).get(b)
                        if val is None:
                            valid = False
                            break
                        profit += val
                    if valid:
                        cand.append((r, profit))
            else:
                cand = _list_paths(
                    prices,
                    amount,
                    mempool_rate=mempool_rate,
                    **kwargs,
                )
            if cand:
                idx = rank_routes(model, [p for p, _ in cand])
                return cand[idx]
    path_algo = kwargs.get("path_algorithm")
    if path_algo == "dijkstra":
        return _best_route_numba(
            prices,
            amount,
            mempool_rate=mempool_rate,
            **kwargs,
        )
    if USE_FFI_ROUTE and _routeffi.is_routeffi_available():
        fees = dict(kwargs.get("fees") or {})
        gas = dict(kwargs.get("gas") or {})
        latency = dict(kwargs.get("latency") or {})
        for v in prices.keys():
            fees.setdefault(v, DEX_FEES.get(v, 0.0))
            gas.setdefault(v, DEX_GAS.get(v, 0.0))
            latency.setdefault(v, DEX_LATENCY.get(v, 0.0))
        ffi_kwargs = {
            "fees": fees,
            "gas": gas,
            "latency": latency,
            "max_hops": kwargs.get("max_hops", MAX_HOPS),
        }
        try:
            if _routeffi.parallel_enabled():
                func = _routeffi.best_route_parallel
            else:
                func = _routeffi.best_route
            res = func(dict(prices), amount, **ffi_kwargs)
            if res:
                return res
        except Exception as exc:  # pragma: no cover - optional ffi failures
            logger.warning("ffi.best_route failed: %s", exc)
    return _best_route_py(prices, amount, mempool_rate=mempool_rate, **kwargs)


async def _compute_route(
    token: str,
    amount: float,
    price_map: Mapping[str, float],
    *,
    fees: Mapping[str, float],
    gas: Mapping[str, float],
    latency: Mapping[str, float],
    use_service: bool,
    use_flash_loans: bool,
    max_flash_amount: float,
    max_hops: int,
    path_algorithm: str,
) -> tuple[list[str], float]:
    key = _route_key(token, amount, fees, gas, latency)
    cached = ROUTE_CACHE.get(key)
    if cached is not None:
        return cached

    res = None
    if use_service:
        try:
            res = await depth_client.cached_route(
                token,
                amount,
                socket_path=DEPTH_SERVICE_SOCKET,
                max_hops=max_hops,
            )
        except Exception as exc:  # pragma: no cover - service optional
            logger.debug("depth_service.cached_route failed: %s", exc)
            res = None
        if not res:
            try:
                res = await depth_client.best_route(
                    token,
                    amount,
                    socket_path=DEPTH_SERVICE_SOCKET,
                    max_hops=max_hops,
                )
            except Exception as exc:  # pragma: no cover - service optional
                logger.warning("depth_service.best_route failed: %s", exc)
                res = None
        if res:
            path, profit, _ = res
        else:
            use_service = False
    if not use_service or not res:
        depth_map, mempool_rate = depth_client.snapshot(token)
        total = sum(
            float(v.get("bids", 0.0)) + float(v.get("asks", 0.0))
            for v in depth_map.values()
        )
        _LAST_DEPTH[token] = total
        path, profit = _best_route(
            price_map,
            amount,
            token=token,
            fees=fees,
            gas=gas,
            latency=latency,
            depth=depth_map,
            mempool_rate=mempool_rate,
            use_flash_loans=use_flash_loans,
            max_flash_amount=max_flash_amount,
            max_hops=max_hops,
            path_algorithm=path_algorithm,
        )

    ROUTE_CACHE[key] = (path, profit)
    return path, profit


async def _detect_for_token(
    token: str,
    feeds: Sequence[PriceFeed] | None = None,
    streams: Sequence[AsyncGenerator[float, None]] | None = None,
    *,
    threshold: float = 0.0,
    amount: float = 1.0,
    testnet: bool = False,
    dry_run: bool = False,
    keypair=None,
    max_updates: int | None = None,
    fees: Mapping[str, float] | None = None,
    gas: Mapping[str, float] | None = None,
    latencies: Mapping[str, float] | None = None,
    stream_names: Sequence[str] | None = None,
    executor: "EventExecutor | None" = None,
    use_service: bool = False,
    use_flash_loans: bool | None = None,
    use_mev_bundles: bool | None = None,
    max_flash_amount: float | None = None,
    max_hops: int | None = None,
    path_algorithm: str | None = None,
) -> Optional[Tuple[int, int]]:
    """Check for price discrepancies and place arbitrage orders.

    Parameters
    ----------
    token:
        Token symbol or address to trade.
    feeds:
        Sequence of callables returning the token price from different DEXes.
    threshold:
        Minimum fractional price difference required to trigger arbitrage.
    amount:
        Trade size to use for buy/sell orders.
    use_flash_loans:
        Borrow funds via a flash-loan program before executing the swap chain.
    max_flash_amount:
        Maximum amount to borrow when flash loans are enabled.
    max_hops:
        Maximum number of venues to traverse when searching for a path.
    path_algorithm:
        "graph" to use the dynamic graph search or "permutation" for the
        legacy exhaustive search.

    Returns
    -------
    Optional[Tuple[int, int]]
        Indices of the feeds used for buy and sell orders when an opportunity is
        executed. ``None`` when no profitable opportunity is found.
    """

    if use_flash_loans is None:
        use_flash_loans = USE_FLASH_LOANS
    if max_flash_amount is None:
        max_flash_amount = MAX_FLASH_AMOUNT

    if fees is None or gas is None or latencies is None:
        env_fees, env_gas, env_lat = refresh_costs()
        if fees is None:
            fees = env_fees
        else:
            for k, v in env_fees.items():
                fees.setdefault(k, v)
        if gas is None:
            gas = env_gas
        else:
            for k, v in env_gas.items():
                gas.setdefault(k, v)
        if latencies is None:
            latencies = env_lat
        else:
            for k, v in env_lat.items():
                latencies.setdefault(k, v)
    if streams:
        prices: list[Optional[float]] = [None] * len(streams)
        result: Optional[Tuple[int, int]] = None

        async def maybe_execute() -> Optional[Tuple[int, int]]:
            if any(p is None for p in prices):
                return None
            names = [
                stream_names[i] if stream_names and i < len(stream_names) else str(i)
                for i in range(len(prices))
            ]
            price_map = {n: p for n, p in zip(names, prices) if p is not None and p > 0}
            if len(price_map) < 2:
                return None
            path, profit = await _compute_route(
                token,
                amount,
                price_map,
                fees=fees,
                gas=gas,
                latency=latencies,
                use_service=USE_SERVICE_ROUTE,
                use_flash_loans=use_flash_loans,
                max_flash_amount=max_flash_amount,
                max_hops=max_hops,
                path_algorithm=path_algorithm,
            )
            if not path:
                return None
            buy_name, sell_name = path[0], path[-1]
            buy_index = names.index(buy_name)
            sell_index = names.index(sell_name)
            trade_base = (
                min(max_flash_amount or amount, amount) if use_flash_loans else amount
            )
            diff_base = price_map[buy_name] * trade_base
            if diff_base <= 0:
                return None
            diff = profit / diff_base
            if diff < threshold:
                return None
            logger.info(
                "Arbitrage detected on %s via %s: profit %.6f",
                token,
                "->".join(path),
                profit,
            )
            trade_amount = amount
            if use_flash_loans:
                flash_amt = max_flash_amount or amount
                trade_amount = min(flash_amt, amount)
                swap_ix = [
                    Instruction(Pubkey.default(), b"swap", [])
                    for _ in range(len(path) - 1)
                ]
                sig = await borrow_flash(
                    trade_amount,
                    token,
                    swap_ix,
                    payer=keypair or Keypair(),
                )
                tasks = []
            else:
                sig = None
                if use_service:
                    txs: list[str] = []
                    for i in range(len(path) - 1):
                        buy_v = path[i]
                        sell_v = path[i + 1]
                        base_buy = VENUE_URLS.get(buy_v, buy_v)
                        base_sell = VENUE_URLS.get(sell_v, sell_v)
                        tx1 = await _prepare_service_tx(
                            token,
                            "buy",
                            trade_amount,
                            price_map[buy_v],
                            base_buy,
                        )
                        tx2 = await _prepare_service_tx(
                            token,
                            "sell",
                            trade_amount,
                            price_map[sell_v],
                            base_sell,
                        )
                        if tx1:
                            txs.append(tx1)
                        if tx2:
                            txs.append(tx2)

                    if txs:
                        if use_mev_bundles:
                            from .mev_executor import MEVExecutor

                            mev = MEVExecutor(
                                token,
                                priority_rpc=(
                                    getattr(executor, "priority_rpc", None)
                                    if executor
                                    else None
                                ),
                            )
                            await mev.submit_bundle(txs)
                        elif executor:
                            for tx in txs:
                                await executor.enqueue(tx)
                        else:
                            from .depth_client import submit_raw_tx

                            for tx in txs:
                                await submit_raw_tx(
                                    tx,
                                    priority_rpc=(
                                        getattr(executor, "priority_rpc", None)
                                        if executor
                                        else None
                                    ),
                                )
                else:
                    tasks = []
                    for i in range(len(path) - 1):
                        buy_v = path[i]
                        sell_v = path[i + 1]
                        tasks.append(
                            place_order_async(
                                token,
                                "buy",
                                trade_amount,
                                price_map[buy_v],
                                testnet=testnet,
                                dry_run=dry_run,
                                keypair=keypair,
                            )
                        )
                        tasks.append(
                            place_order_async(
                                token,
                                "sell",
                                trade_amount,
                                price_map[sell_v],
                                testnet=testnet,
                                dry_run=dry_run,
                                keypair=keypair,
                            )
                        )
                    if tasks:
                        await asyncio.gather(*tasks)
            if sig:
                await repay_flash(sig)
            return buy_index, sell_index

        async def consume(idx: int, gen: AsyncGenerator[float, None]):
            nonlocal result
            count = 0
            async for price in gen:
                if result is not None:
                    break
                prices[idx] = price
                res = await maybe_execute()
                if res:
                    result = res
                    break
                count += 1
                if max_updates is not None and count >= max_updates:
                    break

        tasks = [asyncio.create_task(consume(i, g)) for i, g in enumerate(streams)]
        await asyncio.gather(*tasks, return_exceptions=True)
        return result

    if not feeds:
        feeds = [
            fetch_orca_price_async,
            fetch_raydium_price_async,
            fetch_phoenix_price_async,
            fetch_meteora_price_async,
            fetch_jupiter_price_async,
        ]
        for name in ("orca", "raydium", "phoenix", "meteora", "jupiter"):
            if fees is not None and name not in fees:
                fees[name] = DEX_FEES.get(name, 0.0)
            if gas is not None and name not in gas:
                gas[name] = DEX_GAS.get(name, 0.0)
            if latencies is not None and name not in latencies:
                latencies[name] = DEX_LATENCY.get(name, 0.0)
        for name, url in EXTRA_API_URLS.items():
            feeds.append(make_api_price_fetch(url, name))
            if fees is not None and name not in fees:
                fees[name] = DEX_FEES.get(name, 0.0)
            if gas is not None and name not in gas:
                gas[name] = DEX_GAS.get(name, 0.0)
            if latencies is not None and name not in latencies:
                latencies[name] = DEX_LATENCY.get(name, 0.0)

    prices = await asyncio.gather(*(feed(token) for feed in feeds))
    if not prices:
        return None

    names = (
        [getattr(f, "__name__", str(i)) for i, f in enumerate(feeds)] if feeds else []
    )
    price_map = {n: p for n, p in zip(names, prices) if p > 0}
    if len(price_map) < 2:
        return None

    path, profit = await _compute_route(
        token,
        amount,
        price_map,
        fees=fees,
        gas=gas,
        latency=latencies,
        use_service=USE_SERVICE_ROUTE,
        use_flash_loans=use_flash_loans,
        max_flash_amount=max_flash_amount,
        max_hops=max_hops,
        path_algorithm=path_algorithm,
    )
    if not path:
        return None
    buy_name, sell_name = path[0], path[-1]
    trade_base = min(max_flash_amount or amount, amount) if use_flash_loans else amount
    diff_base = price_map[buy_name] * trade_base
    if diff_base <= 0:
        return None
    diff = profit / diff_base
    if diff < threshold:
        logger.info("No arbitrage opportunity: diff %.4f below threshold", diff)
        return None

    logger.info(
        "Arbitrage detected on %s via %s: profit %.6f", token, "->".join(path), profit
    )

    trade_amount = amount
    sig = None
    tasks = []
    if use_flash_loans:
        flash_amt = max_flash_amount or amount
        trade_amount = min(flash_amt, amount)
        swap_ix = [
            Instruction(Pubkey.default(), b"swap", []) for _ in range(len(path) - 1)
        ]
        sig = await borrow_flash(
            trade_amount,
            token,
            swap_ix,
            payer=keypair or Keypair(),
        )
    else:
        if use_service:
            txs: list[str] = []
            for i in range(len(path) - 1):
                buy_v = path[i]
                sell_v = path[i + 1]
                base_buy = VENUE_URLS.get(buy_v, buy_v)
                base_sell = VENUE_URLS.get(sell_v, sell_v)
                tx1 = await _prepare_service_tx(
                    token,
                    "buy",
                    trade_amount,
                    price_map[buy_v],
                    base_buy,
                )
                tx2 = await _prepare_service_tx(
                    token,
                    "sell",
                    trade_amount,
                    price_map[sell_v],
                    base_sell,
                )
                if tx1:
                    txs.append(tx1)
                if tx2:
                    txs.append(tx2)
            if txs:
                if use_mev_bundles:
                    from .mev_executor import MEVExecutor

                    mev = MEVExecutor(
                        token,
                        priority_rpc=(
                            getattr(executor, "priority_rpc", None)
                            if executor
                            else None
                        ),
                    )
                    await mev.submit_bundle(txs)
                elif executor:
                    for tx in txs:
                        await executor.enqueue(tx)
                else:
                    from .depth_client import submit_raw_tx

                    for tx in txs:
                        await submit_raw_tx(
                            tx,
                            priority_rpc=(
                                getattr(executor, "priority_rpc", None)
                                if executor
                                else None
                            ),
                        )
        else:
            tasks = []
            for i in range(len(path) - 1):
                buy_v = path[i]
                sell_v = path[i + 1]
                tasks.append(
                    place_order_async(
                        token,
                        "buy",
                        trade_amount,
                        price_map[buy_v],
                        testnet=testnet,
                        dry_run=dry_run,
                        keypair=keypair,
                    )
                )
                tasks.append(
                    place_order_async(
                        token,
                        "sell",
                        trade_amount,
                        price_map[sell_v],
                        testnet=testnet,
                        dry_run=dry_run,
                        keypair=keypair,
                    )
                )
            if tasks:
                await asyncio.gather(*tasks)
    if sig:
        await repay_flash(sig)

    buy_index = names.index(buy_name)
    sell_index = names.index(sell_name)
    return buy_index, sell_index


async def detect_and_execute_arbitrage(
    tokens: str | Sequence[str],
    feeds: Sequence[PriceFeed] | None = None,
    streams: Sequence[AsyncGenerator[float, None]] | None = None,
    *,
    fees: Mapping[str, float] | None = None,
    executor: EventExecutor | None = None,
    use_service: bool | None = None,
    use_flash_loans: bool | None = None,
    use_mev_bundles: bool | None = None,
    max_flash_amount: float | None = None,
    max_hops: int | None = None,
    path_algorithm: str | None = None,
    **kwargs,
) -> Optional[Tuple[int, int]] | list[Optional[Tuple[int, int]]]:
    """Run arbitrage detection for one or multiple tokens.

    When ``tokens`` is a sequence, price checks for each token are executed
    concurrently using :func:`asyncio.gather`.
    """

    user_gas = kwargs.pop("gas", None)
    user_lat = kwargs.pop("latencies", None)

    if use_flash_loans is None:
        use_flash_loans = USE_FLASH_LOANS
    if max_flash_amount is None:
        max_flash_amount = MAX_FLASH_AMOUNT

    if use_service is None:
        use_service = USE_SERVICE_EXEC
    if max_hops is None:
        max_hops = MAX_HOPS
    if path_algorithm is None:
        path_algorithm = PATH_ALGORITHM

    def _streams_for(token: str):
        if streams is not None:
            return streams, None
        if feeds is not None:
            return None, None

        available = {
            "orca": (ORCA_WS_URL, stream_orca_prices),
            "raydium": (RAYDIUM_WS_URL, stream_raydium_prices),
            "phoenix": (PHOENIX_WS_URL, stream_phoenix_prices),
            "meteora": (METEORA_WS_URL, stream_meteora_prices),
            "jupiter": (JUPITER_WS_URL, stream_jupiter_prices),
            "service": (f"ipc://{DEPTH_SERVICE_SOCKET}?{token}", None),
        }
        for name, url in EXTRA_WS_URLS.items():
            available[name] = (url, make_ws_stream(url))
        auto: list[AsyncGenerator[float, None]] = []
        names: list[str] = []
        for name in DEX_PRIORITIES:
            url, fn = available.get(name, ("", None))
            if not url:
                continue
            if name == "service":
                auto.append(
                    order_book_ws.stream_order_book(url, rate_limit=DEPTH_RATE_LIMIT)
                )
                names.append(name)
                continue
            if fn is not None:
                auto.append(fn(token, url=url))
                names.append(name)
        if not auto:
            return None, None
        return auto, names

    if isinstance(tokens, Sequence) and not isinstance(tokens, str):
        tasks = []
        for t in tokens:
            s, names = _streams_for(t)
            tasks.append(
                _detect_for_token(
                    t,
                    feeds=feeds,
                    streams=s,
                    stream_names=names,
                    fees=fees or DEX_FEES,
                    gas=user_gas or DEX_GAS,
                    latencies=user_lat or DEX_LATENCY,
                    executor=executor,
                    use_service=use_service,
                    use_flash_loans=use_flash_loans,
                    use_mev_bundles=use_mev_bundles,
                    max_flash_amount=max_flash_amount,
                    max_hops=max_hops,
                    path_algorithm=path_algorithm,
                    **kwargs,
                )
            )

        return await asyncio.gather(*tasks)

    s, names = _streams_for(tokens)
    if USE_DEPTH_STREAM:
        async for _ in stream_depth(tokens, max_updates=kwargs.get("max_updates")):
            res = await _detect_for_token(
                tokens,
                feeds=feeds,
                streams=s,
                stream_names=names,
                fees=fees or DEX_FEES,
                gas=user_gas or DEX_GAS,
                latencies=user_lat or DEX_LATENCY,
                executor=executor,
                use_service=use_service,
                use_flash_loans=use_flash_loans,
                use_mev_bundles=use_mev_bundles,
                max_flash_amount=max_flash_amount,
                max_hops=max_hops,
                path_algorithm=path_algorithm,
                **kwargs,
            )
            if res:
                return res
        return None

    return await _detect_for_token(
        tokens,
        feeds=feeds,
        streams=s,
        stream_names=names,
        fees=fees or DEX_FEES,
        gas=user_gas or DEX_GAS,
        latencies=user_lat or DEX_LATENCY,
        executor=executor,
        use_service=use_service,
        use_flash_loans=use_flash_loans,
        use_mev_bundles=use_mev_bundles,
        max_flash_amount=max_flash_amount,
        max_hops=max_hops,
        path_algorithm=path_algorithm,
        **kwargs,
    )


async def evaluate(token: str, portfolio) -> List[dict]:
    """Return arbitrage buy/sell actions for ``token``.

    This function performs a dry-run arbitrage detection and, when an
    opportunity is found, returns corresponding buy and sell actions. The
    amount and threshold are controlled via the ``ARBITRAGE_AMOUNT`` and
    ``ARBITRAGE_THRESHOLD`` environment variables. If either is not set or
    non-positive, no action is taken.
    """

    threshold = float(os.getenv("ARBITRAGE_THRESHOLD", "0") or 0)
    amount = float(os.getenv("ARBITRAGE_AMOUNT", "0") or 0)
    if threshold <= 0 or amount <= 0:
        return []

    try:
        res = await detect_and_execute_arbitrage(
            token, threshold=threshold, amount=amount, dry_run=True
        )
    except Exception as exc:  # pragma: no cover - network issues
        logger.warning("Arbitrage evaluation failed for %s: %s", token, exc)
        return []

    if not res:
        return []

    action = {"token": token, "amount": float(amount), "price": 0.0}
    return [
        dict(action, side="buy"),
        dict(action, side="sell"),
    ]


try:  # pragma: no cover - best effort
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = None
if MEASURE_DEX_LATENCY and loop:
    loop.call_soon(start_latency_refresh)
