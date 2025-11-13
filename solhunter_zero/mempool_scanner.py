from __future__ import annotations

import asyncio
import logging
import re
import os
import statistics
import contextlib
import time
import importlib
import heapq
import itertools
from collections import deque
from typing import AsyncGenerator, Iterable, Dict, Any, Deque
from solders.pubkey import Pubkey
from importlib.machinery import ModuleSpec

from .dynamic_limit import _target_concurrency, _step_limit
from . import resource_monitor
from .event_bus import subscription
from .system import detect_cpu_count
from .url_helpers import as_websocket_url

spec = importlib.util.find_spec("solana.publickey")
if spec is not None:  # pragma: no cover - optional dependency path
    PublicKey = importlib.import_module("solana.publickey").PublicKey  # type: ignore[attr-defined]
else:  # Fallback to solders and install a stub for consumers/tests
    import types, sys

    class _PublicKeyCompat(str):
        def __new__(cls, value: str):
            obj = str.__new__(cls, value)
            try:
                obj._pubkey = Pubkey.from_string(value)
            except Exception:
                obj._pubkey = None
            return obj

        @property
        def _key(self):
            return getattr(self, "_pubkey", None)

        def to_solders(self) -> Pubkey:
            pk = getattr(self, "_pubkey", None)
            if pk is None:
                pk = Pubkey.from_string(str(self))
                self._pubkey = pk
            return pk

        def __bytes__(self) -> bytes:
            return bytes(self.to_solders())

    PublicKey = _PublicKeyCompat  # type: ignore[assignment]

    stub = types.ModuleType("solana.publickey")
    stub.PublicKey = PublicKey
    stub.__spec__ = ModuleSpec("solana.publickey", loader=None)
    sys.modules.setdefault("solana.publickey", stub)

try:
    from solana.rpc.websocket_api import RpcTransactionLogsFilterMentions, connect
except Exception:  # pragma: no cover - optional dependency
    RpcTransactionLogsFilterMentions = None  # type: ignore

    async def connect(*args, **kwargs):  # type: ignore
        raise RuntimeError("solana.rpc.websocket_api not available")

# --- constants defined locally to avoid cross-module import issues ---
# SPL Token Program (mainnet)
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")

from .dex_scanner import DEX_PROGRAM_ID
from . import onchain_metrics
from . import order_book_ws

logger = logging.getLogger(__name__)

MEMPOOL_STATS_WINDOW = int(os.getenv("MEMPOOL_STATS_WINDOW", "5") or 5)
MEMPOOL_SCORE_THRESHOLD = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)
MEMPOOL_RANK_FLUSH_INTERVAL = float(
    os.getenv("MEMPOOL_RANK_FLUSH_INTERVAL", "0.05") or 0.05
)


def _load_depth_timeout() -> float:
    fallback = os.getenv("DISCOVERY_WARM_TIMEOUT") or "5"
    value = os.getenv("MEMPOOL_DEPTH_STREAM_TIMEOUT", fallback)
    try:
        timeout = float(value)
    except (TypeError, ValueError):
        timeout = 5.0
    if timeout < 0:
        return 0.0
    return timeout


MEMPOOL_DEPTH_STREAM_TIMEOUT = _load_depth_timeout()

_ROLLING_STATS: Dict[str, Dict[str, Deque[float]]] = {}
_DYN_INTERVAL: float = 2.0
_METRICS_TIMEOUT: float = 5.0

# Log pattern helpers
NAME_RE = re.compile(r"name:\s*(\S+)", re.IGNORECASE)
MINT_RE = re.compile(r"mint:\s*(\S+)", re.IGNORECASE)
POOL_TOKEN_RE = re.compile(r"token[AB]:\s*([A-Za-z0-9]{32,44})", re.IGNORECASE)

# Base58-ish alphabet used by Solana addresses (no 0,O,I,l)
_BASE58_CHARS = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")


def _looks_like_mint(value: str | None) -> bool:
    """Very light sanity check for a token mint address."""
    if not value or not isinstance(value, str):
        return False
    n = len(value)
    if n < 3 or n > 44:
        return False
    return value.isalnum()


def _to_ws_url(url: str) -> str:
    """Normalise *url* into a websocket endpoint when possible."""

    resolved = as_websocket_url(url)
    if resolved:
        return resolved
    return url


async def stream_mempool_tokens(
    rpc_url: str,
    *,
    suffix: str | None = None,            # kept for backward compatibility (ignored)
    keywords: Iterable[str] | None = None, # kept for backward compatibility (ignored)
    include_pools: bool = True,
    return_metrics: bool = False,
) -> AsyncGenerator[str | Dict[str, Any], None]:
    """Yield token mints seen in unconfirmed transactions (no name/suffix filtering)."""

    ws_env = (os.getenv("SOLANA_WS_URL") or "").strip()
    if not rpc_url:
        rpc_url = ws_env
    if not rpc_url:
        if False:
            yield None
        return

    rpc_url = _to_ws_url(rpc_url)

    def _filter_value(value: Any) -> Any:
        try:
            return Pubkey.from_string(str(value))
        except Exception:
            try:
                return value.to_bytes()  # type: ignore[attr-defined]
            except Exception:
                return value

    async with connect(rpc_url) as ws:
        # Subscribe to SPL Token logs
        try:
            await ws.logs_subscribe(
                RpcTransactionLogsFilterMentions(_filter_value(TOKEN_PROGRAM_ID)),
                commitment="processed",
            )
        except Exception:
            await ws.logs_subscribe(
                RpcTransactionLogsFilterMentions(_filter_value(TOKEN_PROGRAM_ID))
            )

        # Optionally subscribe to DEX program logs to catch pool token mentions
        if include_pools:
            try:
                await ws.logs_subscribe(
                    RpcTransactionLogsFilterMentions(_filter_value(DEX_PROGRAM_ID)),
                    commitment="processed",
                )
            except Exception:
                await ws.logs_subscribe(
                    RpcTransactionLogsFilterMentions(_filter_value(DEX_PROGRAM_ID))
                )

        while True:
            try:
                msgs = await ws.recv()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pragma: no cover - network errors
                logger.error("Websocket error: %s", exc)
                await asyncio.sleep(1)
                continue

            for msg in msgs:
                try:
                    logs = msg.result.value.logs  # type: ignore[attr-defined]
                except Exception:
                    try:
                        logs = msg["result"]["value"]["logs"]  # type: ignore[index]
                    except Exception:
                        continue

                tokens = set()

                # Detect InitializeMint lines (new mint init events)
                if any("InitializeMint" in l for l in logs):
                    name = None
                    mint = None
                    for line in logs:
                        if name is None:
                            m = NAME_RE.search(line)
                            if m:
                                name = m.group(1)
                        if mint is None:
                            m = MINT_RE.search(line)
                            if m:
                                mint = m.group(1)
                    if _looks_like_mint(mint):
                        tokens.add(mint)

                # Also scrape tokens from new pool logs (tokenA/tokenB fields)
                if include_pools:
                    for line in logs:
                        m = POOL_TOKEN_RE.search(line)
                        if m and _looks_like_mint(m.group(1)):
                            tokens.add(m.group(1))

                for tok in tokens:
                    if return_metrics:
                        volume = await asyncio.to_thread(
                            onchain_metrics.fetch_volume_onchain, tok, rpc_url
                        )
                        liquidity_snapshot = await asyncio.to_thread(
                            onchain_metrics.fetch_liquidity_onchain, tok, rpc_url
                        )
                        liquidity_value = float(liquidity_snapshot)
                        payload: Dict[str, Any] = {
                            "address": tok,
                            "volume": float(volume),
                            "liquidity": liquidity_value,
                        }
                        if isinstance(liquidity_snapshot, dict):
                            payload["liquidity_snapshot"] = dict(liquidity_snapshot)
                        yield payload
                    else:
                        yield tok


async def rank_token(token: str, rpc_url: str) -> tuple[float, Dict[str, float]]:
    """Return ranking score and metrics for ``token``."""
    volume, liquidity, insights = await asyncio.gather(
        onchain_metrics.fetch_volume_onchain_async(token, rpc_url),
        onchain_metrics.fetch_liquidity_onchain_async(token, rpc_url),
        onchain_metrics.collect_onchain_insights_async(token, rpc_url),
    )
    liquidity_value = float(liquidity)
    liquidity_snapshot = dict(liquidity) if isinstance(liquidity, dict) else None
    volume_value = float(volume)
    tx_rate = insights.get("tx_rate", 0.0)
    whale_activity = insights.get("whale_activity", 0.0)
    avg_swap = insights.get("avg_swap_size", 0.0)

    wallet_conc = 1.0 - float(whale_activity)

    def _update(token: str, key: str, value: float) -> Deque[float]:
        dq = _ROLLING_STATS.setdefault(token, {}).setdefault(
            key, deque(maxlen=MEMPOOL_STATS_WINDOW)
        )
        dq.append(float(value))
        return dq

    tx_hist = _update(token, "tx", tx_rate)
    _update(token, "wallet", wallet_conc)
    _update(token, "swap", avg_swap)

    momentum = 0.0
    anomaly = 0.0
    if len(tx_hist) > 1:
        prev_avg = sum(list(tx_hist)[:-1]) / (len(tx_hist) - 1)
        momentum = tx_hist[-1] - prev_avg
        if len(tx_hist) > 2:
            mean = statistics.mean(list(tx_hist)[:-1])
            stdev = statistics.stdev(list(tx_hist)[:-1]) or 1.0
            anomaly = (tx_hist[-1] - mean) / stdev

    score = (
        volume_value
        + liquidity_value
        + float(tx_rate)
        + momentum
        + anomaly
        - float(whale_activity)
    )
    metrics = {
        "volume": volume_value,
        "liquidity": liquidity_value,
        "tx_rate": float(tx_rate),
        "whale_activity": float(whale_activity),
        "wallet_concentration": wallet_conc,
        "avg_swap_size": float(avg_swap),
        "momentum": momentum,
        "anomaly": anomaly,
        "score": score,
    }
    if liquidity_snapshot is not None:
        metrics["liquidity_snapshot"] = liquidity_snapshot
    return score, metrics


async def stream_ranked_mempool_tokens(
    rpc_url: str,
    *,
    suffix: str | None = None,             # ignored (kept for API compatibility)
    keywords: Iterable[str] | None = None, # ignored (kept for API compatibility)
    include_pools: bool = True,
    threshold: float | None = None,
    max_concurrency: int | None = None,
    cpu_usage_threshold: float | None = None,
    dynamic_concurrency: bool = False,
    limit: int | None = None,
) -> AsyncGenerator[Dict[str, float], None]:
    """Yield ranked token events from the mempool (no name/suffix filtering)."""

    if threshold is None:
        threshold = MEMPOOL_SCORE_THRESHOLD

    if max_concurrency is None or max_concurrency <= 0:
        max_concurrency = detect_cpu_count()

    sem = asyncio.Semaphore(max_concurrency)
    current_limit = max_concurrency
    cpu_val = {"v": resource_monitor.get_cpu_usage()}
    cpu_ts = {"t": 0.0}

    def _update_metrics(payload: Any) -> None:
        cpu = getattr(payload, "cpu", None)
        if isinstance(payload, dict):
            cpu = payload.get("cpu", cpu)
        if cpu is None:
            return
        try:
            cpu_val["v"] = float(cpu)
            cpu_ts["t"] = time.monotonic()
        except Exception:
            return

    _metrics_sub = subscription("system_metrics_combined", _update_metrics)
    _metrics_sub.__enter__()
    _dyn_interval = float(
        os.getenv("DYNAMIC_CONCURRENCY_INTERVAL", str(_DYN_INTERVAL)) or _DYN_INTERVAL
    )
    ewm = float(os.getenv("CONCURRENCY_EWM_SMOOTHING", "0.15") or 0.15)
    kp = float(os.getenv("CONCURRENCY_SMOOTHING", os.getenv("CONCURRENCY_KP", "0.5")) or 0.5)
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
                    if time.monotonic() - cpu_ts["t"] > _METRICS_TIMEOUT:
                        cpu = resource_monitor.get_cpu_usage()
                    else:
                        cpu = cpu_val["v"]
                    target = _target_concurrency(cpu, max_concurrency, low, high, smoothing=ewm)
                    new_limit = _step_limit(current_limit, target, max_concurrency, smoothing=kp, ki=ki)
                    if new_limit != current_limit:
                        await _set_limit(new_limit)
            except asyncio.CancelledError as exc:
                logger.debug("dynamic concurrency adjust task cancelled: %s", exc)

        adjust_task = asyncio.create_task(_adjust())

    queue: asyncio.Queue[Dict[str, float]] = asyncio.Queue()
    results: asyncio.Queue[Dict[str, float] | object] = asyncio.Queue()
    sentinel = object()
    pending_workers = 0
    producer_done = False

    try:
        limit_value = int(limit) if limit is not None else 0
    except Exception:
        limit_value = 0
    if limit_value < 0:
        limit_value = 0

    flush_interval = MEMPOOL_RANK_FLUSH_INTERVAL
    if flush_interval < 0:
        flush_interval = 0.0

    score_counter = itertools.count()
    heap: list[tuple[float, int, Dict[str, float]]] = []
    flushed_results = False

    def _event_score(evt: Dict[str, float]) -> float:
        try:
            return float(evt.get("combined_score", evt.get("score", 0.0)) or 0.0)
        except Exception:
            return 0.0

    def _push_event(evt: Dict[str, float]) -> None:
        score = _event_score(evt)
        entry = (score, next(score_counter), evt)
        if limit_value > 0:
            if len(heap) < limit_value:
                heapq.heappush(heap, entry)
            else:
                heapq.heappushpop(heap, entry)
        else:
            heapq.heappush(heap, entry)

    async def _flush_heap() -> None:
        nonlocal flushed_results
        if not heap or (limit_value > 0 and flushed_results):
            return
        size = limit_value if limit_value > 0 else len(heap)
        best = heapq.nlargest(size, heap)
        best.sort(key=lambda item: (-item[0], item[1]))
        if limit_value > 0:
            flushed_results = True
        heap.clear()
        remaining = size
        if limit_value > 0:
            remaining = min(limit_value, size)
        for _score, _idx, evt in best[:remaining]:
            await results.put(evt)

    async def worker(addr: str) -> None:
        nonlocal pending_workers
        payload: Dict[str, float] | None = None
        async with sem:
            try:
                score, data = await rank_token(addr, rpc_url)
                if score >= threshold:
                    combined = data["momentum"] * (1.0 - data["whale_activity"])
                    payload = {"address": addr, **data, "combined_score": combined}
            finally:
                pending_workers -= 1
                if payload is not None:
                    await queue.put(payload)

    async def producer() -> None:
        nonlocal pending_workers, producer_done
        try:
            async with asyncio.TaskGroup() as tg:
                async for tok in stream_mempool_tokens(
                    rpc_url,
                    include_pools=include_pools,
                ):
                    if cpu_usage_threshold is not None:
                        while resource_monitor.get_cpu_usage() > cpu_usage_threshold:
                            await asyncio.sleep(0.05)
                    address = tok["address"] if isinstance(tok, dict) else tok
                    pending_workers += 1
                    tg.create_task(worker(address))
        finally:
            producer_done = True

    async def aggregator() -> None:
        nonlocal pending_workers
        try:
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=flush_interval)
                except asyncio.TimeoutError:
                    if limit_value > 0:
                        if not flushed_results and heap:
                            await _flush_heap()
                            break
                    else:
                        await _flush_heap()
                        if producer_done and pending_workers <= 0 and queue.empty():
                            break
                    continue
                else:
                    _push_event(event)
                    if queue.empty():
                        if limit_value > 0:
                            if (
                                not flushed_results
                                and pending_workers <= 0
                                and producer_done
                            ):
                                await _flush_heap()
                                break
                        elif pending_workers <= 0:
                            await _flush_heap()
        finally:
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await _flush_heap()
            with contextlib.suppress(Exception, asyncio.CancelledError):
                await results.put(sentinel)

    producer_task = asyncio.create_task(producer())
    aggregator_task = asyncio.create_task(aggregator())

    try:
        while True:
            item = await results.get()
            if item is sentinel:
                break
            yield item  # type: ignore[misc]
    finally:
        producer_done = True
        for task in (producer_task, aggregator_task):
            if not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    if adjust_task:
        adjust_task.cancel()
        with contextlib.suppress(Exception):
            await adjust_task
    try:
        _metrics_sub.__exit__(None, None, None)
    except Exception as exc:
        logger.warning("metrics subscription exit failed: %s", exc)


async def stream_ranked_mempool_tokens_with_depth(
    rpc_url: str,
    *,
    depth_threshold: float = 0.0,
    **kwargs,
) -> AsyncGenerator[Dict[str, float], None]:
    """Yield ranked mempool tokens enriched with depth metrics."""
    timeout = MEMPOOL_DEPTH_STREAM_TIMEOUT
    stream = stream_ranked_mempool_tokens(rpc_url, **kwargs)
    try:
        while True:
            try:
                next_event = stream.__anext__()
                if timeout > 0:
                    event = await asyncio.wait_for(next_event, timeout=timeout)
                else:
                    event = await next_event
            except StopAsyncIteration:
                break
            except asyncio.TimeoutError:
                logger.warning(
                    "mempool depth stream timed out after %.2fs", timeout
                )
                with contextlib.suppress(Exception):
                    await stream.aclose()
                break
            except asyncio.CancelledError:
                with contextlib.suppress(Exception):
                    await stream.aclose()
                raise
            token = event["address"]
            depth, _imb, txr = order_book_ws.snapshot(token)
            event["depth"] = depth
            event["depth_tx_rate"] = txr
            event["combined_score"] += depth + txr
            if depth >= depth_threshold:
                yield event
    finally:
        with contextlib.suppress(Exception):
            await stream.aclose()
