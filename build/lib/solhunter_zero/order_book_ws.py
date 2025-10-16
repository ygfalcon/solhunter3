from __future__ import annotations

import asyncio
import logging
import mmap
import os
import time
import threading
from typing import AsyncGenerator, Dict, Any, Optional, Tuple

try:
    from watchfiles import awatch
except Exception:  # pragma: no cover - optional dependency
    awatch = None

import aiohttp
from .http import get_session, loads, dumps
from . import event_pb2 as pb

logger = logging.getLogger(__name__)

# how long cached order book entries remain valid
ORDERBOOK_CACHE_TTL = float(os.getenv("ORDERBOOK_CACHE_TTL", "5") or 5)

# Cache of latest bid/ask and tx rate per token along with timestamp
_DEPTH_CACHE: Dict[str, Tuple[float, Dict[str, float]]] = {}

_MMAP_PATH = os.getenv("DEPTH_MMAP_PATH", "/tmp/depth_service.mmap")

# Interval in seconds for polling the mmap file modification time
DEPTH_MMAP_POLL_INTERVAL = float(os.getenv("DEPTH_MMAP_POLL_INTERVAL", "1") or 1)

_watch_task: asyncio.Task | None = None
_watch_loop: asyncio.AbstractEventLoop | None = None


async def _watch_mmap(interval: float) -> None:
    """Watch :data:`_MMAP_PATH` and clear :data:`_DEPTH_CACHE` on changes."""
    if awatch is None:  # pragma: no cover - watchfiles not installed
        return
    async for _ in awatch(
        _MMAP_PATH, poll_delay_ms=int(interval * 1000), debounce=0
    ):
        _DEPTH_CACHE.clear()


def start_mmap_watch(interval: float = DEPTH_MMAP_POLL_INTERVAL) -> asyncio.Task:
    """Start the mmap watch task if not already running."""
    global _watch_task, _watch_loop
    if _watch_task is not None and not _watch_task.done():
        return _watch_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if _watch_loop is None:
            _watch_loop = asyncio.new_event_loop()
            threading.Thread(target=_watch_loop.run_forever, daemon=True).start()
        loop = _watch_loop
        _watch_task = asyncio.run_coroutine_threadsafe(_watch_mmap(interval), loop)
        return _watch_task
    _watch_task = loop.create_task(_watch_mmap(interval))
    return _watch_task


def stop_mmap_watch() -> None:
    """Cancel the running mmap watch task, if any."""
    global _watch_task
    if _watch_task is not None:
        _watch_task.cancel()
        _watch_task = None


def _snapshot_from_mmap(token: str) -> tuple[float, float, float]:
    try:
        with open(_MMAP_PATH, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as m:
                raw = bytes(m).rstrip(b"\x00")
                if not raw:
                    return 0.0, 0.0, 0.0
                data = loads(raw)
                entry = data.get(token)
                if not entry:
                    return 0.0, 0.0, 0.0
                rate = float(entry.get("tx_rate", 0.0))
                if "bids" in entry and "asks" in entry:
                    bids = float(entry.get("bids", 0.0))
                    asks = float(entry.get("asks", 0.0))
                else:
                    total_bids = 0.0
                    total_asks = 0.0
                    for k, v in entry.items():
                        if k == "tx_rate":
                            continue
                        if isinstance(v, dict):
                            total_bids += float(v.get("bids", 0.0))
                            total_asks += float(v.get("asks", 0.0))
                    bids = total_bids
                    asks = total_asks
                depth = bids + asks
                imb = (bids - asks) / depth if depth else 0.0
                return depth, imb, rate
    except Exception as exc:
        logger.exception("Failed to read depth snapshot", exc_info=exc)
        return 0.0, 0.0, 0.0


def snapshot(token: str) -> tuple[float, float, float]:
    """Return current depth, imbalance and mempool rate for ``token``."""
    cached = _DEPTH_CACHE.get(token)
    if not cached:
        return _snapshot_from_mmap(token)
    ts, data = cached
    if time.time() - ts > ORDERBOOK_CACHE_TTL:
        return _snapshot_from_mmap(token)
    bids = float(data.get("bids", 0.0))
    asks = float(data.get("asks", 0.0))
    rate = float(data.get("tx_rate", 0.0))
    depth = bids + asks
    imbalance = (bids - asks) / depth if depth else 0.0
    return depth, imbalance, rate


async def stream_order_book(
    url: str,
    *,
    rate_limit: float = 0.1,
    max_updates: Optional[int] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Yield depth updates from ``url`` with reconnection and rate limiting."""
    start_mmap_watch()
    if url.startswith("ipc://"):
        if "?" not in url[6:]:
            raise ValueError("ipc url must include token")
        path, token = url[6:].split("?", 1)
        count = 0
        backoff = 1.0
        max_backoff = 30.0
        while True:
            try:
                backoff = 1.0
                reader, writer = await asyncio.open_unix_connection(path)
                payload = dumps({"cmd": "snapshot", "token": token})
                writer.write(payload if isinstance(payload, (bytes, bytearray)) else payload.encode())
                await writer.drain()
                data = await reader.read()
                writer.close()
                await writer.wait_closed()
                try:
                    info = loads(data)
                    if "bids" in info and "asks" in info:
                        bids = float(info.get("bids", 0.0))
                        asks = float(info.get("asks", 0.0))
                        rate = float(info.get("tx_rate", 0.0))
                    else:
                        bids = 0.0
                        asks = 0.0
                        rate = float(info.get("tx_rate", 0.0))
                        for v in info.values():
                            if isinstance(v, dict):
                                bids += float(v.get("bids", 0.0))
                                asks += float(v.get("asks", 0.0))
                    now = time.time()
                    # expire stale entries
                    for k, (ts, _) in list(_DEPTH_CACHE.items()):
                        if now - ts > ORDERBOOK_CACHE_TTL:
                            _DEPTH_CACHE.pop(k, None)
                    _DEPTH_CACHE[token] = (now, {"bids": bids, "asks": asks, "tx_rate": rate})
                    depth, imb, txr = snapshot(token)
                    yield {
                        "token": token,
                        "depth": depth,
                        "imbalance": imb,
                        "tx_rate": txr,
                    }
                except Exception as exc:
                    logger.error("Failed to parse IPC depth update: %s", exc)
                count += 1
                if max_updates is not None and count >= max_updates:
                    return
                if rate_limit > 0:
                    await asyncio.sleep(rate_limit)
            except Exception as exc:  # pragma: no cover - IPC errors
                logger.error("IPC order book error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
    else:
        count = 0
        backoff = 1.0
        max_backoff = 30.0
        while True:
            try:
                session = await get_session()
                async with session.ws_connect(url) as ws:
                    backoff = 1.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = loads(msg.data)
                            except Exception as exc:
                                logger.error("Failed to parse depth message: %s", exc)
                                continue
                            token = data.get("token")
                            if not token:
                                continue
                            if "bids" in data and "asks" in data:
                                bids = float(data.get("bids", 0.0))
                                asks = float(data.get("asks", 0.0))
                                rate = float(data.get("tx_rate", 0.0))
                            else:
                                bids = 0.0
                                asks = 0.0
                                rate = float(data.get("tx_rate", 0.0))
                                for v in data.values():
                                    if isinstance(v, dict):
                                        bids += float(v.get("bids", 0.0))
                                        asks += float(v.get("asks", 0.0))
                                updates = {token: {"bids": bids, "asks": asks, "tx_rate": rate}}
                        elif msg.type == aiohttp.WSMsgType.BINARY:
                                try:
                                    ev = pb.Event()
                                    ev.ParseFromString(msg.data)
                                    if ev.topic == "depth_update" and ev.HasField("depth_update"):
                                        entries = ev.depth_update.entries
                                    elif ev.topic == "depth_diff" and ev.HasField("depth_diff"):
                                        entries = ev.depth_diff.entries
                                    else:
                                        continue
                                except Exception:
                                    continue
                                updates = {
                                    tok: {
                                        "bids": e.bids,
                                        "asks": e.asks,
                                        "tx_rate": e.tx_rate,
                                    }
                                    for tok, e in entries.items()
                                }
                        else:
                            continue

                            now = time.time()
                            for k, (ts, _) in list(_DEPTH_CACHE.items()):
                                if now - ts > ORDERBOOK_CACHE_TTL:
                                    _DEPTH_CACHE.pop(k, None)
                            for token, info in updates.items():
                                _DEPTH_CACHE[token] = (
                                    now,
                                    info,
                                )
                                depth, imb, txr = snapshot(token)
                                yield {
                                    "token": token,
                                    "depth": depth,
                                    "imbalance": imb,
                                    "tx_rate": txr,
                                }
                            count += 1
                            if max_updates is not None and count >= max_updates:
                                return
                            if rate_limit > 0:
                                await asyncio.sleep(rate_limit)
            except Exception as exc:  # pragma: no cover - network errors
                logger.error("Order book websocket error: %s", exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
