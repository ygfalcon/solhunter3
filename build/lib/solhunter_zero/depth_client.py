import os
import mmap

from .system import set_rayon_threads

# Ensure Rayon thread pool is configured for the Rust FFI
set_rayon_threads()

import asyncio
import time
import atexit
from contextlib import suppress
import struct
from typing import AsyncGenerator, Dict, Any, Optional, Tuple
from . import routeffi

import aiohttp
from .http import get_session, loads, dumps
from .util import sanitize_priority_urls

from .event_bus import publish, subscription
from .config import get_depth_ws_addr
from . import event_pb2 as pb

from . import order_book_ws

DEPTH_SERVICE_SOCKET = os.getenv("DEPTH_SERVICE_SOCKET", "/tmp/depth_service.sock")

MMAP_PATH = os.getenv("DEPTH_MMAP_PATH", "/tmp/depth_service.mmap")
DEPTH_WS_ADDR, DEPTH_WS_PORT = get_depth_ws_addr()

# Persistent mmap for depth snapshots
_MMAP: mmap.mmap | None = None
_MMAP_SIZE: int = 0
# Cached token offsets from the IDX1 header
TOKEN_OFFSETS: Dict[str, Tuple[int, int]] = {}
_TOKEN_MAP_SIZE: int = 0


def open_mmap() -> mmap.mmap | None:
    """Return a persistent mmap for :data:`MMAP_PATH`."""
    global _MMAP, _MMAP_SIZE, TOKEN_OFFSETS, _TOKEN_MAP_SIZE
    try:
        size = os.path.getsize(MMAP_PATH)
    except OSError:
        size = 0
    if size == 0:
        close_mmap()
        return None
    if _MMAP is not None:
        if getattr(_MMAP, "closed", False) or _MMAP_SIZE != size:
            try:
                _MMAP.close()
            except Exception:
                pass
            _MMAP = None
            # Invalidate IDX1 token cache on size change
            TOKEN_OFFSETS.clear()
            global _TOKEN_MAP_SIZE
            _TOKEN_MAP_SIZE = 0
    if _MMAP is None:
        f = open(MMAP_PATH, "rb")
        _MMAP = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        _MMAP_SIZE = size
        f.close()
        TOKEN_OFFSETS.clear()
        _TOKEN_MAP_SIZE = 0
    return _MMAP


def close_mmap() -> None:
    """Close the persistent depth mmap."""
    global _MMAP, _MMAP_SIZE, TOKEN_OFFSETS, _TOKEN_MAP_SIZE
    if _MMAP is not None and not getattr(_MMAP, "closed", False):
        try:
            _MMAP.close()
        except Exception:
            pass
    _MMAP = None
    _MMAP_SIZE = 0
    TOKEN_OFFSETS.clear()
    _TOKEN_MAP_SIZE = 0


atexit.register(close_mmap)


def _build_token_offsets(buf: memoryview) -> None:
    """Parse IDX1 header and populate :data:`TOKEN_OFFSETS`."""
    global TOKEN_OFFSETS, _TOKEN_MAP_SIZE
    TOKEN_OFFSETS.clear()
    if len(buf) < 8 or bytes(buf[:4]) != b"IDX1":
        _TOKEN_MAP_SIZE = _MMAP_SIZE
        return
    count = int.from_bytes(buf[4:8], "little")
    off = 8
    for _ in range(count):
        if off + 2 > len(buf):
            break
        tlen = int.from_bytes(buf[off : off + 2], "little")
        off += 2
        if off + tlen > len(buf):
            break
        tok = bytes(buf[off : off + tlen]).decode()
        off += tlen
        if off + 8 > len(buf):
            break
        data_off = int.from_bytes(buf[off : off + 4], "little")
        data_len = int.from_bytes(buf[off + 4 : off + 8], "little")
        off += 8
        TOKEN_OFFSETS[tok] = (data_off, data_len)
    _TOKEN_MAP_SIZE = _MMAP_SIZE


def _decode_token_agg_py(buf: bytes) -> Dict[str, Any]:
    """Decode a :class:`TokenAgg` serialized with fixed-int bincode."""
    off = 0
    dex = {}
    if off + 8 > len(buf):
        return {}
    count = struct.unpack_from("<Q", buf, off)[0]
    off += 8
    for _ in range(count):
        if off + 8 > len(buf):
            return {}
        klen = struct.unpack_from("<Q", buf, off)[0]
        off += 8
        if off + klen > len(buf):
            return {}
        key = buf[off : off + klen].decode()
        off += klen
        if off + 24 > len(buf):
            return {}
        bids, asks, tx_rate = struct.unpack_from("<ddd", buf, off)
        off += 24
        dex[key] = {"bids": bids, "asks": asks, "tx_rate": tx_rate}
    if off + 24 > len(buf):
        return {}
    bids, asks, tx_rate = struct.unpack_from("<ddd", buf, off)
    off += 24
    if off + 8 > len(buf):
        return {}
    ts = struct.unpack_from("<q", buf, off)[0]
    return {
        "dex": dex,
        "bids": bids,
        "asks": asks,
        "tx_rate": tx_rate,
        "ts": ts,
    }


_decode_token_agg = routeffi.decode_token_agg or _decode_token_agg_py


def _decode_adj_matrix_py(buf: bytes) -> tuple[list[str], list[float]]:
    off = 0
    if off + 8 > len(buf):
        return [], []
    count = struct.unpack_from("<Q", buf, off)[0]
    off += 8
    venues = []
    for _ in range(count):
        if off + 8 > len(buf):
            return [], []
        l = struct.unpack_from("<Q", buf, off)[0]
        off += 8
        if off + l > len(buf):
            return [], []
        venues.append(buf[off : off + l].decode())
        off += l
    if off + 8 > len(buf):
        return [], []
    mlen = struct.unpack_from("<Q", buf, off)[0]
    off += 8
    if off + 8 * mlen > len(buf):
        return venues, []
    matrix = list(struct.unpack_from(f"<{mlen}d", buf, off))
    return venues, matrix


_decode_adj_matrix = _decode_adj_matrix_py


# Depth snapshot caching
DEPTH_CACHE_TTL = float(os.getenv("DEPTH_CACHE_TTL", "0.5"))
SNAPSHOT_CACHE: Dict[str, Tuple[float, float, Dict[str, Dict[str, float]]]] = {}
WS_SNAPSHOT: Dict[str, Dict[str, Any]] = {}

# IPC connection pooling
_CONNECTIONS: Dict[str, "_IPCClient"] = {}


class _IPCClient:
    """Maintain a persistent UNIX socket connection."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def _ensure(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if self.writer is None or getattr(self.writer, "is_closing", lambda: False)():
            self.reader, self.writer = await asyncio.open_unix_connection(self.path)
        return self.reader, self.writer

    async def request(
        self, payload: Dict[str, Any], timeout: float | None = None
    ) -> bytes:
        reader, writer = await self._ensure()
        data = dumps(payload)
        writer.write(data if isinstance(data, (bytes, bytearray)) else data.encode())
        await writer.drain()
        if timeout is not None:
            data = await asyncio.wait_for(reader.read(), timeout)
        else:
            data = await reader.read()
        return data

    async def close(self) -> None:
        if (
            self.writer is not None
            and not getattr(self.writer, "is_closing", lambda: False)()
        ):
            self.writer.close()
            with suppress(Exception):
                await self.writer.wait_closed()
        self.reader = None
        self.writer = None


async def get_ipc_client(socket_path: str = DEPTH_SERVICE_SOCKET) -> _IPCClient:
    client = _CONNECTIONS.get(socket_path)
    if client is None:
        client = _IPCClient(socket_path)
        _CONNECTIONS[socket_path] = client
    return client


async def close_ipc_clients() -> None:
    for client in list(_CONNECTIONS.values()):
        with suppress(Exception):
            await client.close()
    _CONNECTIONS.clear()


def _reload_depth(cfg) -> None:
    global DEPTH_WS_ADDR, DEPTH_WS_PORT
    DEPTH_WS_ADDR, DEPTH_WS_PORT = get_depth_ws_addr(cfg)


subscription("config_updated", _reload_depth).__enter__()


async def stream_depth(
    token: str,
    *,
    rate_limit: float = 0.1,
    max_updates: Optional[int] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Stream aggregated depth and mempool rate from the Rust service."""
    url = f"ipc://{DEPTH_SERVICE_SOCKET}?{token}"
    async for data in order_book_ws.stream_order_book(
        url, rate_limit=rate_limit, max_updates=max_updates
    ):
        yield data


async def stream_depth_ws(
    token: str,
    *,
    rate_limit: float = 0.1,
    max_updates: Optional[int] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Stream depth updates from the WebSocket server with mmap fallback."""
    url = f"ws://{DEPTH_WS_ADDR}:{DEPTH_WS_PORT}"
    count = 0
    was_connected = False
    while True:
        try:
            session = await get_session()
            async with session.ws_connect(url) as ws:
                publish(
                    "depth_service_status",
                    {"status": "reconnected" if was_connected else "connected"},
                )
                was_connected = True
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.BINARY and msg.data.startswith(
                        b"IDX1"
                    ):
                        buf = msg.data
                        count = int.from_bytes(buf[4:8], "little")
                        off = 8
                        offsets = {}
                        for _ in range(count):
                            if off + 2 > len(buf):
                                break
                            tlen = int.from_bytes(buf[off : off + 2], "little")
                            off += 2
                            if off + tlen > len(buf):
                                break
                            tok = buf[off : off + tlen].decode()
                            off += tlen
                            if off + 8 > len(buf):
                                break
                            data_off = int.from_bytes(buf[off : off + 4], "little")
                            data_len = int.from_bytes(buf[off + 4 : off + 8], "little")
                            off += 8
                            offsets[tok] = (data_off, data_len)
                        entry_off = offsets.get(token)
                        if not entry_off:
                            continue
                        data_off, data_len = entry_off
                        if data_off + data_len > len(buf):
                            continue
                        try:
                            entry = _decode_token_agg(
                                buf[data_off : data_off + data_len]
                            )
                        except Exception:
                            continue
                    elif msg.type == aiohttp.WSMsgType.BINARY:
                        try:
                            ev = pb.Event()
                            ev.ParseFromString(msg.data)
                            if ev.topic != "depth_update" or not ev.HasField(
                                "depth_update"
                            ):
                                continue
                            e = ev.depth_update.entries.get(token)
                            if e is None:
                                continue
                            entry = {
                                "bids": e.bids,
                                "asks": e.asks,
                                "tx_rate": e.tx_rate,
                                "dex": {
                                    d: {
                                        "bids": di.bids,
                                        "asks": di.asks,
                                        "tx_rate": di.tx_rate,
                                    }
                                    for d, di in e.dex.items()
                                },
                            }
                        except Exception:
                            continue
                    else:
                        continue

                    bids = float(entry.get("bids", 0.0)) if "bids" in entry else 0.0
                    asks = float(entry.get("asks", 0.0)) if "asks" in entry else 0.0
                    rate = float(entry.get("tx_rate", 0.0))
                    for v in entry.values():
                        if isinstance(v, dict):
                            bids += float(v.get("bids", 0.0))
                            asks += float(v.get("asks", 0.0))
                    depth = bids + asks
                    imb = (bids - asks) / depth if depth else 0.0
                    yield {
                        "token": token,
                        "depth": depth,
                        "imbalance": imb,
                        "tx_rate": rate,
                    }
                    count += 1
                    if max_updates is not None and count >= max_updates:
                        publish("depth_service_status", {"status": "disconnected"})
                        return
                    if rate_limit > 0:
                        await asyncio.sleep(rate_limit)
        except Exception:
            if was_connected:
                publish("depth_service_status", {"status": "disconnected"})
                was_connected = False
            venues, rate = snapshot(token)
            bids = sum(float(v.get("bids", 0.0)) for v in venues.values())
            asks = sum(float(v.get("asks", 0.0)) for v in venues.values())
            depth = bids + asks
            imb = (bids - asks) / depth if depth else 0.0
            yield {
                "token": token,
                "depth": depth,
                "imbalance": imb,
                "tx_rate": rate,
            }
            count += 1
            if max_updates is not None and count >= max_updates:
                return
            sleep_time = max(rate_limit, DEPTH_CACHE_TTL)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        else:
            if was_connected:
                publish("depth_service_status", {"status": "disconnected"})
                was_connected = False


async def listen_depth_ws(*, max_updates: Optional[int] = None) -> None:
    """Connect to the depth websocket and publish updates."""

    url = f"ws://{DEPTH_WS_ADDR}:{DEPTH_WS_PORT}"
    count = 0
    was_connected = False
    while True:
        try:
            session = await get_session()
            async with session.ws_connect(url) as ws:
                publish(
                    "depth_service_status",
                    {"status": "reconnected" if was_connected else "connected"},
                )
                was_connected = True
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.BINARY:
                        continue
                    try:
                        ev = pb.Event()
                        ev.ParseFromString(msg.data)
                        if ev.topic == "depth_update" and ev.HasField("depth_update"):
                            entries = ev.depth_update.entries
                            topic = "depth_update"
                        elif ev.topic == "depth_diff" and ev.HasField("depth_diff"):
                            entries = ev.depth_diff.entries
                            topic = "depth_diff"
                        else:
                            continue
                    except Exception:
                        continue
                    data = {
                        tok: {
                            "bids": e.bids,
                            "asks": e.asks,
                            "tx_rate": e.tx_rate,
                            "dex": {
                                d: {
                                    "bids": di.bids,
                                    "asks": di.asks,
                                    "tx_rate": di.tx_rate,
                                }
                                for d, di in e.dex.items()
                            },
                        }
                        for tok, e in entries.items()
                    }
                    if topic == "depth_update":
                        WS_SNAPSHOT.clear()
                    for k, v in data.items():
                        WS_SNAPSHOT[k] = v
                    publish(topic, data)
                    count += 1
                    if max_updates is not None and count >= max_updates:
                        publish("depth_service_status", {"status": "disconnected"})
                        return
        except Exception:
            if was_connected:
                publish("depth_service_status", {"status": "disconnected"})
                was_connected = False
            await asyncio.sleep(1.0)
        else:
            if was_connected:
                publish("depth_service_status", {"status": "disconnected"})
                was_connected = False


def snapshot(token: str) -> Tuple[Dict[str, Dict[str, float]], float]:
    """Return order book depth per venue and mempool rate."""
    now = time.monotonic()
    cached = SNAPSHOT_CACHE.get(token)
    if cached and now - cached[0] < DEPTH_CACHE_TTL:
        return cached[2], cached[1]
    try:
        m = open_mmap()
        if m is None:
            return {}, 0.0
        buf = memoryview(m)
        if _TOKEN_MAP_SIZE != _MMAP_SIZE or not TOKEN_OFFSETS:
            _build_token_offsets(buf)
        offset = TOKEN_OFFSETS.get(token)
        if offset:
            data_off, data_len = offset
            if data_off + data_len <= len(buf):
                slice_bytes = bytes(buf[data_off : data_off + data_len]).rstrip(b"\x00")
                if not slice_bytes:
                    return {}, 0.0
                entry = _decode_token_agg(slice_bytes)
                rate = float(entry.get("tx_rate", 0.0))
                venues = {
                    d: {
                        "bids": float(i.get("bids", 0.0)),
                        "asks": float(i.get("asks", 0.0)),
                    }
                    for d, i in entry.get("dex", {}).items()
                }
                SNAPSHOT_CACHE[token] = (now, rate, venues)
                return venues, rate
            return {}, 0.0
        if len(buf) >= 8 and bytes(buf[:4]) == b"IDX1":
            return {}, 0.0
        # Fallback to legacy JSON structure
        raw = bytes(buf).rstrip(b"\x00")
        if not raw:
            return {}, 0.0
        data = loads(raw)
        entry = data.get(token)
        if not entry:
            return {}, 0.0
        rate = float(entry.get("tx_rate", 0.0))
        venues = {
            d: {
                "bids": float(i.get("bids", 0.0)),
                "asks": float(i.get("asks", 0.0)),
            }
            for d, i in entry.items()
            if isinstance(i, dict)
        }
        SNAPSHOT_CACHE[token] = (now, rate, venues)
        return venues, rate
    except Exception:
        return {}, 0.0


def get_adjacency_matrix(token: str) -> tuple[list[str], list[list[float]]] | None:
    """Return the precomputed adjacency matrix for ``token`` if available."""

    try:
        m = open_mmap()
        if m is None:
            return None
        buf = memoryview(m)
        if _TOKEN_MAP_SIZE != _MMAP_SIZE or not TOKEN_OFFSETS:
            _build_token_offsets(buf)
        offset = TOKEN_OFFSETS.get(f"adj_{token}")
        if not offset:
            return None
        data_off, data_len = offset
        if data_off + data_len > len(buf):
            return None
        slice_bytes = bytes(buf[data_off : data_off + data_len])
        if not slice_bytes.strip(b"\x00"):
            return None
        venues, matrix = _decode_adj_matrix(slice_bytes)
        n = len(venues)
        if n == 0 or len(matrix) != n * n:
            return None
        mat2d = [matrix[i * n : (i + 1) * n] for i in range(n)]
        return venues, mat2d
    except Exception:
        return None


async def submit_signed_tx(
    tx_b64: str,
    *,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
) -> Optional[str]:
    """Forward a pre-signed transaction to the Rust service."""

    client = await get_ipc_client(socket_path)
    payload = {"cmd": "signed_tx", "tx": tx_b64}
    data = await client.request(payload, timeout)
    if not data:
        return None
    try:
        resp = loads(data)
    except Exception:
        return None
    return resp.get("signature")


async def prepare_signed_tx(
    msg_b64: str,
    *,
    priority_fee: int | None = None,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
) -> Optional[str]:
    """Request a signed transaction from a template message."""

    client = await get_ipc_client(socket_path)
    payload: Dict[str, Any] = {"cmd": "prepare", "msg": msg_b64}
    if priority_fee is not None:
        payload["priority_fee"] = int(priority_fee)
    data = await client.request(payload, timeout)
    if not data:
        return None
    try:
        resp = loads(data)
    except Exception:
        return None
    return resp.get("tx")


async def submit_tx_batch(
    txs: list[str],
    *,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
) -> list[str] | None:
    """Submit multiple pre-signed transactions at once."""

    client = await get_ipc_client(socket_path)
    payload = {"cmd": "batch", "txs": txs}
    data = await client.request(payload, timeout)
    if not data:
        return None
    try:
        resp = loads(data)
    except Exception:
        return None
    if isinstance(resp, list):
        return [str(s) for s in resp]
    return None


async def submit_raw_tx(
    tx_b64: str,
    *,
    priority_rpc: list[str] | None = None,
    priority_fee: int | None = None,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
) -> Optional[str]:
    """Submit a transaction with optional priority RPC endpoints."""

    client = await get_ipc_client(socket_path)
    payload: Dict[str, Any] = {"cmd": "raw_tx", "tx": tx_b64}
    cleaned_priority = sanitize_priority_urls(priority_rpc)
    if cleaned_priority:
        payload["priority_rpc"] = cleaned_priority
    if priority_fee is not None:
        payload["priority_fee"] = int(priority_fee)
    data = await client.request(payload, timeout)
    if not data:
        return None
    try:
        resp = loads(data)
    except Exception:
        return None
    return resp.get("signature")


async def auto_exec(
    token: str,
    threshold: float,
    txs: list[str],
    *,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
) -> bool:
    """Register auto-execution triggers with the Rust service."""

    client = await get_ipc_client(socket_path)
    payload: Dict[str, Any] = {
        "cmd": "auto_exec",
        "token": token,
        "threshold": threshold,
        "txs": list(txs),
    }
    data = await client.request(payload, timeout)
    if not data:
        return False
    try:
        resp = loads(data)
    except Exception:
        return False
    return bool(resp.get("ok"))


async def best_route(
    token: str,
    amount: float,
    *,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
    max_hops: int | None = None,
) -> tuple[list[str], float, float] | None:
    """Return the optimal trading path from the Rust service."""

    client = await get_ipc_client(socket_path)
    payload: Dict[str, Any] = {"cmd": "route", "token": token, "amount": amount}
    if max_hops is not None:
        payload["max_hops"] = int(max_hops)
    data = await client.request(payload, timeout)
    if not data:
        return None


async def cached_route(
    token: str,
    amount: float,
    *,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    timeout: float | None = None,
    max_hops: int | None = None,
) -> tuple[list[str], float, float] | None:
    """Request a precomputed trading path from the Rust service."""

    client = await get_ipc_client(socket_path)
    req = pb.RouteRequest(token=token, amount=amount)
    if max_hops is not None:
        req.max_hops = int(max_hops)
    reader, writer = await client._ensure()
    writer.write(req.SerializeToString())
    await writer.drain()
    if timeout is not None:
        data = await asyncio.wait_for(reader.read(), timeout)
    else:
        data = await reader.read()
    if not data:
        return None
    try:
        resp = pb.RouteResponse()
        resp.ParseFromString(data)
        return list(resp.path), float(resp.profit), float(resp.slippage)
    except Exception:
        return None
    try:
        resp = loads(data)
        path = [str(p) for p in resp.get("path", [])]
        profit = float(resp.get("profit", 0.0))
        slippage = float(resp.get("slippage", 0.0))
        return path, profit, slippage
    except Exception:
        return None
