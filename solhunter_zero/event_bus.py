"""Event bus helpers for serializing and broadcasting application events."""

import asyncio
import inspect
import logging
import errno

from urllib.parse import urlparse

from .util import install_uvloop

install_uvloop()

try:
    import orjson as json  # type: ignore
    _USE_ORJSON = True
except Exception:  # pragma: no cover - optional dependency
    import json  # type: ignore
    _USE_ORJSON = False
import os
import zlib
import mmap
import threading
import time

try:  # optional compression libraries
    import lz4.frame
    _HAS_LZ4 = True
except Exception:  # pragma: no cover - optional dependency
    lz4 = None
    _HAS_LZ4 = False

try:
    import zstandard as zstd
    _HAS_ZSTD = True
    _ZSTD_COMPRESSOR = zstd.ZstdCompressor()
    _ZSTD_DECOMPRESSOR = zstd.ZstdDecompressor()
except Exception:  # pragma: no cover - optional dependency
    zstd = None
    _HAS_ZSTD = False
    _ZSTD_COMPRESSOR = None
    _ZSTD_DECOMPRESSOR = None
from contextlib import contextmanager
from collections import defaultdict, deque
from typing import Any, Awaitable, Callable, Dict, Generator, Iterable, List, Mapping, Set, Sequence, cast

try:
    import msgpack
except Exception:  # pragma: no cover - optional dependency
    msgpack = None

# default serialization format for JSON events
_EVENT_SERIALIZATION = os.getenv("EVENT_SERIALIZATION")
if not _EVENT_SERIALIZATION:
    _EVENT_SERIALIZATION = "msgpack" if msgpack is not None else "json"
_EVENT_SERIALIZATION = _EVENT_SERIALIZATION.lower()
_USE_MSGPACK = msgpack is not None and _EVENT_SERIALIZATION == "msgpack"

logger = logging.getLogger(__name__)

_EVENT_SCHEMA_VERSION = os.getenv("EVENT_SCHEMA_VERSION") or "v3"
_ACCEPTED_SCHEMA = os.getenv("EVENT_SCHEMA_VERSION_ACCEPT")
if _ACCEPTED_SCHEMA:
    _EXPECTED_EVENT_SCHEMA_VERSIONS = tuple(
        v.strip() for v in _ACCEPTED_SCHEMA.split(",") if v.strip()
    )
else:
    _EXPECTED_EVENT_SCHEMA_VERSIONS = (_EVENT_SCHEMA_VERSION,)

DEFAULT_WS_URL = "ws://127.0.0.1:8779"
_DEFAULT_WS = urlparse(DEFAULT_WS_URL)
_WS_LISTEN_HOST = _DEFAULT_WS.hostname or "127.0.0.1"
_WS_LISTEN_PORT = _DEFAULT_WS.port or 8779

try:  # optional redis / nats support
    import redis.asyncio as aioredis  # type: ignore
    from redis.exceptions import ConnectionError as RedisConnectionError  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aioredis = None

    class RedisConnectionError(Exception):
        pass

try:
    import nats  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    nats = None


def _broker_preflight() -> None:
    """Ensure optional broker dependencies are installed when configured."""
    urls: list[str] = []
    url = os.getenv("BROKER_URL")
    if url:
        urls.append(url)
    more = os.getenv("BROKER_URLS")
    if more:
        urls.extend(u.strip() for u in more.split(",") if u.strip())
    for u in urls:
        if u.startswith(("redis://", "rediss://")) and aioredis is None:
            raise RuntimeError(
                "redis broker configured via BROKER_URL(S) but redis package is not installed"
            )
        if u.startswith("nats://") and nats is None:
            raise RuntimeError(
                "nats broker configured via BROKER_URL(S) but nats-py package is not installed"
            )


_broker_preflight()

from asyncio import Queue

try:
    _EVENT_DEDUPE_TTL = float(os.getenv("EVENT_BUS_DEDUPE_TTL", "30") or 30.0)
except Exception:
    _EVENT_DEDUPE_TTL = 30.0
if _EVENT_DEDUPE_TTL <= 0:
    _EVENT_DEDUPE_TTL = 30.0

_DEDUPE_SEEN: dict[str, float] = {}
_DEDUPE_QUEUE: deque[tuple[float, str]] = deque()
_DEDUPE_LOCK = threading.Lock()


def _normalize_dedupe_key(value: Any | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe_prune(now: float) -> None:
    while _DEDUPE_QUEUE and _DEDUPE_QUEUE[0][0] <= now:
        _, key = _DEDUPE_QUEUE.popleft()
        expiry = _DEDUPE_SEEN.get(key)
        if expiry is not None and expiry <= now:
            _DEDUPE_SEEN.pop(key, None)


def _dedupe_should_drop(key: str) -> bool:
    now = time.time()
    with _DEDUPE_LOCK:
        _dedupe_prune(now)
        expiry = _DEDUPE_SEEN.get(key)
        if expiry is not None and expiry > now:
            return True
        new_expiry = now + _EVENT_DEDUPE_TTL
        _DEDUPE_SEEN[key] = new_expiry
        _DEDUPE_QUEUE.append((new_expiry, key))
    return False


def _dedupe_reset() -> None:
    with _DEDUPE_LOCK:
        _DEDUPE_SEEN.clear()
        _DEDUPE_QUEUE.clear()


from .schemas import validate_message, to_dict
from . import event_pb2 as _pb

try:
    from google.protobuf import struct_pb2 as _struct_pb2
    from google.protobuf.message import DecodeError as _ProtoDecodeError
except Exception:  # pragma: no cover - protobuf optional dependency guard
    _struct_pb2 = None
    _ProtoDecodeError = Exception

pb = cast(Any, _pb)


def _coerce_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _normalize_tags(value: Any) -> list[str]:
    if value is None:
        return []
    result: list[str] = []
    if isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = [value]
    for item in candidates:
        if isinstance(item, str):
            text = item.strip()
        else:
            text = str(item).strip()
        if text:
            if text not in result:
                result.append(text)
    return result


def _sanitize_struct_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Mapping):
        sanitized: Dict[str, Any] = {}
        for key, val in value.items():
            if not isinstance(key, str):
                key = str(key)
            sanitized[key] = _sanitize_struct_value(val)
        return sanitized
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_sanitize_struct_value(item) for item in value]
    return str(value)


def _struct_from_mapping(data: Mapping[str, Any]) -> Any:
    if _struct_pb2 is None:
        return None
    struct = _struct_pb2.Struct()
    sanitized: Dict[str, Any] = {}
    for key, value in data.items():
        if not isinstance(key, str):
            key = str(key)
        sanitized[key] = _sanitize_struct_value(value)
    struct.update(sanitized)
    return struct


def _struct_to_python(message: Any) -> Dict[str, Any]:
    if _struct_pb2 is None or message is None:
        return {}
    if isinstance(message, _struct_pb2.Struct):
        return {key: _struct_to_python(value) for key, value in message.items()}
    if isinstance(message, _struct_pb2.ListValue):
        return [_struct_to_python(item) for item in message]
    if hasattr(_struct_pb2, "Value") and isinstance(message, _struct_pb2.Value):
        kind = message.WhichOneof("kind")
        if kind == "struct_value":
            return _struct_to_python(message.struct_value)
        if kind == "list_value":
            return _struct_to_python(message.list_value)
        return getattr(message, kind)
    return message


def _normalize_discovery_entries(payload: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    root_context = payload if isinstance(payload, Mapping) else None

    def _iter_entries() -> Iterable[tuple[Any, Mapping[str, Any] | None]]:
        if isinstance(payload, Mapping):
            tokens = payload.get("tokens")
            if isinstance(tokens, Sequence) and not isinstance(tokens, (str, bytes, bytearray)):
                for entry in tokens:
                    yield entry, payload
                return
            entries_value = payload.get("entries")
            if isinstance(entries_value, Sequence) and not isinstance(entries_value, (str, bytes, bytearray)):
                for entry in entries_value:
                    yield entry, payload
                return
            yield payload, None
            return
        if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
            for entry in payload:
                yield entry, None
            return
        yield payload, None

    def _merge(source: Mapping[str, Any], dest: dict[str, Any], extra: dict[str, Any]) -> None:
        for key, value in source.items():
            if key in {"topic", "tokens", "entries"}:
                continue
            if key in {"mint", "token", "address"}:
                if not dest.get("mint") and isinstance(value, str) and value.strip():
                    dest["mint"] = value.strip()
                continue
            if key == "source":
                if isinstance(value, str) and value.strip():
                    dest.setdefault("source", value.strip())
                continue
            if key == "score":
                if "score" not in dest:
                    number = _coerce_float(value)
                    if number is not None:
                        dest["score"] = number
                    else:
                        extra.setdefault("score", value)
                continue
            if key == "tx":
                if value is not None and "tx" not in dest:
                    dest["tx"] = str(value)
                continue
            if key in {"ts", "timestamp", "discovered_at"}:
                if "ts" not in dest:
                    number = _coerce_float(value)
                    if number is not None:
                        dest["ts"] = number
                    else:
                        extra.setdefault("ts", value)
                continue
            if key == "tags":
                tags = _normalize_tags(value)
                if tags:
                    existing = dest.get("tags") or []
                    combined = list(dict.fromkeys(list(existing) + tags))
                    dest["tags"] = combined
                continue
            if key == "interface":
                if value is not None and "interface" not in dest:
                    dest["interface"] = str(value)
                continue
            if key == "discovery" and isinstance(value, Mapping):
                existing = dest.get("discovery")
                merged = dict(existing) if isinstance(existing, Mapping) else {}
                merged.update(value)
                dest["discovery"] = merged
                continue
            if key == "attributes" and isinstance(value, Mapping):
                attrs = extra.setdefault("attributes", {})
                if isinstance(attrs, dict):
                    attrs.update(value)
                continue
            extra.setdefault(key, value)

    for entry, context in _iter_entries():
        data: dict[str, Any] = {}
        extra: dict[str, Any] = {}
        if root_context and root_context is not context:
            _merge(root_context, data, extra)
        if isinstance(context, Mapping):
            _merge(context, data, extra)
        if isinstance(entry, Mapping):
            _merge(entry, data, extra)
        elif isinstance(entry, str):
            if entry.strip():
                data.setdefault("mint", entry.strip())
        elif entry is not None:
            data.setdefault("mint", str(entry))
        mint = data.get("mint")
        if not mint:
            continue
        data["mint"] = str(mint)
        source = data.get("source")
        if isinstance(source, str):
            data["source"] = source.strip()
        if "tags" in data:
            tags = _normalize_tags(data.get("tags"))
            if tags:
                data["tags"] = tags
            else:
                data.pop("tags", None)
        discovery = data.get("discovery")
        if isinstance(discovery, Mapping):
            data["discovery"] = dict(discovery)
        else:
            data.pop("discovery", None)
        if extra:
            attrs = extra.get("attributes")
            remaining = {k: v for k, v in extra.items() if k != "attributes"}
            merged_attrs: dict[str, Any] = {}
            if isinstance(attrs, Mapping):
                merged_attrs.update(attrs)
            if remaining:
                merged_attrs.update(remaining)
            if merged_attrs:
                data["attributes"] = merged_attrs
        entries.append(data)
    deduped: dict[str, dict[str, Any]] = {}

    def _should_replace(existing: dict[str, Any], candidate: dict[str, Any]) -> bool:
        existing_score = _coerce_float(existing.get("score"))
        candidate_score = _coerce_float(candidate.get("score"))
        if candidate_score is not None and (
            existing_score is None or candidate_score > existing_score
        ):
            return True
        if existing_score is not None and candidate_score is not None:
            if candidate_score < existing_score:
                return False
        existing_ts = _coerce_float(existing.get("ts"))
        candidate_ts = _coerce_float(candidate.get("ts"))
        if candidate_ts is not None and (
            existing_ts is None or candidate_ts > existing_ts
        ):
            return True
        return False

    for entry in entries:
        key = str(entry.get("mint", "")).strip()
        if not key:
            continue
        if key not in deduped or _should_replace(deduped[key], entry):
            deduped[key] = entry
    return list(deduped.values())

_PB_MAP = {
    "action_executed": getattr(pb, "ActionExecuted", None),
    "weights_updated": getattr(pb, "WeightsUpdated", None),
    "rl_weights": getattr(pb, "RLWeights", None),
    "rl_checkpoint": getattr(pb, "RLCheckpoint", None),
    "portfolio_updated": getattr(pb, "PortfolioUpdated", None),
    "depth_update": getattr(pb, "DepthUpdate", None),
    "depth_diff": getattr(pb, "DepthDiff", None),
    "depth_service_status": getattr(pb, "DepthServiceStatus", None),
    "heartbeat": getattr(pb, "Heartbeat", None),
    "trade_logged": getattr(pb, "TradeLogged", None),
    "rl_metrics": getattr(pb, "RLMetrics", None),
    "system_metrics": getattr(pb, "SystemMetrics", None),
    "price_update": getattr(pb, "PriceUpdate", None),
    "config_updated": getattr(pb, "ConfigUpdated", None),
    "pending_swap": getattr(pb, "PendingSwap", None),
    "remote_system_metrics": getattr(pb, "RemoteSystemMetrics", None),
    "risk_metrics": getattr(pb, "RiskMetrics", None),
    "risk_updated": getattr(pb, "RiskUpdated", None),
    "system_metrics_combined": getattr(pb, "SystemMetricsCombined", None),
    "runtime.log": getattr(pb, "RuntimeLog", None),
    "token_discovered": getattr(pb, "TokenDiscovered", None),
    "memory_sync_request": getattr(pb, "MemorySyncRequest", None),
    "memory_sync_response": getattr(pb, "MemorySyncResponse", None),
    "action_proposal": getattr(pb, "ActionProposal", None),
    "action_decision": getattr(pb, "ActionDecision", None),
    "decision_summary": getattr(pb, "DecisionSummary", None),
    "decision_metrics": getattr(pb, "DecisionMetrics", None),
    "dex_latency_update": getattr(pb, "DexLatencyUpdate", None),
    "runtime.stage_changed": getattr(pb, "RuntimeStageChanged", None),
    "startup_config_load_duration": getattr(pb, "ScalarMetric", None),
    "startup_connectivity_check_duration": getattr(pb, "ScalarMetric", None),
    "startup_depth_service_start_duration": getattr(pb, "ScalarMetric", None),
    "startup_complete": getattr(pb, "StartupComplete", None),
    "virtual_pnl": getattr(pb, "VirtualPnL", None),
}

# compress protobuf messages when broadcasting if enabled
_COMPRESS_EVENTS = os.getenv("COMPRESS_EVENTS")
if _COMPRESS_EVENTS is None:
    COMPRESS_EVENTS = _HAS_ZSTD or _HAS_LZ4
else:
    COMPRESS_EVENTS = _COMPRESS_EVENTS not in ("", "0")

# chosen compression algorithm for protobuf events
_EVENT_COMPRESSION = os.getenv("EVENT_COMPRESSION")
_USE_ZLIB_EVENTS = os.getenv("USE_ZLIB_EVENTS")
EVENT_COMPRESSION_THRESHOLD = int(
    os.getenv("EVENT_COMPRESSION_THRESHOLD", "512") or 512
)
EVENT_COMPRESSION: str | None
if _EVENT_COMPRESSION is None:
    if COMPRESS_EVENTS:
        if _USE_ZLIB_EVENTS:
            EVENT_COMPRESSION = "zlib"
        elif _HAS_ZSTD:
            EVENT_COMPRESSION = "zstd"
        else:
            EVENT_COMPRESSION = "zlib"
    else:
        EVENT_COMPRESSION = None
else:
    comp = _EVENT_COMPRESSION.lower()
    if comp in {"", "none", "0"}:
        EVENT_COMPRESSION = None
    else:
        EVENT_COMPRESSION = comp

# optional mmap ring buffer path for outgoing protobuf frames
_EVENT_BUS_MMAP = os.getenv("EVENT_BUS_MMAP")
_EVENT_BUS_MMAP_SIZE = int(os.getenv("EVENT_BUS_MMAP_SIZE", str(1 << 20)) or (1 << 20))

# how long to buffer mmap writes before flushing (ms)
_EVENT_MMAP_BATCH_MS = int(os.getenv("EVENT_MMAP_BATCH_MS", "5") or 5)
# number of events to batch before flushing
_EVENT_MMAP_BATCH_SIZE = int(os.getenv("EVENT_MMAP_BATCH_SIZE", "16") or 16)

_MMAP_BUFFER: list[bytes] = []
_MMAP_FLUSH_HANDLE = None
_MMAP: mmap.mmap | None = None


def _maybe_decompress(data: bytes) -> bytes:
    """Return decompressed ``data`` if it appears to be compressed."""
    if len(data) > 4 and _HAS_ZSTD and data.startswith(b"\x28\xb5\x2f\xfd"):
        try:
            return _ZSTD_DECOMPRESSOR.decompress(data)
        except Exception:
            return data
    if len(data) > 4 and _HAS_LZ4 and data.startswith(b"\x04\x22\x4d\x18"):
        try:
            return lz4.frame.decompress(data)
        except Exception:
            return data
    if len(data) > 2 and data[0] == 0x78:
        try:
            return zlib.decompress(data)
        except Exception:
            return data
    return data


def _compress_event(data: bytes) -> bytes:
    """Compress ``data`` using the selected algorithm if any."""
    if len(data) < EVENT_COMPRESSION_THRESHOLD:
        return data
    if EVENT_COMPRESSION in {"zstd", "zstandard"} and _HAS_ZSTD:
        return _ZSTD_COMPRESSOR.compress(data)
    if EVENT_COMPRESSION == "lz4" and _HAS_LZ4:
        return lz4.frame.compress(data)
    if EVENT_COMPRESSION == "zlib":
        return zlib.compress(data)
    return data


def _dumps(obj: Any) -> bytes:
    """Serialize ``obj`` to bytes using ``msgpack`` if enabled."""
    if _USE_MSGPACK:
        return msgpack.dumps(obj, use_bin_type=True)
    if _USE_ORJSON:
        return json.dumps(obj)
    return json.dumps(obj).encode()


def _dumps_text(obj: Any) -> str:
    """Serialize ``obj`` to a UTF-8 string regardless of the bus encoding."""

    # ``msgpack`` serialization returns binary data that cannot be fed into the
    # protobuf ``string`` fields used for select events (e.g. ``action_executed``).
    # Instead of relying on the global bus format, always fall back to JSON here
    # so we provide a stable textual representation.
    obj = to_dict(obj)
    try:
        data = json.dumps(obj)
    except TypeError:
        # ``orjson`` returns ``bytes`` whereas the stdlib returns ``str``.  If the
        # object contains unserializable types, try to coerce nested dataclasses to
        # dictionaries via ``to_dict`` recursively.
        data = json.dumps(obj, default=to_dict)
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")
    return data


def _json_loads(data: Any) -> Any:
    """Deserialize ``data`` using JSON regardless of the backend in use."""
    if isinstance(data, memoryview):
        data = data.tobytes()
    if _USE_ORJSON:
        if isinstance(data, (bytes, bytearray)):
            data = bytes(data)
        return json.loads(data)
    if isinstance(data, (bytes, bytearray)):
        data = bytes(data).decode()
    return json.loads(data)


def _loads(data: Any) -> Any:
    """Deserialize ``data`` using the configured serialization format."""
    if _USE_MSGPACK:
        if isinstance(data, str):
            data = data.encode()
        try:
            return msgpack.loads(data, raw=False)
        except Exception as exc:
            try:
                return _json_loads(data)
            except Exception:
                raise exc
    return _json_loads(data)


def open_mmap(path: str | None = None, size: int | None = None) -> mmap.mmap | None:
    """Open or return the configured event mmap ring buffer."""
    global _MMAP
    if _MMAP is not None and not getattr(_MMAP, "closed", False):
        return _MMAP
    if path is None:
        path = _EVENT_BUS_MMAP
    if not path:
        return None
    if size is None:
        size = _EVENT_BUS_MMAP_SIZE
    try:
        dirpath = os.path.dirname(path)
        if dirpath:
            os.makedirs(dirpath, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT)
        try:
            if os.path.getsize(path) < size:
                os.ftruncate(fd, size)
            mm = mmap.mmap(fd, size)
        finally:
            os.close(fd)
        if mm.size() >= 4 and int.from_bytes(mm[:4], "little") == 0:
            mm[:4] = (4).to_bytes(4, "little")
        _MMAP = mm
    except Exception:
        _MMAP = None
    return _MMAP


def close_mmap() -> None:
    """Close the event mmap if open."""
    _flush_mmap_buffer()
    global _MMAP
    if _MMAP is not None:
        try:
            _MMAP.close()
        except Exception:
            logging.getLogger(__name__).exception("Failed to close event mmap")
    _MMAP = None


def _write_frame(mm: mmap.mmap, pos: int, data: bytes) -> int:
    size = mm.size()
    if pos < 4 or pos + 4 + len(data) > size:
        pos = 4
    if len(data) > size - 8:
        data = data[: size - 8]
    mm[pos:pos + 4] = len(data).to_bytes(4, "little")
    mm[pos + 4 : pos + 4 + len(data)] = data
    pos += 4 + len(data)
    if pos >= size:
        pos = 4
    return pos


def _flush_mmap_buffer() -> None:
    global _MMAP_FLUSH_HANDLE
    handle = _MMAP_FLUSH_HANDLE
    _MMAP_FLUSH_HANDLE = None
    if handle is not None:
        try:
            handle.cancel()
        except Exception:
            logging.getLogger(__name__).exception("Failed to cancel mmap flush handle")
    if not _MMAP_BUFFER:
        return
    mm = open_mmap()
    if mm is None or mm.size() < 8:
        _MMAP_BUFFER.clear()
        return
    pos = int.from_bytes(mm[:4], "little") or 4
    for buf in _MMAP_BUFFER:
        pos = _write_frame(mm, pos, buf)
    mm[:4] = pos.to_bytes(4, "little")
    _MMAP_BUFFER.clear()
    try:
        mm.flush()
    except Exception:
        logging.getLogger(__name__).exception("Failed to flush event mmap buffer")
        raise


def _mmap_write(data: bytes) -> None:
    global _MMAP_FLUSH_HANDLE
    if len(data) == 0:
        return
    if _EVENT_MMAP_BATCH_MS <= 0 and _EVENT_MMAP_BATCH_SIZE <= 1:
        mm = open_mmap()
        if mm is None or mm.size() < 8:
            return
        pos = int.from_bytes(mm[:4], "little") or 4
        pos = _write_frame(mm, pos, data)
        mm[:4] = pos.to_bytes(4, "little")
        try:
            mm.flush()
        except Exception:
            logging.getLogger(__name__).exception("Failed to flush event mmap")
            raise
        return

    _MMAP_BUFFER.append(data)
    if _EVENT_MMAP_BATCH_SIZE > 0 and len(_MMAP_BUFFER) >= _EVENT_MMAP_BATCH_SIZE:
        _flush_mmap_buffer()
        return
    if _EVENT_MMAP_BATCH_MS > 0 and _MMAP_FLUSH_HANDLE is None:
        try:
            loop = asyncio.get_running_loop()
            _MMAP_FLUSH_HANDLE = loop.call_later(
                _EVENT_MMAP_BATCH_MS / 1000.0, _flush_mmap_buffer
            )
        except RuntimeError:
            _flush_mmap_buffer()


def _get_bus_url(cfg=None):
    from .config import get_event_bus_url
    return get_event_bus_url(cfg)

# Public shim for modules expecting this symbol here.
def get_event_bus_url(cfg=None):  # pragma: no cover - thin wrapper
    return _get_bus_url(cfg)

def _get_event_serialization(cfg=None) -> str | None:
    from .config import get_event_serialization
    return get_event_serialization(cfg)

def _get_event_batch_ms(cfg=None) -> int:
    from .config import get_event_batch_ms
    return get_event_batch_ms(cfg)

def _get_event_mmap_batch_ms(cfg=None) -> int:
    from .config import get_event_mmap_batch_ms
    return get_event_mmap_batch_ms(cfg)


def _get_event_mmap_batch_size(cfg=None) -> int:
    from .config import get_event_mmap_batch_size
    return get_event_mmap_batch_size(cfg)

try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    websockets = None

# compression algorithm for websockets
_WS_COMPRESSION: str | None = os.getenv("EVENT_BUS_COMPRESSION")
if not _WS_COMPRESSION:
    _WS_COMPRESSION = None
else:
    comp = _WS_COMPRESSION.lower()
    _WS_COMPRESSION = None if comp in {"", "none", "0"} else comp

# ping interval and timeout for websocket connections
_WS_PING_INTERVAL = float(os.getenv("WS_PING_INTERVAL", "20") or 20)
_WS_PING_TIMEOUT = float(os.getenv("WS_PING_TIMEOUT", "20") or 20)

# how long to buffer outgoing events before flushing (ms)
_EVENT_BATCH_MS = int(os.getenv("EVENT_BATCH_MS", "10") or 10)

# magic header for batched binary websocket messages
_BATCH_MAGIC = b"EBAT"
_MP_BATCH_MAGIC = b"EBMP"

# ---------------------------------------------------------------------------
#  Outgoing queue backpressure controls
# ---------------------------------------------------------------------------
_OUTGOING_QUEUE_MAX = int(os.getenv("EVENT_OUTGOING_QUEUE_MAX", "2000") or 2000)
_OUTGOING_QUEUE_POLICY = (os.getenv("EVENT_OUTGOING_QUEUE_POLICY", "drop_oldest") or "drop_oldest").lower()
_OUTGOING_QUEUE_DROP_COUNT = 0
_OUTGOING_QUEUE_LOG_EVERY = 200  # log a line every N drops to avoid spam


def _enqueue_outgoing(msg: Any) -> None:
    """Enqueue an outgoing frame with bounded backpressure and drop policy."""

    global _OUTGOING_QUEUE_DROP_COUNT
    q = _outgoing_queue
    if q is None:
        return
    try:
        q.put_nowait(msg)
        return
    except asyncio.QueueFull:
        pass
    # queue is full; apply drop policy
    try:
        if _OUTGOING_QUEUE_POLICY == "drop_newest":
            # drop the new frame (do nothing)
            _OUTGOING_QUEUE_DROP_COUNT += 1
        else:
            # default: drop_oldest -> remove one and enqueue the new one
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            q.put_nowait(msg)
            _OUTGOING_QUEUE_DROP_COUNT += 1
    except Exception:
        # as a last resort, drop silently to keep the bus alive
        _OUTGOING_QUEUE_DROP_COUNT += 1
    if _OUTGOING_QUEUE_DROP_COUNT % _OUTGOING_QUEUE_LOG_EVERY == 0:
        logging.getLogger(__name__).warning(
            "event bus dropped %d outgoing frame(s) (policy=%s, max=%d)",
            _OUTGOING_QUEUE_DROP_COUNT,
            _OUTGOING_QUEUE_POLICY,
            _OUTGOING_QUEUE_MAX,
        )


def _pack_batch(msgs: List[Any]) -> Any:
    """Return a single frame containing all ``msgs``."""
    if not msgs:
        return b""
    if msgpack is not None:
        return _MP_BATCH_MAGIC + msgpack.packb(msgs)
    first = msgs[0]
    if isinstance(first, bytes):
        parts = [len(m).to_bytes(4, "big") + m for m in msgs]
        return _BATCH_MAGIC + len(msgs).to_bytes(4, "big") + b"".join(parts)
    return _dumps(msgs)


def _unpack_batch(data: Any) -> List[Any] | None:
    """Return list of messages if ``data`` is a batched frame."""
    if isinstance(data, bytes):
        if len(data) >= len(_MP_BATCH_MAGIC) and data.startswith(_MP_BATCH_MAGIC) and msgpack is not None:
            try:
                obj = msgpack.unpackb(data[len(_MP_BATCH_MAGIC):])
            except Exception:
                return None
            if isinstance(obj, list):
                return list(obj)
            return None
        if len(data) >= len(_BATCH_MAGIC) + 4 and data.startswith(_BATCH_MAGIC):
            off = len(_BATCH_MAGIC)
            count = int.from_bytes(data[off:off + 4], "big")
            off += 4
            msgs = []
            for _ in range(count):
                if off + 4 > len(data):
                    break
                ln = int.from_bytes(data[off:off + 4], "big")
                off += 4
                msgs.append(data[off:off + ln])
                off += ln
            if len(msgs) == count:
                return msgs
        return None
    if isinstance(data, str):
        try:
            obj = _loads(data)
        except Exception:
            return None
        if isinstance(obj, list):
            return obj
    return None


def _extract_topic(msg: Any) -> str | None:
    """Return the event topic encoded in ``msg`` if possible."""
    if isinstance(msg, (bytes, bytearray)):
        try:
            data = _maybe_decompress(msg)
            ev = pb.Event()
            ev.ParseFromString(data)
            return ev.topic
        except Exception:
            return None
    try:
        obj = _loads(msg)
    except Exception:
        return None
    if isinstance(obj, dict):
        return obj.get("topic")
    return None

# mapping of topic -> list of handlers
_subscribers: Dict[str, List[Callable[[Any], Awaitable[None] | None]]] = defaultdict(list)

# websocket related globals
_ws_clients: Set[Any] = set()  # clients connected to our server
_ws_client_topics: Dict[Any, Set[str] | None] = {}  # allowed topics per client
_peer_clients: Dict[str, Any] = {}  # outbound connections to peers
_peer_urls: Set[str] = set()
_watch_tasks: Dict[str, Any] = {}
_ws_server = None
_flush_task: asyncio.Task | None = None
_outgoing_queue: Queue | None = None
_ws_tasks: Set[asyncio.Task] = set()
_ws_health_server: asyncio.AbstractServer | None = None
_ws_health_host: str | None = None
_ws_health_port: int | None = None


async def _health_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Simple HTTP handler returning websocket health metrics."""

    # Helper to return bytes for JSON regardless of backend (orjson vs stdlib)
    def _json_bytes(obj) -> bytes:
        try:
            # orjson.dumps -> bytes
            data = json.dumps(obj)  # type: ignore[attr-defined]
            return data if isinstance(data, (bytes, bytearray)) else str(data).encode("utf-8")
        except Exception:
            # stdlib json.dumps -> str
            import json as _stdlib_json  # type: ignore
            return _stdlib_json.dumps(obj).encode("utf-8")

    status = {
        "status": "ok",
        "clients": len(_ws_clients),
        "peers": len(_peer_clients),
        "pending": (_outgoing_queue.qsize() if _outgoing_queue is not None else 0),
        "ws_host": _WS_LISTEN_HOST,
        "ws_port": _WS_LISTEN_PORT,
    }
    body = _json_bytes(status)
    headers = [
        b"HTTP/1.1 200 OK",
        b"Content-Type: application/json",
        f"Content-Length: {len(body)}".encode("ascii"),
        b"Connection: close",
        b"",
    ]
    payload = b"\r\n".join(headers) + b"\r\n" + body
    try:
        writer.write(payload)
        await writer.drain()
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:  # pragma: no cover - best effort
            pass


async def _start_ws_health_server(host: str, ws_port: int) -> None:
    """Launch the optional websocket health endpoint."""

    global _ws_health_server, _ws_health_host, _ws_health_port
    if _ws_health_server is not None:
        return
    if os.getenv("EVENT_BUS_HEALTH_DISABLE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    health_host = os.getenv("EVENT_BUS_HEALTH_HOST") or host
    port_raw = os.getenv("EVENT_BUS_HEALTH_PORT")
    if port_raw is None or not port_raw.strip():
        port_candidate = ws_port + 1
    else:
        try:
            port_candidate = int(port_raw)
        except Exception:
            port_candidate = ws_port + 1
    if port_candidate <= 0:
        return
    try:
        server = await asyncio.start_server(_health_handler, health_host, port_candidate)
    except OSError as exc:
        if getattr(exc, "errno", None) in {errno.EADDRINUSE, errno.EACCES}:
            raise RuntimeError(
                f"Event bus health port {port_candidate} unavailable"
            ) from exc
        raise
    _ws_health_server = server
    _ws_health_host = health_host
    sockets = getattr(server, "sockets", None)
    resolved_port = port_candidate
    if sockets:
        try:
            resolved_port = sockets[0].getsockname()[1]
        except Exception:  # pragma: no cover - platform variance
            pass
    _ws_health_port = resolved_port
    logging.getLogger(__name__).info(
        "Event bus health server listening on %s:%s", health_host, resolved_port
    )


async def _stop_ws_health_server() -> None:
    """Tear down the health endpoint if it is running."""

    global _ws_health_server, _ws_health_host, _ws_health_port
    if _ws_health_server is None:
        return
    _ws_health_server.close()
    await _ws_health_server.wait_closed()
    _ws_health_server = None
    _ws_health_host = None
    _ws_health_port = None


def _track_ws_task(task: asyncio.Task) -> None:
    """Track websocket-related ``task`` for later cleanup."""
    _ws_tasks.add(task)
    task.add_done_callback(lambda t: _ws_tasks.discard(t))

# message broker globals
_BROKER_URLS: list[str] = []
_BROKER_TYPES: list[str] = []
_BROKER_CONNS: list[Any] = []
_BROKER_TASKS: list[Any] = []
_BROKER_CHANNEL: str = os.getenv("BROKER_CHANNEL") or "solhunter-events-v3"
_BROKER_HEARTBEAT_INTERVAL = float(os.getenv("BROKER_HEARTBEAT_INTERVAL", "30") or 30)
_BROKER_RETRY_LIMIT = int(os.getenv("BROKER_RETRY_LIMIT", "3") or 3)
_BROKER_HEARTBEAT_TASK: asyncio.Task | None = None


def _encode_event(topic: str, payload: Any, dedupe_key: str | None = None) -> Any:
    cls = _PB_MAP.get(topic)
    if cls is None:
        # Preserve payloads for unknown topics using JSON serialization so
        # downstream consumers receive the original data instead of an empty
        # protobuf envelope.
        data = {"topic": topic, "payload": to_dict(payload)}
        if dedupe_key:
            data["dedupe_key"] = dedupe_key
        return _dumps(data)

    if topic == "action_executed":
        event = pb.Event(
            topic=topic,
            action_executed=pb.ActionExecuted(
                action_json=_dumps_text(payload.action),
                result_json=_dumps_text(payload.result),
            ),
        )
    elif topic == "weights_updated":
        event = pb.Event(topic=topic, weights_updated=pb.WeightsUpdated(weights=payload.weights))
    elif topic == "rl_weights":
        event = pb.Event(
            topic=topic,
            rl_weights=pb.RLWeights(weights=payload.weights, risk=payload.risk or {}),
        )
    elif topic == "rl_checkpoint":
        event = pb.Event(topic=topic, rl_checkpoint=pb.RLCheckpoint(time=payload.time, path=payload.path))
    elif topic == "portfolio_updated":
        event = pb.Event(topic=topic, portfolio_updated=pb.PortfolioUpdated(balances=payload.balances))
    elif topic in {"depth_update", "depth_diff"}:
        entries = {}
        for token, entry in to_dict(payload).items():
            if not isinstance(entry, dict):
                continue
            dex_map = entry.get("dex") or {
                k: v for k, v in entry.items() if isinstance(v, dict)
            }
            dex = {
                str(d): pb.TokenInfo(
                    bids=float(info.get("bids", 0.0)),
                    asks=float(info.get("asks", 0.0)),
                    tx_rate=float(info.get("tx_rate", 0.0)),
                )
                for d, info in dex_map.items()
                if isinstance(info, dict)
            }
            bids = float(entry.get("bids", 0.0))
            asks = float(entry.get("asks", 0.0))
            if not bids and not asks and dex:
                bids = sum(i.bids for i in dex.values())
                asks = sum(i.asks for i in dex.values())
            entries[str(token)] = pb.TokenAgg(
                dex=dex,
                bids=bids,
                asks=asks,
                tx_rate=float(entry.get("tx_rate", 0.0)),
                ts=int(entry.get("ts", 0)),
            )
        if topic == "depth_update":
            event = pb.Event(topic=topic, depth_update=pb.DepthUpdate(entries=entries))
        else:
            event = pb.Event(topic=topic, depth_diff=pb.DepthDiff(entries=entries))
        if hasattr(event, "schema_version"):
            event.schema_version = _EVENT_SCHEMA_VERSION
        if dedupe_key:
            event.dedupe_key = dedupe_key
        data = event.SerializeToString()
        return _compress_event(data)
    elif topic == "depth_service_status":
        event = pb.Event(topic=topic, depth_service_status=pb.DepthServiceStatus(status=payload.get("status")))
    elif topic == "heartbeat":
        service = getattr(payload, "service", None)
        if service is None and isinstance(payload, dict):
            service = payload.get("service")
        event = pb.Event(topic=topic, heartbeat=pb.Heartbeat(service=service or ""))
    elif topic == "trade_logged":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            trade_logged=pb.TradeLogged(
                token=str(data.get("token", "")),
                direction=str(data.get("direction", "")),
                amount=float(data.get("amount", 0.0)),
                price=float(data.get("price", 0.0)),
                reason=str(data.get("reason", "")),
                context=str(data.get("context", "")),
                emotion=str(data.get("emotion", "")),
                simulation_id=int(data.get("simulation_id", 0) or 0),
                uuid=str(data.get("uuid", "")),
                trade_id=int(data.get("trade_id", 0) or 0),
            ),
        )
    elif topic == "rl_metrics":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            rl_metrics=pb.RLMetrics(
                loss=float(data.get("loss", 0.0)),
                reward=float(data.get("reward", 0.0)),
            ),
        )
    elif topic == "system_metrics":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            system_metrics=pb.SystemMetrics(
                cpu=float(data.get("cpu", 0.0)),
                memory=float(data.get("memory", 0.0)),
            ),
        )
    elif topic == "runtime.log":
        data = to_dict(payload)
        runtime_log = pb.RuntimeLog(
            stage=str(data.get("stage", "")),
            detail=str(data.get("detail", "")),
        )
        if data.get("ts") is not None:
            try:
                runtime_log.ts = float(data.get("ts"))
            except (TypeError, ValueError):
                runtime_log.ts = 0.0
        if data.get("level") is not None:
            runtime_log.level = str(data.get("level"))
        if data.get("actions") is not None:
            try:
                runtime_log.actions = int(data.get("actions") or 0)
            except (TypeError, ValueError):
                runtime_log.actions = 0
        event = pb.Event(topic=topic, runtime_log=runtime_log)
    elif topic == "price_update":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            price_update=pb.PriceUpdate(
                venue=str(data.get("venue", "")),
                token=str(data.get("token", "")),
                price=float(data.get("price", 0.0)),
            ),
        )
    elif topic == "config_updated":
        data = to_dict(payload)
        if _USE_ORJSON:
            config_json = json.dumps(data).decode()
        else:
            config_json = json.dumps(data)
        event = pb.Event(
            topic=topic,
            config_updated=pb.ConfigUpdated(
                config_json=config_json,
            ),
        )
    elif topic == "pending_swap":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            pending_swap=pb.PendingSwap(
                token=str(data.get("token", "")),
                address=str(data.get("address", "")),
                size=float(data.get("size", 0.0)),
                slippage=float(data.get("slippage", 0.0)),
            ),
        )
    elif topic == "action_proposal":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            action_proposal=pb.ActionProposal(
                token=str(data.get("token", "")),
                side=str(data.get("side", "")),
                size=float(data.get("size", 0.0)),
                score=float(data.get("score", 0.0)),
                agent=str(data.get("agent", "")),
                price=float(data.get("price", 0.0)),
            ),
        )
    elif topic == "action_decision":
        data = to_dict(payload)
        rationale = data.get("rationale") if isinstance(data, dict) else None
        rationale_kwargs: dict[str, Any] = {}
        if isinstance(rationale, dict):
            agent = rationale.get("agent")
            if agent:
                rationale_kwargs["agent"] = str(agent)
            agents = rationale.get("agents")
            if isinstance(agents, (list, tuple, set)):
                rationale_kwargs["agents"] = [
                    str(a) for a in agents if str(a).strip()
                ]
            score = rationale.get("score")
            if score is not None:
                try:
                    rationale_kwargs["score"] = float(score)
                except (TypeError, ValueError):
                    pass
            confidence = rationale.get("confidence")
            if confidence is not None:
                try:
                    rationale_kwargs["confidence"] = float(confidence)
                except (TypeError, ValueError):
                    pass
            conviction = rationale.get("conviction_delta")
            if conviction is not None:
                try:
                    rationale_kwargs["conviction_delta"] = float(conviction)
                except (TypeError, ValueError):
                    pass
            snapshot_hash = rationale.get("snapshot_hash")
            if snapshot_hash:
                rationale_kwargs["snapshot_hash"] = str(snapshot_hash)
        decision = pb.ActionDecision(
            token=str(data.get("token", "")),
            side=str(data.get("side", "")),
            size=float(data.get("size", 0.0)),
            price=float(data.get("price", 0.0)),
        )
        if rationale_kwargs:
            decision.rationale.CopyFrom(pb.DecisionRationale(**rationale_kwargs))
        event = pb.Event(topic=topic, action_decision=decision)
    elif topic == "decision_summary":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            decision_summary=pb.DecisionSummary(
                token=str(data.get("token", "")),
                count=int(data.get("count", 0) or 0),
                buys=int(data.get("buys", 0) or 0),
                sells=int(data.get("sells", 0) or 0),
            ),
        )
    elif topic == "decision_metrics":
        data = to_dict(payload)
        metrics = pb.DecisionMetrics()
        if data.get("window_sec") is not None:
            try:
                metrics.window_sec = float(data.get("window_sec", 0.0))
            except (TypeError, ValueError):
                metrics.window_sec = 0.0
        if data.get("count") is not None:
            try:
                metrics.count = int(data.get("count", 0) or 0)
            except (TypeError, ValueError):
                metrics.count = 0
        if data.get("decision_rate") is not None:
            try:
                metrics.decision_rate = float(data.get("decision_rate", 0.0))
            except (TypeError, ValueError):
                metrics.decision_rate = 0.0
        if data.get("buys") is not None:
            try:
                metrics.buys = int(data.get("buys", 0) or 0)
            except (TypeError, ValueError):
                metrics.buys = 0
        if data.get("sells") is not None:
            try:
                metrics.sells = int(data.get("sells", 0) or 0)
            except (TypeError, ValueError):
                metrics.sells = 0
        if data.get("avg_size") is not None:
            try:
                metrics.avg_size = float(data.get("avg_size", 0.0))
            except (TypeError, ValueError):
                metrics.avg_size = 0.0
        if data.get("ttf_decision") is not None:
            try:
                metrics.ttf_decision = float(data.get("ttf_decision", 0.0))
            except (TypeError, ValueError):
                metrics.ttf_decision = 0.0
        event = pb.Event(topic=topic, decision_metrics=metrics)
    elif topic == "dex_latency_update":
        data = to_dict(payload)
        if isinstance(data, dict):
            latencies = {str(k): float(v) for k, v in data.items() if v is not None}
        else:
            latencies = {}
        event = pb.Event(
            topic=topic,
            dex_latency_update=pb.DexLatencyUpdate(latencies=latencies),
        )
    elif topic == "runtime.stage_changed":
        data = to_dict(payload)
        ok = data.get("ok")
        if isinstance(ok, str):
            ok = ok.lower() in {"1", "true", "yes", "ok"}
        elif isinstance(ok, (int, float)):
            ok = bool(ok)
        elif not isinstance(ok, bool):
            ok = False
        event = pb.Event(
            topic=topic,
            runtime_stage_changed=pb.RuntimeStageChanged(
                stage=str(data.get("stage", "")),
                ok=bool(ok),
                detail=str(data.get("detail", "")),
            ),
        )
    elif topic == "remote_system_metrics":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            remote_system_metrics=pb.RemoteSystemMetrics(
                cpu=float(data.get("cpu", 0.0)),
                memory=float(data.get("memory", 0.0)),
            ),
        )
    elif topic == "risk_metrics":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            risk_metrics=pb.RiskMetrics(
                covariance=float(data.get("covariance", 0.0)),
                portfolio_cvar=float(data.get("portfolio_cvar", 0.0)),
                portfolio_evar=float(data.get("portfolio_evar", 0.0)),
                correlation=float(data.get("correlation", 0.0)),
                cov_matrix=[pb.DoubleList(values=[float(x) for x in row]) for row in data.get("cov_matrix", [])],
                corr_matrix=[pb.DoubleList(values=[float(x) for x in row]) for row in data.get("corr_matrix", [])],
            ),
        )
    elif topic == "risk_updated":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            risk_updated=pb.RiskUpdated(
                multiplier=float(data.get("multiplier", 0.0)),
            ),
        )
    elif topic == "system_metrics_combined":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            system_metrics_combined=pb.SystemMetricsCombined(
                cpu=float(data.get("cpu", 0.0)),
                memory=float(data.get("memory", 0.0)),
                iter_ms=float(data.get("iter_ms", 0.0)),
            ),
        )
    elif topic in {
        "startup_config_load_duration",
        "startup_connectivity_check_duration",
        "startup_depth_service_start_duration",
    }:
        value = payload
        if isinstance(payload, dict):
            value = payload.get("value", 0.0)
        try:
            metric_value = float(value)
        except (TypeError, ValueError):
            metric_value = 0.0
        event = pb.Event(topic=topic, scalar_metric=pb.ScalarMetric(value=metric_value))
    elif topic == "startup_complete":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            startup_complete=pb.StartupComplete(
                startup_duration_ms=float(data.get("startup_duration_ms", 0.0))
            ),
        )
    elif topic == "virtual_pnl":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            virtual_pnl=pb.VirtualPnL(
                order_id=str(data.get("order_id", "")),
                mint=str(data.get("mint", "")),
                snapshot_hash=str(data.get("snapshot_hash", "")),
                realized_usd=float(data.get("realized_usd", 0.0)),
                unrealized_usd=float(data.get("unrealized_usd", 0.0)),
                ts=float(data.get("ts", 0.0)),
            ),
        )
    elif topic == "token_discovered":
        entries = _normalize_discovery_entries(payload)
        pb_entries: list[Any] = []
        for item in entries:
            mint = item.get("mint")
            if not mint:
                continue
            discovery_entry = pb.TokenDiscovery(mint=str(mint))
            source = item.get("source")
            if source:
                discovery_entry.source = str(source)
            if "score" in item:
                try:
                    discovery_entry.score = float(item.get("score") or 0.0)
                except Exception:
                    pass
            tx = item.get("tx")
            if tx:
                discovery_entry.tx = str(tx)
            if "ts" in item:
                try:
                    discovery_entry.ts = float(item.get("ts") or 0.0)
                except Exception:
                    pass
            tags = item.get("tags")
            if isinstance(tags, Sequence) and not isinstance(tags, (str, bytes, bytearray)):
                discovery_entry.tags.extend(str(tag) for tag in tags if str(tag))
            interface = item.get("interface")
            if interface:
                discovery_entry.interface = str(interface)
            discovery = item.get("discovery")
            if isinstance(discovery, Mapping):
                struct = _struct_from_mapping(discovery)
                if struct is not None:
                    discovery_entry.discovery.CopyFrom(struct)
            attributes = item.get("attributes")
            if isinstance(attributes, Mapping):
                struct = _struct_from_mapping(attributes)
                if struct is not None:
                    discovery_entry.attributes.CopyFrom(struct)
            pb_entries.append(discovery_entry)
        event = pb.Event(
            topic=topic,
            token_discovered=pb.TokenDiscovered(entries=pb_entries),
        )
    elif topic == "memory_sync_request":
        data = to_dict(payload)
        event = pb.Event(
            topic=topic,
            memory_sync_request=pb.MemorySyncRequest(last_id=int(data.get("last_id", 0))),
        )
    elif topic == "memory_sync_response":
        data = to_dict(payload)
        trades = [
            pb.TradeLogged(
                token=str(t.get("token", "")),
                direction=str(t.get("direction", "")),
                amount=float(t.get("amount", 0.0)),
                price=float(t.get("price", 0.0)),
                reason=str(t.get("reason", "")),
                context=str(t.get("context", "")),
                emotion=str(t.get("emotion", "")),
                simulation_id=int(t.get("simulation_id", 0) or 0),
                uuid=str(t.get("uuid", "")),
                trade_id=int(t.get("trade_id", 0) or 0),
            )
            for t in (data.get("trades") or [])
        ]
        event = pb.Event(
            topic=topic,
            memory_sync_response=pb.MemorySyncResponse(trades=trades, index=data.get("index") or b""),
        )
    if hasattr(event, "schema_version"):
        event.schema_version = _EVENT_SCHEMA_VERSION
    if dedupe_key:
        event.dedupe_key = dedupe_key
    data = event.SerializeToString()
    return _compress_event(data)

def _decode_payload(ev: Any) -> Any:
    field = ev.WhichOneof("kind")
    if not field:
        return None
    msg = getattr(ev, field)
    if field == "action_executed":
        return {
            "action": _loads(msg.action_json),
            "result": _loads(msg.result_json),
        }
    if field == "weights_updated":
        return {"weights": dict(msg.weights)}
    if field == "rl_weights":
        return {"weights": dict(msg.weights), "risk": dict(msg.risk)}
    if field == "rl_checkpoint":
        return {"time": msg.time, "path": msg.path}
    if field == "portfolio_updated":
        return {"balances": dict(msg.balances)}
    if field in {"depth_update", "depth_diff"}:
        result = {}
        for token, entry in msg.entries.items():
            dex = {
                dk: {"bids": di.bids, "asks": di.asks, "tx_rate": di.tx_rate}
                for dk, di in entry.dex.items()
            }
            result[token] = {
                "dex": dex,
                "bids": entry.bids,
                "asks": entry.asks,
                "tx_rate": entry.tx_rate,
                "ts": entry.ts,
                "depth": entry.bids + entry.asks,
            }
        return result
    if field == "depth_service_status":
        return {"status": msg.status}
    if field == "heartbeat":
        return {"service": msg.service}
    if field == "trade_logged":
        return {
            "token": msg.token,
            "direction": msg.direction,
            "amount": msg.amount,
            "price": msg.price,
            "reason": msg.reason,
            "context": msg.context,
            "emotion": msg.emotion,
            "simulation_id": msg.simulation_id,
            "uuid": msg.uuid,
            "trade_id": msg.trade_id,
        }
    if field == "rl_metrics":
        return {"loss": msg.loss, "reward": msg.reward}
    if field == "system_metrics":
        return {"cpu": msg.cpu, "memory": msg.memory}
    if field == "runtime_log":
        return {
            "stage": msg.stage,
            "detail": msg.detail,
            "ts": msg.ts,
            "level": msg.level,
            "actions": msg.actions,
        }
    if field == "price_update":
        return {
            "venue": msg.venue,
            "token": msg.token,
            "price": msg.price,
        }
    if field == "config_updated":
        return json.loads(msg.config_json)
    if field == "pending_swap":
        return {
            "token": msg.token,
            "address": msg.address,
            "size": msg.size,
            "slippage": msg.slippage,
        }
    if field == "remote_system_metrics":
        return {"cpu": msg.cpu, "memory": msg.memory}
    if field == "risk_metrics":
        cov = [list(row.values) for row in msg.cov_matrix]
        corr = [list(row.values) for row in msg.corr_matrix]
        return {
            "covariance": msg.covariance,
            "portfolio_cvar": msg.portfolio_cvar,
            "portfolio_evar": msg.portfolio_evar,
            "correlation": msg.correlation,
            "cov_matrix": cov,
            "corr_matrix": corr,
        }
    if field == "risk_updated":
        return {"multiplier": msg.multiplier}
    if field == "system_metrics_combined":
        return {"cpu": msg.cpu, "memory": msg.memory, "iter_ms": msg.iter_ms}
    if field == "token_discovered":
        entries: list[dict[str, Any]] = []
        for entry in msg.entries:
            data: dict[str, Any] = {"mint": entry.mint}
            if entry.source:
                data["source"] = entry.source
            if entry.HasField("score"):
                data["score"] = entry.score
            if entry.tx:
                data["tx"] = entry.tx
            if entry.HasField("ts"):
                data["ts"] = entry.ts
            if entry.tags:
                data["tags"] = list(entry.tags)
            if entry.interface:
                data["interface"] = entry.interface
            if entry.HasField("discovery"):
                discovery = _struct_to_python(entry.discovery)
                if discovery:
                    data["discovery"] = discovery
            if entry.HasField("attributes"):
                attributes = _struct_to_python(entry.attributes)
                if attributes:
                    data["attributes"] = attributes
            entries.append(data)
        return entries
    if field == "memory_sync_request":
        return {"last_id": msg.last_id}
    if field == "memory_sync_response":
        trades = [
            {
                "token": t.token,
                "direction": t.direction,
                "amount": t.amount,
                "price": t.price,
                "reason": t.reason,
                "context": t.context,
                "emotion": t.emotion,
                "simulation_id": t.simulation_id,
                "uuid": t.uuid,
                "trade_id": t.trade_id,
            }
            for t in msg.trades
        ]
        return {"trades": trades, "index": bytes(msg.index)}
    if field == "action_proposal":
        return {
            "token": msg.token,
            "side": msg.side,
            "size": msg.size,
            "score": msg.score,
            "agent": msg.agent,
            "price": msg.price,
        }
    if field == "action_decision":
        data = {
            "token": msg.token,
            "side": msg.side,
            "size": msg.size,
            "price": msg.price,
        }
        if msg.HasField("rationale"):
            rationale = {}
            if msg.rationale.agent:
                rationale["agent"] = msg.rationale.agent
            if msg.rationale.agents:
                rationale["agents"] = list(msg.rationale.agents)
            if msg.rationale.score:
                rationale["score"] = msg.rationale.score
            if msg.rationale.confidence:
                rationale["confidence"] = msg.rationale.confidence
            if msg.rationale.conviction_delta:
                rationale["conviction_delta"] = msg.rationale.conviction_delta
            if msg.rationale.snapshot_hash:
                rationale["snapshot_hash"] = msg.rationale.snapshot_hash
            if rationale:
                data["rationale"] = rationale
        return data
    if field == "decision_summary":
        return {
            "token": msg.token,
            "count": msg.count,
            "buys": msg.buys,
            "sells": msg.sells,
        }
    if field == "decision_metrics":
        data: dict[str, Any] = {}
        if msg.window_sec:
            data["window_sec"] = msg.window_sec
        if msg.count:
            data["count"] = msg.count
        if msg.decision_rate:
            data["decision_rate"] = msg.decision_rate
        if msg.buys:
            data["buys"] = msg.buys
        if msg.sells:
            data["sells"] = msg.sells
        if msg.avg_size:
            data["avg_size"] = msg.avg_size
        if msg.ttf_decision:
            data["ttf_decision"] = msg.ttf_decision
        return data
    if field == "dex_latency_update":
        return dict(msg.latencies)
    if field == "runtime_stage_changed":
        return {
            "stage": msg.stage,
            "ok": msg.ok,
            "detail": msg.detail,
        }
    if field == "scalar_metric":
        return msg.value
    if field == "startup_complete":
        return {"startup_duration_ms": msg.startup_duration_ms}
    if field == "virtual_pnl":
        return {
            "order_id": msg.order_id,
            "mint": msg.mint,
            "snapshot_hash": msg.snapshot_hash,
            "realized_usd": msg.realized_usd,
            "unrealized_usd": msg.unrealized_usd,
            "ts": msg.ts,
        }
    return None

def _subscribe_impl(topic: str, handler: Callable[[Any], Awaitable[None] | None]):
    """Register ``handler`` for ``topic`` events.

    Returns a callable that will remove the handler when invoked.
    """
    _subscribers[topic].append(handler)

    def _unsub() -> None:
        _unsubscribe_impl(topic, handler)

    return _unsub


@contextmanager
def _subscription_impl(topic: str, handler: Callable[[Any], Awaitable[None] | None]) -> Generator[Callable[[Any], Awaitable[None] | None], None, None]:
    """Context manager that registers ``handler`` for ``topic`` and automatically unsubscribes."""
    unsub = _subscribe_impl(topic, handler)
    try:
        yield handler
    finally:
        unsub()

def _unsubscribe_impl(topic: str, handler: Callable[[Any], Awaitable[None] | None]):
    """Remove ``handler`` from ``topic`` subscriptions."""
    handlers = _subscribers.get(topic)
    if not handlers:
        return
    try:
        handlers.remove(handler)
    except ValueError:
        return
    if not handlers:
        _subscribers.pop(topic, None)

def _publish_impl(
    topic: str,
    payload: Any,
    *,
    dedupe_key: str | None = None,
    _broadcast: bool = True,
) -> None:
    """Publish ``payload`` to all subscribers of ``topic`` and over websockets."""
    key = _normalize_dedupe_key(dedupe_key)
    if key and _dedupe_should_drop(key):
        return
    payload = validate_message(topic, payload)
    handlers = list(_subscribers.get(topic, []))
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    for h in handlers:
        if inspect.iscoroutinefunction(h):
            if loop:
                loop.create_task(h(payload))
            else:
                asyncio.run(h(payload))
        else:
            h(payload)

    msg: Any | None = None
    if (websockets or _BROKER_CONNS or _EVENT_BUS_MMAP) and _broadcast:
        msg = _encode_event(topic, payload, dedupe_key=key)
    if _EVENT_BUS_MMAP and _broadcast and isinstance(msg, (bytes, bytearray)):
        _mmap_write(bytes(msg))
    if websockets and _broadcast:
        assert msg is not None
        if loop:
            if _outgoing_queue is not None:
                _enqueue_outgoing(msg)
            else:
                loop.create_task(broadcast_ws(msg))
        else:
            asyncio.run(broadcast_ws(msg))
    if _BROKER_CONNS and _broadcast:
        assert msg is not None
        if loop:
            loop.create_task(_broker_send(msg))
        else:
            asyncio.run(_broker_send(msg))


class EventBus:
    """Lightweight wrapper holding configuration and broker state."""

    def __init__(self) -> None:
        self.serialization = _EVENT_SERIALIZATION
        self.compression = EVENT_COMPRESSION
        self.broker_urls = _BROKER_URLS
        self.broker_conns = _BROKER_CONNS

    def configure(
        self,
        *,
        serialization: str | None = None,
        compression: str | None = None,
        broker_urls: Sequence[str] | None = None,
    ) -> None:
        """Update event bus configuration dynamically."""
        global _EVENT_SERIALIZATION, _USE_MSGPACK, EVENT_COMPRESSION, _BROKER_URLS
        if serialization is not None:
            _EVENT_SERIALIZATION = serialization.lower()
            self.serialization = _EVENT_SERIALIZATION
            _USE_MSGPACK = msgpack is not None and _EVENT_SERIALIZATION == "msgpack"
        if compression is not None:
            EVENT_COMPRESSION = compression
            self.compression = EVENT_COMPRESSION
        if broker_urls is not None:
            _BROKER_URLS.clear()
            _BROKER_URLS.extend(broker_urls)
            self.broker_urls = _BROKER_URLS

    def subscribe(self, topic: str, handler: Callable[[Any], Awaitable[None] | None]):
        return _subscribe_impl(topic, handler)

    @contextmanager
    def subscription(
        self, topic: str, handler: Callable[[Any], Awaitable[None] | None]
    ) -> Generator[Callable[[Any], Awaitable[None] | None], None, None]:
        with _subscription_impl(topic, handler) as h:
            yield h

    def unsubscribe(self, topic: str, handler: Callable[[Any], Awaitable[None] | None]):
        _unsubscribe_impl(topic, handler)

    def publish(
        self,
        topic: str,
        payload: Any,
        *,
        dedupe_key: str | None = None,
        _broadcast: bool = True,
    ) -> None:
        _publish_impl(topic, payload, dedupe_key=dedupe_key, _broadcast=_broadcast)

    def reset(self) -> None:
        """Reset subscriptions and broker state for tests."""
        _subscribers.clear()
        _BROKER_CONNS.clear()
        _BROKER_TASKS.clear()
        _BROKER_URLS.clear()
        _BROKER_TYPES.clear()
        _dedupe_reset()


BUS = EventBus()


def subscribe(topic: str, handler: Callable[[Any], Awaitable[None] | None]):
    return BUS.subscribe(topic, handler)


@contextmanager
def subscription(topic: str, handler: Callable[[Any], Awaitable[None] | None]) -> Generator[Callable[[Any], Awaitable[None] | None], None, None]:
    with BUS.subscription(topic, handler) as h:
        yield h


def unsubscribe(topic: str, handler: Callable[[Any], Awaitable[None] | None]):
    BUS.unsubscribe(topic, handler)


def publish(
    topic: str,
    payload: Any,
    *,
    dedupe_key: str | None = None,
    _broadcast: bool = True,
) -> None:
    BUS.publish(topic, payload, dedupe_key=dedupe_key, _broadcast=_broadcast)


def configure(**kw) -> None:
    BUS.configure(**kw)


def reset() -> None:
    BUS.reset()


async def broadcast_ws(
    message: Any,
    *,
    to_clients: bool = True,
    to_server: bool = True,
) -> None:
    """Send ``message`` to websocket peers respecting topic subscriptions."""
    if isinstance(message, list):
        msgs = list(message)
    else:
        msgs = _unpack_batch(message) or [message]
    frame_all = _pack_batch(msgs) if len(msgs) > 1 else msgs[0]
    if to_clients:
        clients = list(_ws_clients)
        coros = []
        for ws in clients:
            allowed = _ws_client_topics.get(ws)
            out_msgs = []
            if allowed is None:
                frame = frame_all
            else:
                for m in msgs:
                    topic = _extract_topic(m)
                    if topic is None or topic in allowed:
                        out_msgs.append(m)
                if not out_msgs:
                    continue
                frame = _pack_batch(out_msgs) if len(out_msgs) > 1 else out_msgs[0]
            coros.append(ws.send(frame))
        if coros:
            results = await asyncio.gather(*coros, return_exceptions=True)
            for ws, res in zip([c for c in clients if c in _ws_clients], results):
                if isinstance(res, Exception):  # pragma: no cover - network errors
                    _ws_clients.discard(ws)
                    _ws_client_topics.pop(ws, None)
    if to_server and _peer_clients:
        peers = list(_peer_clients.items())
        coros = [ws.send(frame_all) for _, ws in peers]
        results = await asyncio.gather(*coros, return_exceptions=True)
        for (url, ws), res in zip(peers, results):
            if isinstance(res, Exception):  # pragma: no cover - connection issues
                asyncio.create_task(reconnect_ws(url))


async def _flush_outgoing() -> None:
    """Background task to flush queued websocket messages."""
    global _outgoing_queue
    q = _outgoing_queue
    if q is None:
        return
    loop = asyncio.get_running_loop()
    while True:
        try:
            # wait for at least one message
            msg = await q.get()
        except asyncio.CancelledError:
            break
        msgs: list[Any] = [msg]
        delay = _EVENT_BATCH_MS / 1000.0
        if delay > 0:
            end = loop.time() + delay
            while True:
                timeout = end - loop.time()
                if timeout <= 0:
                    break
                try:
                    nxt = await asyncio.wait_for(q.get(), timeout=timeout)
                    msgs.append(nxt)
                except asyncio.TimeoutError:
                    break
        else:
            while True:
                try:
                    msgs.append(q.get_nowait())
                except asyncio.QueueEmpty:
                    break
        if len(msgs) == 1:
            await broadcast_ws(msgs[0])
        else:
            await broadcast_ws(msgs)


async def _broker_send(message: bytes) -> None:
    """Publish ``message`` to all configured message brokers."""
    for typ, conn in zip(_BROKER_TYPES, _BROKER_CONNS):
        if typ == "redis" and conn is not None:
            try:
                await conn.publish(_BROKER_CHANNEL, message)
            except Exception:  # pragma: no cover - connection issues
                pass
        elif typ == "nats" and conn is not None:
            try:
                await conn.publish(_BROKER_CHANNEL, message)
            except Exception:  # pragma: no cover - connection issues
                pass


async def _receiver(ws) -> None:
    """Receive messages from ``ws`` and publish them."""
    try:
        async for msg in ws:
            try:
                msgs = _unpack_batch(msg) or [msg]
                for single in msgs:
                    if isinstance(single, bytes):
                        data = _maybe_decompress(single)
                        ev = pb.Event()
                        ev.ParseFromString(data)
                        payload = _decode_payload(ev)
                        dedupe = getattr(ev, "dedupe_key", None) or None
                        publish(
                            ev.topic,
                            payload,
                            dedupe_key=dedupe,
                            _broadcast=False,
                        )
                    else:
                        data = _loads(single)
                        topic = data.get("topic")
                        payload = data.get("payload")
                        dedupe = data.get("dedupe_key") if isinstance(data, dict) else None
                        publish(topic, payload, dedupe_key=dedupe, _broadcast=False)
                await broadcast_ws(msg, to_server=False)
            except Exception:  # pragma: no cover - malformed msg
                continue
    finally:
        for url, peer in list(_peer_clients.items()):
            if ws is peer and url in _peer_urls:
                _track_ws_task(asyncio.create_task(reconnect_ws(url)))
                break


async def _redis_listener(pubsub) -> None:
    """Listen to Redis pub/sub messages."""
    try:
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            data = msg.get("data")
            if isinstance(data, memoryview):
                data = data.tobytes()
            if isinstance(data, str):
                data = data.encode()
            if not isinstance(data, (bytes, bytearray)):
                logging.warning("Dropping redis event with unexpected payload type %r", type(data))
                continue
            raw = _maybe_decompress(bytes(data))
            ev = pb.Event()
            try:
                ev.ParseFromString(raw)
            except _ProtoDecodeError:
                try:
                    decoded = _loads(raw)
                except Exception:
                    logging.warning("Dropping redis event with incompatible protobuf schema")
                    continue
                if not isinstance(decoded, dict) or not decoded.get("topic"):
                    logging.warning("Dropping redis event missing topic after fallback decode")
                    continue
                payload = decoded.get("payload")
                if payload is None:
                    payload = {
                        key: value
                        for key, value in decoded.items()
                        if key not in {"topic", "dedupe_key"}
                    }
                publish(
                    str(decoded["topic"]),
                    payload,
                    dedupe_key=_normalize_dedupe_key(decoded.get("dedupe_key")),
                    _broadcast=False,
                )
                continue
            schema_version = getattr(ev, "schema_version", None)
            if _EXPECTED_EVENT_SCHEMA_VERSIONS and schema_version not in _EXPECTED_EVENT_SCHEMA_VERSIONS:
                logger.error(
                    "Schema mismatch: got=%s expected=%s",
                    schema_version,
                    ",".join(_EXPECTED_EVENT_SCHEMA_VERSIONS),
                )
                raise RuntimeError("Incompatible protobuf schema on event bus")
            dedupe = getattr(ev, "dedupe_key", None) or None
            publish(
                ev.topic,
                _decode_payload(ev),
                dedupe_key=dedupe,
                _broadcast=False,
            )
    except asyncio.CancelledError:  # pragma: no cover - shutdown
        pass


async def _connect_nats(url: str):
    nc = nats.NATS()
    await nc.connect(servers=[url])

    async def _cb(msg):
        ev = pb.Event()
        ev.ParseFromString(_maybe_decompress(msg.data))
        dedupe = getattr(ev, "dedupe_key", None) or None
        publish(ev.topic, _decode_payload(ev), dedupe_key=dedupe, _broadcast=False)

    await nc.subscribe(_BROKER_CHANNEL, cb=_cb)
    return nc


async def _reconnect_broker(index: int) -> None:
    """Attempt to reconnect broker at ``index`` up to the retry limit."""
    url = _BROKER_URLS[index]
    typ = _BROKER_TYPES[index]
    conn = _BROKER_CONNS[index]
    task = _BROKER_TASKS[index]
    if task is not None:
        task.cancel()
    if conn is not None:
        try:
            await conn.close()
        except Exception:
            pass
    for attempt in range(1, _BROKER_RETRY_LIMIT + 1):
        try:
            if typ == "redis":
                if aioredis is None:
                    raise RuntimeError("redis package required for redis broker")
                new_conn = aioredis.from_url(url)
                pubsub = new_conn.pubsub()
                await pubsub.subscribe(_BROKER_CHANNEL)
                new_task = asyncio.create_task(_redis_listener(pubsub))
                _BROKER_CONNS[index] = new_conn
                _BROKER_TASKS[index] = new_task
            elif typ == "nats":
                if nats is None:
                    raise RuntimeError("nats-py package required for nats broker")
                new_conn = await _connect_nats(url)
                _BROKER_CONNS[index] = new_conn
                _BROKER_TASKS[index] = None
            return
        except Exception as exc:
            logging.warning(
                "Broker reconnect failed for %s (attempt %d/%d): %s",
                url,
                attempt,
                _BROKER_RETRY_LIMIT,
                exc,
            )
            await asyncio.sleep(_BROKER_HEARTBEAT_INTERVAL)
    logging.warning("Failed to reconnect to broker %s after %d attempts", url, _BROKER_RETRY_LIMIT)


async def _broker_heartbeat() -> None:
    """Periodically ping brokers and reconnect if unresponsive."""
    while True:
        await asyncio.sleep(_BROKER_HEARTBEAT_INTERVAL)
        for idx, (typ, conn, url) in enumerate(zip(_BROKER_TYPES, _BROKER_CONNS, _BROKER_URLS)):
            if conn is None:
                continue
            try:
                if typ == "redis":
                    await asyncio.wait_for(conn.ping(), timeout=_BROKER_HEARTBEAT_INTERVAL)
                elif typ == "nats":
                    await asyncio.wait_for(conn.flush(), timeout=_BROKER_HEARTBEAT_INTERVAL)
            except Exception:
                logging.warning("Broker %s unresponsive, attempting reconnect", url)
                await _reconnect_broker(idx)

async def connect_broker(urls: Sequence[str] | str) -> None:
    """Connect to one or more Redis or NATS message brokers."""
    global _BROKER_URLS, _BROKER_CONNS, _BROKER_TASKS, _BROKER_TYPES, _BROKER_HEARTBEAT_TASK
    if isinstance(urls, str):
        urls = [urls]
    for url in urls:
        if url.startswith("redis://") or url.startswith("rediss://"):
            if aioredis is None:
                raise RuntimeError("redis package required for redis broker")
            conn = aioredis.from_url(url)
            pubsub = conn.pubsub()
            await pubsub.subscribe(_BROKER_CHANNEL)
            logging.getLogger(__name__).info(
                "Event bus: connected redis broker %s channel=%s", url, _BROKER_CHANNEL
            )
            task = asyncio.create_task(_redis_listener(pubsub))
            _BROKER_TYPES.append("redis")
            _BROKER_CONNS.append(conn)
            _BROKER_TASKS.append(task)
            _BROKER_URLS.append(url)
        elif url.startswith("nats://"):
            if nats is None:
                raise RuntimeError("nats-py package required for nats broker")
            conn = await _connect_nats(url)
            _BROKER_TYPES.append("nats")
            _BROKER_CONNS.append(conn)
            _BROKER_TASKS.append(None)
            _BROKER_URLS.append(url)
        else:
            raise ValueError(f"unsupported broker url: {url}")
    if _BROKER_HEARTBEAT_TASK is None:
        _BROKER_HEARTBEAT_TASK = asyncio.create_task(_broker_heartbeat())


async def disconnect_broker() -> None:
    """Disconnect from all active message brokers."""
    global _BROKER_URLS, _BROKER_CONNS, _BROKER_TASKS, _BROKER_TYPES, _BROKER_HEARTBEAT_TASK
    tasks = list(_BROKER_TASKS)
    conns = list(_BROKER_CONNS)
    types = list(_BROKER_TYPES)
    _BROKER_TASKS.clear()
    _BROKER_CONNS.clear()
    _BROKER_TYPES.clear()
    _BROKER_URLS.clear()
    if _BROKER_HEARTBEAT_TASK is not None:
        _BROKER_HEARTBEAT_TASK.cancel()
        _BROKER_HEARTBEAT_TASK = None
    for t in tasks:
        if t is not None:
            t.cancel()
    for typ, conn in zip(types, conns):
        if typ == "redis" and conn is not None:
            try:
                await conn.close()
            except Exception:
                pass
        elif typ == "nats" and conn is not None:
            try:
                await conn.close()
            except Exception:
                pass


async def verify_broker_connection(
    urls: Sequence[str] | str | None = None, *, timeout: float = 3.0
) -> bool:
    """Return ``True`` if a publish/subscribe round-trip succeeds.

    When ``urls`` is ``None`` the configured broker URLs are used. Any
    failures are logged and ``False`` is returned. This function performs a
    lightweight ping by publishing a message to a temporary channel and
    awaiting its delivery.
    """
    if urls is None:
        try:
            urls = _get_broker_urls()
        except Exception:
            urls = []
    if isinstance(urls, str):
        urls = [urls]
    if not urls:
        return True

    ok = True
    for url in urls:
        try:
            if url.startswith("redis://") or url.startswith("rediss://"):
                if aioredis is None:
                    raise RuntimeError(
                        "redis package required for redis broker"
                    )
                conn = aioredis.from_url(url)
                pubsub = conn.pubsub()
                ch = f"_verify_{os.urandom(4).hex()}"
                # Subscribe and wait for the server to acknowledge the subscription
                await pubsub.subscribe(ch)
                subscribed = False
                end = asyncio.get_event_loop().time() + max(0.2, timeout)
                while asyncio.get_event_loop().time() < end:
                    ack = await pubsub.get_message(
                        ignore_subscribe_messages=False, timeout=0.05
                    )
                    if ack:
                        # redis-py returns a dict with a 'type' field for subscribe acks
                        if isinstance(ack, dict) and ack.get("type") == "subscribe":
                            subscribed = True
                            break
                # Publish after subscription is confirmed (or best-effort timeout)
                await conn.publish(ch, b"ping")
                msg = await pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=timeout
                )
                await pubsub.unsubscribe(ch)
                await conn.close()
                if not msg:
                    raise RuntimeError("no message received")
            elif url.startswith("nats://"):
                if nats is None:
                    raise RuntimeError(
                        "nats-py package required for nats broker"
                    )
                nc = nats.NATS()
                await nc.connect(servers=[url])
                subj = f"_verify_{os.urandom(4).hex()}"
                fut = asyncio.get_event_loop().create_future()

                async def _cb(msg):
                    if not fut.done():
                        fut.set_result(msg.data)

                await nc.subscribe(subj, cb=_cb)
                await nc.publish(subj, b"ping")
                await asyncio.wait_for(fut, timeout)
                await nc.drain()
                await nc.close()
            else:
                raise ValueError(f"unsupported broker url: {url}")
        except Exception as exc:  # pragma: no cover - network issues
            logging.getLogger(__name__).error(
                "broker verification failed for %s: %s", url, exc
            )
            ok = False
    return ok


async def start_ws_server(host: str = "localhost", port: int = 8779):
    """Start websocket server broadcasting published events."""
    global _ws_server, _flush_task, _outgoing_queue, _WS_LISTEN_HOST, _WS_LISTEN_PORT, DEFAULT_WS_URL

    if os.getenv("EVENT_BUS_DISABLE_LOCAL", "").lower() in {"1", "true", "yes"}:
        if _outgoing_queue is None:
            _outgoing_queue = Queue(maxsize=max(1, _OUTGOING_QUEUE_MAX))
        return None
    if not websockets:
        raise RuntimeError("websockets library required")

    listen_host = host or _WS_LISTEN_HOST
    current_port = int(port or _WS_LISTEN_PORT)

    if _ws_server is not None:
        raise RuntimeError(
            f"Event bus websocket already running on {_WS_LISTEN_HOST}:{_WS_LISTEN_PORT}"
        )

    async def handler(ws, path):
        allowed: Set[str] | None = None
        try:
            import urllib.parse as _urlparse
            parsed = _urlparse.urlparse(path or "")
            qs = _urlparse.parse_qs(parsed.query)
            topics = qs.get("topics")
            if topics:
                allowed = {t for t in topics[0].split(",") if t}
        except Exception:
            allowed = None
        _ws_clients.add(ws)
        _ws_client_topics[ws] = allowed
        try:
            async for msg in ws:
                try:
                    msgs = _unpack_batch(msg) or [msg]
                    for single in msgs:
                        if isinstance(single, bytes):
                            data = _maybe_decompress(single)
                            ev = pb.Event()
                            ev.ParseFromString(data)
                            publish(ev.topic, _decode_payload(ev), _broadcast=False)
                        else:
                            data = _loads(single)
                            publish(data.get("topic"), data.get("payload"), _broadcast=False)
                except Exception:  # pragma: no cover - malformed message
                    continue
        finally:
            _ws_clients.discard(ws)
            _ws_client_topics.pop(ws, None)

    if _outgoing_queue is None:
        # bounded queue for backpressure
        _outgoing_queue = Queue(maxsize=max(1, _OUTGOING_QUEUE_MAX))
    if _flush_task is None or _flush_task.done():
        loop = asyncio.get_running_loop()
        _flush_task = loop.create_task(_flush_outgoing())

    async def _reset_flush() -> None:
        global _flush_task, _outgoing_queue
        if _flush_task is not None:
            _flush_task.cancel()
            try:
                await _flush_task
            except Exception:  # pragma: no cover - best effort
                pass
            _flush_task = None
        _outgoing_queue = None

    try:
        _ws_server = await websockets.serve(
            handler,
            listen_host,
            current_port,
            compression=_WS_COMPRESSION,
            ping_interval=_WS_PING_INTERVAL,
            ping_timeout=_WS_PING_TIMEOUT,
        )
    except OSError as exc:
        await _reset_flush()
        if getattr(exc, "errno", None) in {errno.EADDRINUSE, errno.EACCES}:
            raise RuntimeError(
                f"Event bus websocket port {current_port} already bound"
            ) from exc
        raise
    except Exception:
        await _reset_flush()
        raise

    try:
        sock = _ws_server.sockets[0]
        current_port = sock.getsockname()[1]
    except Exception:  # pragma: no cover - sockets differ by platform
        pass

    _WS_LISTEN_HOST = listen_host
    _WS_LISTEN_PORT = current_port
    DEFAULT_WS_URL = f"ws://{listen_host}:{current_port}"
    os.environ["EVENT_BUS_URL"] = DEFAULT_WS_URL
    os.environ["BROKER_WS_URLS"] = DEFAULT_WS_URL

    await _start_ws_health_server(_WS_LISTEN_HOST, _WS_LISTEN_PORT)

    return _ws_server


def subscribe_ws_topics(ws: Any, topics: Sequence[str] | None) -> None:
    """Update allowed websocket topics for ``ws``."""
    if topics is None:
        _ws_client_topics[ws] = None
    else:
        _ws_client_topics[ws] = {t for t in topics if t}


async def stop_ws_server() -> None:
    """Stop the running websocket server and close client connections."""
    global _ws_server, _flush_task, _outgoing_queue
    if _ws_server is not None:
        _ws_server.close()
        await _ws_server.wait_closed()
        _ws_server = None
    await _stop_ws_health_server()
    if _flush_task is not None:
        _flush_task.cancel()
        try:
            await _flush_task
        except Exception:
            pass
        _flush_task = None
    drained = 0
    if _outgoing_queue is not None:
        while True:
            try:
                _outgoing_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                drained += 1
        if drained:
            logging.getLogger(__name__).info(
                "Event bus websocket drained %d pending frame(s)", drained
            )
    _outgoing_queue = None
    for ws in list(_ws_clients):
        try:
            await ws.close()
        except Exception:
            pass
    _ws_clients.clear()
    _ws_client_topics.clear()


def get_ws_address() -> tuple[str, int]:
    """Return the host/port the local websocket server is listening on."""

    return _WS_LISTEN_HOST, _WS_LISTEN_PORT


async def connect_ws(url: str):
    """Connect to external websocket bus at ``url``."""
    if not websockets:
        raise RuntimeError("websockets library required")
    if not url or not url.startswith(("ws://", "wss://")):
        raise RuntimeError(
            "BROKER_WS_URLS must contain at least one valid ws:// or wss:// URI"
        )

    if _WS_COMPRESSION is None:
        ws = await websockets.connect(
            url,
            ping_interval=_WS_PING_INTERVAL,
            ping_timeout=_WS_PING_TIMEOUT,
        )
    else:
        ws = await websockets.connect(
            url,
            compression=_WS_COMPRESSION,
            ping_interval=_WS_PING_INTERVAL,
            ping_timeout=_WS_PING_TIMEOUT,
        )
    try:
        from . import resource_monitor

        resource_monitor.start_monitor()
    except Exception:  # pragma: no cover - optional psutil failure
        pass
    _peer_clients[url] = ws
    _peer_urls.add(url)
    global _outgoing_queue, _flush_task
    if _outgoing_queue is None:
        _outgoing_queue = Queue(maxsize=max(1, _OUTGOING_QUEUE_MAX))
    if _flush_task is None or _flush_task.done():
        loop = asyncio.get_running_loop()
        _flush_task = loop.create_task(_flush_outgoing())
    _track_ws_task(asyncio.create_task(_receiver(ws)))
    task = _watch_tasks.get(url)
    if task is None or task.done():
        loop = asyncio.get_running_loop()
        task = loop.create_task(_watch_ws(url))
        _watch_tasks[url] = task
        _track_ws_task(task)
    return ws


async def disconnect_ws() -> None:
    """Close websocket connection opened via ``connect_ws``."""
    global _peer_clients, _peer_urls, _watch_tasks, _outgoing_queue, _flush_task
    for ws in list(_peer_clients.values()):
        try:
            await ws.close()
        except Exception:
            pass
    _peer_clients.clear()
    _peer_urls.clear()
    for t in _watch_tasks.values():
        t.cancel()
    _watch_tasks.clear()
    if _flush_task is not None:
        _flush_task.cancel()
        try:
            await _flush_task
        except Exception:
            pass
        _flush_task = None
    _outgoing_queue = None
    try:
        from . import resource_monitor

        resource_monitor.stop_monitor()
    except Exception:
        pass


async def reconnect_ws(url: str | None = None) -> None:
    """(Re)connect the websocket client with exponential backoff."""
    if url:
        _peer_urls.add(url)
    if not websockets:
        return
    urls = [url] if url else list(_peer_urls)
    for u in urls:
        backoff = 1.0
        max_backoff = 30
        while True:
            try:
                ws = _peer_clients.get(u)
                if ws is not None:
                    try:
                        await ws.close()
                    except Exception:
                        pass
                if _WS_COMPRESSION is None:
                    ws = await websockets.connect(
                        u,
                        ping_interval=_WS_PING_INTERVAL,
                        ping_timeout=_WS_PING_TIMEOUT,
                    )
                else:
                    ws = await websockets.connect(
                        u,
                        compression=_WS_COMPRESSION,
                        ping_interval=_WS_PING_INTERVAL,
                        ping_timeout=_WS_PING_TIMEOUT,
                    )
                try:
                    from . import resource_monitor

                    resource_monitor.start_monitor()
                except Exception:  # pragma: no cover - optional psutil failure
                    pass
                _peer_clients[u] = ws
                _track_ws_task(asyncio.create_task(_receiver(ws)))
                task = _watch_tasks.get(u)
                if task is None or task.done():
                    loop = asyncio.get_running_loop()
                    task = loop.create_task(_watch_ws(u))
                    _watch_tasks[u] = task
                    _track_ws_task(task)
                break
            except Exception:  # pragma: no cover - connection errors
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)


async def _watch_ws(url: str) -> None:
    """Background task to watch the websocket connection for ``url``."""
    while url in _peer_urls:
        ws = _peer_clients.get(url)
        if ws is None:
            await reconnect_ws(url)
            ws = _peer_clients.get(url)
        if ws is None:
            await asyncio.sleep(1.0)
            continue
        try:
            await ws.wait_closed()
        except Exception:
            await asyncio.sleep(0.1)
        if url in _peer_urls:
            await reconnect_ws(url)


# Automatically connect to external event bus peers if configured
_ENV_PEERS: Set[str] = set()
_reconnect_task: asyncio.Task | None = None


def _local_ws_disabled() -> bool:
    return os.getenv("EVENT_BUS_DISABLE_LOCAL", "").lower() in {"1", "true", "yes", "on"}


def _validate_ws_urls(urls: Iterable[str]) -> Set[str]:
    """Ensure ``urls`` contains at least one valid websocket URI.

    If no URLs are provided, default to the embedded broker URL.
    """
    urls_set = {u.strip() for u in urls if u and u.strip()}
    if not urls_set:
        if _local_ws_disabled():
            return set()
        logging.warning(
            "BROKER_WS_URLS is empty; falling back to %s", DEFAULT_WS_URL
        )
        urls_set = {DEFAULT_WS_URL}
    invalid = {u for u in urls_set if not u.startswith(("ws://", "wss://"))}
    if invalid:
        raise RuntimeError(
            "BROKER_WS_URLS must contain at least one valid ws:// or wss:// URI",
        )
    return urls_set
async def _reachable_ws_urls(
    urls: Iterable[str], timeout: float = 1.0
) -> Set[str]:
    """Return subset of ``urls`` that respond to a TCP connection."""
    reachable: Set[str] = set()
    for u in urls:
        try:
            parsed = urlparse(u)
            host = parsed.hostname
            if not host:
                continue
            port = parsed.port or (443 if parsed.scheme == "wss" else 80)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout
            )
            writer.close()
            await writer.wait_closed()
            reachable.add(u)
        except Exception:
            continue
    return reachable


def _resolve_ws_urls(cfg) -> Set[str]:
    """Return validated websocket broker URLs from env or config."""
    from .config import get_event_bus_peers, get_event_bus_url

    urls: Set[str] = set()
    env_val = os.getenv("BROKER_WS_URLS")
    if env_val:
        urls.update(u.strip() for u in env_val.split(",") if u.strip())
    else:
        alias = os.getenv("EVENT_BUS_URL")
        if alias:
            urls.add(alias)
        else:
            single = get_event_bus_url(cfg)
            if single:
                urls.add(single)

    urls.update(get_event_bus_peers(cfg))
    try:
        return _validate_ws_urls(urls)
    except RuntimeError:
        logging.warning(
            "Invalid websocket URL configuration; falling back to %s",
            DEFAULT_WS_URL,
        )
        return {DEFAULT_WS_URL}


def _reload_bus(cfg) -> None:
    global _ENV_PEERS, _reconnect_task

    urls = _resolve_ws_urls(cfg)
    if urls == _ENV_PEERS:
        return

    async def _reconnect() -> None:
        await disconnect_ws()
        if not urls:
            logging.getLogger(__name__).info(
                "Event bus local websocket disabled; no peers configured"
            )
            return
        reachable = await _reachable_ws_urls(urls)
        if not reachable:
            if urls == {DEFAULT_WS_URL}:
                # Allow disabling the local websocket server in restricted environments
                disable_local = _local_ws_disabled()
                if disable_local:
                    logging.getLogger(__name__).info(
                        "Local websocket disabled via EVENT_BUS_DISABLE_LOCAL"
                    )
                    reachable = set()
                else:
                    parsed = urlparse(DEFAULT_WS_URL)
                    host = parsed.hostname or "127.0.0.1"
                    port = parsed.port or 8779
                    await start_ws_server(host, port)
                    reachable = {DEFAULT_WS_URL}
            else:
                raise RuntimeError(
                    "No websocket brokers responded at BROKER_WS_URLS. "
                    "Start the broker (e.g. via start.command) or set BROKER_WS_URLS to a running ws:// URL."
                )
        for u in reachable:
            await connect_ws(u)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop:
        async def _runner() -> None:
            task = asyncio.create_task(_reconnect())
            try:
                await task
            except Exception:
                task.cancel()
        _reconnect_task = loop.create_task(_runner())
    else:
        asyncio.run(_reconnect())
    _ENV_PEERS = urls


def shutdown_event_bus() -> None:
    """Best-effort shutdown of background event bus tasks."""

    async def _shutdown() -> None:
        global _reconnect_task, _ws_tasks
        if _reconnect_task is not None:
            _reconnect_task.cancel()
            try:
                await _reconnect_task
            except Exception:
                pass
            _reconnect_task = None
        await disconnect_ws()
        tasks = list(_ws_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        _ws_tasks.clear()
        try:
            await stop_ws_server()
        except Exception:
            pass

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_shutdown())
    else:
        loop.create_task(_shutdown())


subscription("config_updated", _reload_bus).__enter__()


# ---------------------------------------------------------------------------
#  Serialization configuration
# ---------------------------------------------------------------------------


def _reload_serialization(cfg) -> None:
    global _EVENT_SERIALIZATION, _USE_MSGPACK
    val = _get_event_serialization(cfg)
    if not val:
        val = "msgpack" if msgpack is not None else "json"
    val = val.lower()
    _EVENT_SERIALIZATION = val
    _USE_MSGPACK = msgpack is not None and val == "msgpack"


subscription("config_updated", _reload_serialization).__enter__()


# ---------------------------------------------------------------------------
#  Batch interval configuration
# ---------------------------------------------------------------------------


def _reload_batch(cfg) -> None:
    global _EVENT_BATCH_MS
    try:
        val = int(_get_event_batch_ms(cfg) or 0)
    except Exception:
        val = 0
    _EVENT_BATCH_MS = val


subscription("config_updated", _reload_batch).__enter__()


# ---------------------------------------------------------------------------
#  Mmap batch configuration
# ---------------------------------------------------------------------------


def _reload_mmap_batch(cfg) -> None:
    global _EVENT_MMAP_BATCH_MS, _EVENT_MMAP_BATCH_SIZE
    try:
        ms = int(_get_event_mmap_batch_ms(cfg) or 0)
    except Exception:
        ms = 0
    try:
        sz = int(_get_event_mmap_batch_size(cfg) or 0)
    except Exception:
        sz = 0
    _EVENT_MMAP_BATCH_MS = ms
    _EVENT_MMAP_BATCH_SIZE = sz


subscription("config_updated", _reload_mmap_batch).__enter__()


# ---------------------------------------------------------------------------
#  Message broker integration
# ---------------------------------------------------------------------------

_ENV_BROKER: Set[str] = set()


def _get_broker_urls(cfg=None):
    from .config import get_broker_urls
    return get_broker_urls(cfg)


def _reload_broker(cfg) -> None:
    urls = set(_get_broker_urls(cfg))
    if urls == _ENV_BROKER:
        return

    async def _reconnect() -> None:
        await disconnect_broker()
        use_urls = urls
        if urls:
            try:
                await connect_broker(list(urls))
            except RedisConnectionError:
                logging.warning(
                    "Failed to connect to Redis broker; using in-process event bus"
                )
                use_urls = set()
        global _ENV_BROKER
        _ENV_BROKER = use_urls

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None
    if loop:
        loop.create_task(_reconnect())
    else:
        asyncio.run(_reconnect())


subscription("config_updated", _reload_broker).__enter__()

# ---------------------------------------------------------------------------
#  Initialization helper
# ---------------------------------------------------------------------------

def initialize_event_bus() -> None:
    """Reload event bus settings after environment variables are configured."""
    try:
        _reload_serialization(None)
        _reload_bus(None)
        _reload_broker(None)
    except RedisConnectionError:
        logging.warning(
            "Failed to connect to Redis broker during initialization; using in-process event bus"
        )
    except Exception:
        logging.exception("Failed to reload event bus settings")

# initialize mmap on import if configured
open_mmap()


async def send_heartbeat(
    service: str,
    interval: float = 30.0,
    metrics_interval: float | None = None,
) -> None:
    """Publish heartbeat for ``service`` every ``interval`` seconds."""
    if metrics_interval:
        from . import resource_monitor

        resource_monitor.start_monitor(metrics_interval)
    while True:
        publish("heartbeat", {"service": service})
        await asyncio.sleep(interval)
