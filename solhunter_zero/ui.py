from __future__ import annotations

import asyncio
import contextlib
import errno
import json
import logging
import os
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from queue import Queue
from typing import Any, Callable, Dict, Iterable, List, Optional
from urllib.parse import urlparse, urlunparse

from flask import Flask, Request, Response, jsonify, render_template_string, request
from werkzeug.serving import BaseWSGIServer, make_server

from .agents.discovery import (
    DEFAULT_DISCOVERY_METHOD,
    DISCOVERY_METHODS,
    resolve_discovery_method,
)


log = logging.getLogger(__name__)


try:  # pragma: no cover - imported lazily in tests
    import websockets
except ImportError:  # pragma: no cover - optional dependency
    websockets = None  # type: ignore[assignment]


_WS_HOST_ENV_KEYS = ("UI_WS_HOST", "UI_HOST")
_RL_WS_PORT_DEFAULT = 0
_EVENT_WS_PORT_DEFAULT = 0
_LOG_WS_PORT_DEFAULT = 0
_WS_QUEUE_DEFAULT = 512
_backlog_env = os.getenv("UI_WS_BACKLOG_MAX", "64")
try:
    BACKLOG_MAX = int(_backlog_env or 64)
except (TypeError, ValueError):
    log.warning("Invalid backlog value %r; using default 64", _backlog_env)
    BACKLOG_MAX = 64
else:
    if BACKLOG_MAX < 0:
        log.warning("Negative backlog %s not allowed; using default 64", BACKLOG_MAX)
        BACKLOG_MAX = 64
_ADDR_IN_USE_ERRNOS = {errno.EADDRINUSE}


rl_ws_loop: asyncio.AbstractEventLoop | None = None
event_ws_loop: asyncio.AbstractEventLoop | None = None
log_ws_loop: asyncio.AbstractEventLoop | None = None

_RL_WS_PORT = _RL_WS_PORT_DEFAULT
_EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
_LOG_WS_PORT = _LOG_WS_PORT_DEFAULT


class _WebsocketState:
    __slots__ = (
        "loop",
        "server",
        "clients",
        "queue",
        "task",
        "thread",
        "port",
        "name",
        "host",
        "metrics",
    )

    def __init__(self, name: str) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.server: Any | None = None
        self.clients: set[Any] = set()
        self.queue: asyncio.Queue[str] | None = None
        self.task: asyncio.Task[Any] | None = None
        self.thread: threading.Thread | None = None
        self.port: int = 0
        self.name = name
        self.host: str | None = None
        self.metrics = _ChannelMetrics()


class _ChannelMetrics:
    __slots__ = (
        "lock",
        "queue_limit",
        "queue_depth",
        "max_queue_depth",
        "total_enqueued",
        "queue_drops",
        "send_failures",
        "backlog_depth",
        "max_backlog_depth",
        "close_codes",
    )

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.queue_limit: int | None = None
        self.queue_depth = 0
        self.max_queue_depth = 0
        self.total_enqueued = 0
        self.queue_drops = 0
        self.send_failures = 0
        self.backlog_depth = 0
        self.max_backlog_depth = 0
        self.close_codes: list[int] = []

    def reset(self, queue_limit: int | None = None) -> None:
        with self.lock:
            self.queue_limit = queue_limit
            self.queue_depth = 0
            self.max_queue_depth = 0
            self.total_enqueued = 0
            self.queue_drops = 0
            self.send_failures = 0
            self.backlog_depth = 0
            self.max_backlog_depth = 0
            self.close_codes.clear()

    def record_enqueue(self, depth: int, dropped: bool) -> None:
        with self.lock:
            self.queue_depth = depth
            if depth > self.max_queue_depth:
                self.max_queue_depth = depth
            self.total_enqueued += 1
            if dropped:
                self.queue_drops += 1

    def record_dequeue(self, depth: int) -> None:
        with self.lock:
            self.queue_depth = depth

    def record_backlog(self, depth: int) -> None:
        with self.lock:
            self.backlog_depth = depth
            if depth > self.max_backlog_depth:
                self.max_backlog_depth = depth

    def record_send_failure(self, count: int = 1) -> None:
        if count <= 0:
            return
        with self.lock:
            self.send_failures += count

    def record_close(self, code: int | None) -> None:
        if code is None:
            return
        with self.lock:
            self.close_codes.append(int(code))

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            dropped = self.queue_drops + self.send_failures
            return {
                "queue_limit": self.queue_limit,
                "queue_depth": self.queue_depth,
                "max_queue_depth": self.max_queue_depth,
                "total_enqueued": self.total_enqueued,
                "queue_drops": self.queue_drops,
                "send_failures": self.send_failures,
                "dropped_frames": dropped,
                "backlog_depth": self.backlog_depth,
                "max_backlog_depth": self.max_backlog_depth,
                "close_codes": list(self.close_codes),
            }


_WS_CHANNELS: dict[str, _WebsocketState] = {
    "rl": _WebsocketState("rl"),
    "events": _WebsocketState("events"),
    "logs": _WebsocketState("logs"),
}


def _resolve_host() -> str:
    for key in _WS_HOST_ENV_KEYS:
        host = os.getenv(key)
        if host:
            return host
    return "127.0.0.1"


def _parse_port(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        port = int(value)
    except (TypeError, ValueError):
        log.warning("Invalid port value %r; using default %s", value, default)
        return default
    if port < 0:
        log.warning("Negative port %s not allowed; using default %s", port, default)
        return default
    return port


def _resolve_port(*keys: str, default: int) -> int:
    for key in keys:
        env_value = os.getenv(key)
        if env_value is not None and env_value != "":
            return _parse_port(env_value, default)
    return default


def _parse_positive_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        log.warning("Invalid integer value %r; using default %s", value, default)
        return default
    if parsed <= 0:
        log.warning("Non-positive integer %s not allowed; using default %s", parsed, default)
        return default
    return parsed


def _format_payload(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload)
    except TypeError:
        return str(payload)


def _enqueue_message(channel: str, payload: Any) -> bool:
    state = _WS_CHANNELS.get(channel)
    if not state or state.loop is None or state.queue is None:
        return False

    message = _format_payload(payload)

    def _put() -> None:
        if state.queue is None:
            return
        dropped = False
        try:
            state.queue.put_nowait(message)
        except asyncio.QueueFull:
            dropped = True
            try:
                state.queue.get_nowait()
                state.queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - race
                pass
            try:
                state.queue.put_nowait(message)
            except asyncio.QueueFull:
                # queue is still full; drop the frame
                pass
        depth = state.queue.qsize()
        state.metrics.record_enqueue(depth, dropped)

    state.loop.call_soon_threadsafe(_put)
    return True


def push_event(payload: Any) -> bool:
    """Broadcast *payload* to UI event websocket listeners."""

    return _enqueue_message("events", payload)


def push_rl(payload: Any) -> bool:
    """Broadcast *payload* to RL websocket listeners."""

    return _enqueue_message("rl", payload)


def push_log(payload: Any) -> bool:
    """Broadcast *payload* to log websocket listeners."""

    return _enqueue_message("logs", payload)


def _normalize_ws_url(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.startswith(("ws://", "wss://")):
        return candidate
    return None


def _infer_ws_scheme(request_scheme: str | None = None) -> str:
    override = (os.getenv("UI_WS_SCHEME") or os.getenv("WS_SCHEME") or "").strip().lower()
    if override in {"ws", "wss"}:
        return override
    if request_scheme and request_scheme.lower() in {"https", "wss"}:
        return "wss"
    return "ws"


def _split_netloc(netloc: str | None) -> tuple[str | None, int | None]:
    if not netloc:
        return None, None
    parsed = urlparse(f"//{netloc}", scheme="http")
    return parsed.hostname, parsed.port


def _channel_path(channel: str) -> str:
    override_map = {
        "rl": "UI_RL_WS_PATH",
        "events": "UI_EVENTS_WS_PATH",
        "logs": "UI_LOGS_WS_PATH",
    }
    override_key = override_map.get(channel)
    override = os.getenv(override_key) if override_key else None
    if override:
        candidate = override
    else:
        template = os.getenv("UI_WS_PATH_TEMPLATE")
        if template:
            try:
                candidate = template.format(channel=channel)
            except Exception:
                log.warning(
                    "Invalid UI_WS_PATH_TEMPLATE %r; falling back to default",
                    template,
                )
                candidate = f"/ws/{channel}"
        else:
            candidate = f"/ws/{channel}"
    if not candidate:
        candidate = f"/ws/{channel}"
    if not candidate.startswith("/"):
        candidate = "/" + candidate.lstrip("/")
    return candidate


def get_ws_urls() -> dict[str, str]:
    """Return websocket URLs for RL, events, and logs channels."""

    channel_env_keys: dict[str, tuple[str, ...]] = {
        "events": ("UI_EVENTS_WS", "UI_EVENTS_WS_URL", "UI_WS_URL"),
        "rl": ("UI_RL_WS", "UI_RL_WS_URL"),
        "logs": ("UI_LOGS_WS", "UI_LOG_WS_URL"),
    }
    urls: dict[str, str] = {}
    for channel, env_keys in channel_env_keys.items():
        resolved: str | None = None
        for env_key in env_keys:
            resolved = _normalize_ws_url(os.environ.get(env_key))
            if resolved:
                break
        if not resolved:
            state = _WS_CHANNELS.get(channel)
            host = state.host if state and state.host else _resolve_host()
            url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
            if channel == "rl":
                port = state.port or _RL_WS_PORT
            elif channel == "events":
                port = state.port or _EVENT_WS_PORT
            else:
                port = state.port or _LOG_WS_PORT
            path = _channel_path(channel)
            scheme = _infer_ws_scheme()
            resolved = f"{scheme}://{url_host}:{port}{path}"
        urls[channel] = resolved
    return urls


def get_ws_metrics() -> Dict[str, Dict[str, Any]]:
    """Return per-channel websocket queue and connection metrics."""

    return {name: state.metrics.snapshot() for name, state in _WS_CHANNELS.items()}


def build_ui_manifest(req: Request | None = None) -> Dict[str, Any]:
    urls = get_ws_urls()
    scheme_hint = _infer_ws_scheme(getattr(req, "scheme", None))
    public_host_env = os.getenv("UI_PUBLIC_HOST") or os.getenv("UI_EXTERNAL_HOST")
    public_host, _ = _split_netloc(public_host_env)
    request_host, _ = _split_netloc(getattr(req, "host", None))

    manifest: Dict[str, Any] = {}
    for channel in ("rl", "events", "logs"):
        raw_url = urls.get(channel, "")
        parsed = urlparse(raw_url)
        host = public_host or parsed.hostname or request_host or _resolve_host()
        port = parsed.port
        path = parsed.path or ""
        if path in {"", "/", "/ws"}:
            path = _channel_path(channel)
        if not path.startswith("/"):
            path = "/" + path.lstrip("/")
        scheme = parsed.scheme or scheme_hint
        netloc = host or ""
        if port:
            netloc = f"{host}:{port}"
        manifest[f"{channel}_ws"] = urlunparse((scheme, netloc, path, "", "", ""))

    ui_port_value = os.getenv("UI_PORT") or os.getenv("PORT")
    manifest["ui_port"] = _parse_port(ui_port_value, 5000)
    return manifest
def _shutdown_state(state: _WebsocketState) -> None:
    loop = state.loop
    if loop is None:
        return

    def _stop_loop() -> None:
        loop.stop()

    loop.call_soon_threadsafe(_stop_loop)
    thread = state.thread
    if thread is not None:
        thread.join(timeout=2)
    state.thread = None
    state.host = None


def _close_server(loop: asyncio.AbstractEventLoop, state: _WebsocketState) -> None:
    server = state.server
    if server is not None:
        server.close()
        with contextlib.suppress(Exception):
            loop.run_until_complete(server.wait_closed())
    state.server = None

    for ws in list(state.clients):
        with contextlib.suppress(Exception):
            loop.run_until_complete(ws.close(code=1012, reason="server shutdown"))
    state.clients.clear()

    if state.task is not None:
        state.task.cancel()
        with contextlib.suppress(BaseException):
            loop.run_until_complete(state.task)
    state.task = None

    if state.queue is not None:
        with contextlib.suppress(Exception):
            loop.run_until_complete(state.queue.join())
    state.queue = None

    with contextlib.suppress(Exception):
        loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    state.loop = None
    state.thread = None
    state.port = 0
    state.host = None


def _start_channel(
    channel: str,
    *,
    host: str,
    port: int,
    queue_size: int,
    ping_interval: float,
    ping_timeout: float,
) -> threading.Thread:
    state = _WS_CHANNELS[channel]
    ready: Queue[Any] = Queue(maxsize=1)

    def _run() -> None:
        nonlocal port
        global rl_ws_loop, event_ws_loop, log_ws_loop
        global _RL_WS_PORT, _EVENT_WS_PORT, _LOG_WS_PORT
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        state.loop = loop
        state.port = 0
        state.host = host

        if channel == "rl":
            rl_ws_loop = loop
        elif channel == "events":
            event_ws_loop = loop
        else:
            log_ws_loop = loop

        queue = asyncio.Queue[str](maxsize=queue_size)
        state.queue = queue
        state.metrics.reset(queue_size)
        backlog: deque[str] = deque()

        async def _broadcast_loop() -> None:
            while True:
                message = await queue.get()
                stale_clients: list[Any] = []
                for ws in list(state.clients):
                    try:
                        await ws.send(message)
                    except Exception:
                        stale_clients.append(ws)
                for ws in stale_clients:
                    state.clients.discard(ws)
                if stale_clients:
                    state.metrics.record_send_failure(len(stale_clients))
                backlog.append(message)
                if len(backlog) > BACKLOG_MAX:
                    backlog.popleft()
                state.metrics.record_backlog(len(backlog))
                queue.task_done()
                state.metrics.record_dequeue(queue.qsize())

        async def _handler(websocket, path) -> None:  # type: ignore[override]
            parsed = urlparse(path or "")
            req_path = parsed.path or "/"
            template_path = _channel_path(channel)
            allowed_paths = {
                "",
                "/",
                "/ws",
                f"/{channel}",
                f"/{channel}/",
                f"/ws/{channel}",
                f"/ws/{channel}/",
                template_path,
                template_path.rstrip("/") + "/",
            }
            if req_path not in allowed_paths:
                await websocket.close(code=1008, reason="invalid path")
                state.metrics.record_close(getattr(websocket, "close_code", 1008))
                return
            state.clients.add(websocket)
            hello = json.dumps({"channel": channel, "event": "hello"})
            try:
                await websocket.send(hello)
            except Exception:
                state.clients.discard(websocket)
                state.metrics.record_close(getattr(websocket, "close_code", None))
                return
            try:
                for cached in list(backlog):
                    try:
                        await websocket.send(cached)
                    except Exception:
                        break
                await websocket.wait_closed()
            finally:
                state.clients.discard(websocket)
                state.metrics.record_close(getattr(websocket, "close_code", None))

        server = None
        last_exc: Exception | None = None

        for candidate_port in (port, 0) if port != 0 else (0,):
            try:
                server = loop.run_until_complete(
                    websockets.serve(  # type: ignore[call-arg]
                        _handler,
                        host,
                        candidate_port,
                        ping_interval=ping_interval,
                        ping_timeout=ping_timeout,
                    )
                )
            except OSError as exc:
                last_exc = exc
                if exc.errno in _ADDR_IN_USE_ERRNOS and candidate_port != 0:
                    log.warning(
                        "%s websocket port %s unavailable; retrying with automatic port",
                        channel,
                        candidate_port,
                    )
                    continue
                ready.put(exc)
                state.loop = None
                state.host = None
                if channel == "rl":
                    rl_ws_loop = None
                    _RL_WS_PORT = _RL_WS_PORT_DEFAULT
                elif channel == "events":
                    event_ws_loop = None
                    _EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
                else:
                    log_ws_loop = None
                    _LOG_WS_PORT = _LOG_WS_PORT_DEFAULT
                try:
                    loop.close()
                except Exception:
                    pass
                return
            except Exception as exc:  # pragma: no cover - unexpected startup failure
                ready.put(exc)
                state.loop = None
                state.host = None
                if channel == "rl":
                    rl_ws_loop = None
                    _RL_WS_PORT = _RL_WS_PORT_DEFAULT
                elif channel == "events":
                    event_ws_loop = None
                    _EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
                else:
                    log_ws_loop = None
                    _LOG_WS_PORT = _LOG_WS_PORT_DEFAULT
                try:
                    loop.close()
                except Exception:
                    pass
                return
            else:
                bound_port = candidate_port
                if getattr(server, "sockets", None):
                    try:
                        sockname = server.sockets[0].getsockname()
                        bound_port = sockname[1] if isinstance(sockname, tuple) else bound_port
                    except Exception:
                        pass
                previous_port = port
                bound_port = bound_port or previous_port
                if candidate_port == 0 and bound_port == previous_port:
                    log.error(
                        "Unable to determine dynamically assigned port for %s websocket",
                        channel,
                    )
                    if server is not None:
                        with contextlib.suppress(Exception):
                            server.close()
                            loop.run_until_complete(server.wait_closed())
                    server = None
                    continue
                port = bound_port
                state.port = bound_port
                if channel == "rl":
                    _RL_WS_PORT = bound_port
                elif channel == "events":
                    _EVENT_WS_PORT = bound_port
                else:
                    _LOG_WS_PORT = bound_port
                if candidate_port == 0 and bound_port != previous_port:
                    log.info(
                        "%s websocket using dynamically assigned port %s", channel, bound_port
                    )
                break

        if server is None:
            ready.put(last_exc or RuntimeError(f"Unable to start {channel} websocket"))
            state.loop = None
            state.host = None
            if channel == "rl":
                rl_ws_loop = None
                _RL_WS_PORT = _RL_WS_PORT_DEFAULT
            elif channel == "events":
                event_ws_loop = None
                _EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
            else:
                log_ws_loop = None
                _LOG_WS_PORT = _LOG_WS_PORT_DEFAULT
            try:
                loop.close()
            except Exception:
                pass
            return

        state.server = server
        state.task = loop.create_task(_broadcast_loop())
        ready.put(None)

        try:
            loop.run_forever()
        finally:
            _close_server(loop, state)

    thread = threading.Thread(target=_run, name=f"ui-ws-{channel}", daemon=True)
    thread.start()
    state.thread = thread

    ready_timeout_raw = os.getenv("UI_WS_READY_TIMEOUT", "5")
    try:
        ready_timeout = float(ready_timeout_raw or 5)
    except (TypeError, ValueError):
        log.warning(
            "Invalid UI_WS_READY_TIMEOUT value %r; using default 5 seconds",
            ready_timeout_raw,
        )
        ready_timeout = 5.0
    try:
        result = ready.get(timeout=ready_timeout)
    except Exception as exc:  # pragma: no cover - unexpected queue failure
        _shutdown_state(state)
        raise RuntimeError(f"Timeout starting {channel} websocket") from exc

    if isinstance(result, Exception):
        _shutdown_state(state)
        raise RuntimeError(
            f"{channel} websocket failed to bind on {host}:{port}: {result}"
        ) from result

    return thread


def start_websockets() -> dict[str, threading.Thread]:
    """Launch UI websocket endpoints for RL, runtime events, and logs."""

    if websockets is None:
        log.warning("UI websockets unavailable: install the 'websockets' package")
        return {}

    if all(state.loop is not None for state in _WS_CHANNELS.values()):
        return {
            name: state.thread
            for name, state in _WS_CHANNELS.items()
            if state.thread is not None
        }

    threads: dict[str, threading.Thread] = {}
    host = _resolve_host()
    queue_size = _parse_positive_int(os.getenv("UI_WS_QUEUE_SIZE"), _WS_QUEUE_DEFAULT)
    ping_interval = float(os.getenv("UI_WS_PING_INTERVAL", os.getenv("WS_PING_INTERVAL", "20")))
    ping_timeout = float(os.getenv("UI_WS_PING_TIMEOUT", os.getenv("WS_PING_TIMEOUT", "20")))
    url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    scheme = _infer_ws_scheme()

    rl_port = _resolve_port("UI_RL_WS_PORT", "RL_WS_PORT", default=_RL_WS_PORT_DEFAULT)
    log_port = _resolve_port("UI_LOG_WS_PORT", default=_LOG_WS_PORT_DEFAULT)
    event_port = _resolve_port("UI_EVENT_WS_PORT", "EVENT_WS_PORT", default=_EVENT_WS_PORT_DEFAULT)

    try:
        threads["rl"] = _start_channel(
            "rl",
            host=host,
            port=rl_port,
            queue_size=queue_size,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        )
        threads["events"] = _start_channel(
            "events",
            host=host,
            port=event_port,
            queue_size=queue_size,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        )
        threads["logs"] = _start_channel(
            "logs",
            host=host,
            port=log_port,
            queue_size=queue_size,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        )
    except Exception:
        for state in _WS_CHANNELS.values():
            _shutdown_state(state)
        raise

    events_url = f"{scheme}://{url_host}:{_EVENT_WS_PORT}{_channel_path('events')}"
    rl_url = f"{scheme}://{url_host}:{_RL_WS_PORT}{_channel_path('rl')}"
    logs_url = f"{scheme}://{url_host}:{_LOG_WS_PORT}{_channel_path('logs')}"

    defaults = {
        "UI_WS_URL": events_url,
        "UI_EVENTS_WS_URL": events_url,
        "UI_EVENTS_WS": events_url,
        "UI_RL_WS_URL": rl_url,
        "UI_RL_WS": rl_url,
        "UI_LOG_WS_URL": logs_url,
        "UI_LOGS_WS": logs_url,
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)
    log.info(
        "UI websockets listening on rl=%s events=%s logs=%s",
        _RL_WS_PORT,
        _EVENT_WS_PORT,
        _LOG_WS_PORT,
    )
    return threads


def stop_websockets() -> None:
    """Shut down all websocket channels."""

    for state in _WS_CHANNELS.values():
        _shutdown_state(state)


StatusProvider = Callable[[], Dict[str, Any]]
ListProvider = Callable[[], Iterable[Dict[str, Any]]]
DictProvider = Callable[[], Dict[str, Any]]


@dataclass
class UIState:
    """Holds callables that provide live data to the UI endpoints."""

    status_provider: StatusProvider = field(
        default=lambda: {"event_bus": False, "trading_loop": False}
    )
    activity_provider: ListProvider = field(default=lambda: [])
    trades_provider: ListProvider = field(default=lambda: [])
    weights_provider: DictProvider = field(default=lambda: {})
    rl_status_provider: DictProvider = field(default=lambda: {})
    logs_provider: ListProvider = field(default=lambda: [])
    summary_provider: DictProvider = field(default=lambda: {})
    discovery_provider: DictProvider = field(default=lambda: {"recent": []})
    config_provider: DictProvider = field(default=lambda: {})
    actions_provider: ListProvider = field(default=lambda: [])
    history_provider: ListProvider = field(default=lambda: [])
    discovery_console_provider: DictProvider = field(
        default=lambda: {"candidates": [], "stats": {}}
    )
    token_facts_provider: DictProvider = field(
        default=lambda: {"tokens": {}, "selected": None}
    )
    market_state_provider: DictProvider = field(
        default=lambda: {"markets": [], "updated_at": None}
    )
    golden_snapshot_provider: DictProvider = field(
        default=lambda: {"snapshots": [], "hash_map": {}}
    )
    suggestions_provider: DictProvider = field(
        default=lambda: {"suggestions": [], "metrics": {}}
    )
    exit_provider: DictProvider = field(
        default=lambda: {"hot_watch": [], "diagnostics": [], "closed": [], "queue": [], "missed_exits": []}
    )
    vote_windows_provider: DictProvider = field(
        default=lambda: {"windows": [], "decisions": []}
    )
    shadow_provider: DictProvider = field(
        default=lambda: {"virtual_fills": [], "paper_positions": [], "live_fills": []}
    )
    rl_provider: DictProvider = field(
        default=lambda: {"weights": {}, "uplift": {}}
    )
    settings_provider: DictProvider = field(
        default=lambda: {"controls": {}, "overrides": {}, "staleness": {}}
    )
    bus_metrics_provider: DictProvider = field(
        default=lambda: {"streams": {}}
    )

    def snapshot_status(self) -> Dict[str, Any]:
        try:
            return dict(self.status_provider())
        except Exception:  # pragma: no cover - defensive coding
            log.exception("UI status provider failed")
            return {"event_bus": False, "trading_loop": False}

    def snapshot_activity(self) -> List[Dict[str, Any]]:
        try:
            return list(self.activity_provider())
        except Exception:  # pragma: no cover
            log.exception("UI activity provider failed")
            return []

    def snapshot_trades(self) -> List[Dict[str, Any]]:
        try:
            return list(self.trades_provider())
        except Exception:  # pragma: no cover
            log.exception("UI trades provider failed")
            return []

    def snapshot_weights(self) -> Dict[str, Any]:
        try:
            return dict(self.weights_provider())
        except Exception:  # pragma: no cover
            log.exception("UI weights provider failed")
            return {}

    def snapshot_rl(self) -> Dict[str, Any]:
        try:
            return dict(self.rl_status_provider())
        except Exception:  # pragma: no cover
            log.exception("UI RL status provider failed")
            return {}

    def snapshot_logs(self) -> List[Dict[str, Any]]:
        try:
            return list(self.logs_provider())
        except Exception:  # pragma: no cover
            log.exception("UI log provider failed")
            return []

    def snapshot_summary(self) -> Dict[str, Any]:
        try:
            return dict(self.summary_provider())
        except Exception:  # pragma: no cover
            log.exception("UI summary provider failed")
            return {}

    def snapshot_discovery(self) -> Dict[str, Any]:
        try:
            data = self.discovery_provider()
            if isinstance(data, dict):
                return dict(data)
            return {"recent": list(data)}
        except Exception:  # pragma: no cover
            log.exception("UI discovery provider failed")
            return {"recent": []}

    def snapshot_config(self) -> Dict[str, Any]:
        try:
            return dict(self.config_provider())
        except Exception:  # pragma: no cover
            log.exception("UI config provider failed")
            return {}

    def snapshot_history(self) -> List[Dict[str, Any]]:
        try:
            return list(self.history_provider())
        except Exception:  # pragma: no cover
            log.exception("UI history provider failed")
            return []

    def snapshot_actions(self) -> List[Dict[str, Any]]:
        try:
            return list(self.actions_provider())
        except Exception:  # pragma: no cover
            log.exception("UI actions provider failed")
            return []

    def snapshot_discovery_console(self) -> Dict[str, Any]:
        try:
            return dict(self.discovery_console_provider())
        except Exception:  # pragma: no cover
            log.exception("UI discovery console provider failed")
            return {"candidates": [], "stats": {}}

    def snapshot_token_facts(self) -> Dict[str, Any]:
        try:
            return dict(self.token_facts_provider())
        except Exception:  # pragma: no cover
            log.exception("UI token facts provider failed")
            return {"tokens": {}, "selected": None}

    def snapshot_market_state(self) -> Dict[str, Any]:
        try:
            return dict(self.market_state_provider())
        except Exception:  # pragma: no cover
            log.exception("UI market state provider failed")
            return {"markets": [], "updated_at": None}

    def snapshot_golden_snapshots(self) -> Dict[str, Any]:
        try:
            return dict(self.golden_snapshot_provider())
        except Exception:  # pragma: no cover
            log.exception("UI golden snapshot provider failed")
            return {"snapshots": [], "hash_map": {}}

    def snapshot_suggestions(self) -> Dict[str, Any]:
        try:
            return dict(self.suggestions_provider())
        except Exception:  # pragma: no cover
            log.exception("UI suggestions provider failed")
            return {"suggestions": [], "metrics": {}}

    def snapshot_exit(self) -> Dict[str, Any]:
        try:
            data = self.exit_provider()
            if not isinstance(data, dict):
                data = {"hot_watch": list(data)}
            payload = {"hot_watch": [], "diagnostics": [], "closed": [], "queue": [], "missed_exits": []}
            payload.update(data)
            return payload
        except Exception:  # pragma: no cover
            log.exception("UI exit provider failed")
            return {"hot_watch": [], "diagnostics": [], "closed": [], "queue": [], "missed_exits": []}

    def snapshot_vote_windows(self) -> Dict[str, Any]:
        try:
            return dict(self.vote_windows_provider())
        except Exception:  # pragma: no cover
            log.exception("UI vote windows provider failed")
            return {"windows": [], "decisions": []}

    def snapshot_shadow(self) -> Dict[str, Any]:
        try:
            return dict(self.shadow_provider())
        except Exception:  # pragma: no cover
            log.exception("UI shadow provider failed")
            return {"virtual_fills": [], "paper_positions": [], "live_fills": []}

    def snapshot_rl_panel(self) -> Dict[str, Any]:
        try:
            return dict(self.rl_provider())
        except Exception:  # pragma: no cover
            log.exception("UI RL panel provider failed")
            return {"weights": {}, "uplift": {}}

    def snapshot_settings(self) -> Dict[str, Any]:
        try:
            return dict(self.settings_provider())
        except Exception:  # pragma: no cover
            log.exception("UI settings provider failed")
            return {"controls": {}, "overrides": {}, "staleness": {}}

    def snapshot_bus_metrics(self) -> Dict[str, Any]:
        try:
            data = self.bus_metrics_provider()
            if isinstance(data, dict):
                return dict(data)
        except Exception:  # pragma: no cover
            log.exception("UI bus metrics provider failed")
        return {"streams": {}}


_PAGE_TEMPLATE = """
<!doctype html>
<html lang=\"en\">
<head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>SolHunter Zero · Swarm Console</title>
    <style>
        :root {
            color-scheme: dark;
            font-family: 'Inter', 'Segoe UI', sans-serif;
            --bg: #0d1117;
            --panel: rgba(20, 27, 36, 0.88);
            --border: rgba(88, 166, 255, 0.15);
            --accent: #58a6ff;
            --danger: #ff7b72;
            --warning: #f2cc60;
            --success: #3fb950;
            --muted: #8b949e;
        }
        body {
            margin: 0;
            padding: 0;
            background: radial-gradient(circle at top, rgba(88,166,255,0.12), transparent 55%), var(--bg);
            color: #e6edf3;
            min-height: 100vh;
            line-height: 1.5;
        }
        .layout {
            max-width: 1280px;
            margin: 0 auto;
            padding: 32px 24px 64px;
            display: grid;
            gap: 24px;
        }
        header {
            background: linear-gradient(160deg, rgba(20,27,36,0.9), rgba(12,16,24,0.9));
            border: 1px solid var(--border);
            border-radius: 20px;
            padding: 24px;
            display: grid;
            gap: 18px;
            box-shadow: 0 18px 44px rgba(0,0,0,0.45);
        }
        header h1 {
            margin: 0;
            font-size: 1.9rem;
        }
        .connection-banner {
            border-radius: 14px;
            padding: 12px 16px;
            display: flex;
            flex-direction: column;
            gap: 4px;
            background: rgba(88,166,255,0.14);
            border: 1px solid rgba(88,166,255,0.28);
            transition: background 0.2s ease, border-color 0.2s ease;
        }
        .connection-banner strong {
            font-weight: 600;
        }
        .connection-banner--connecting {
            background: rgba(88,166,255,0.14);
            border-color: rgba(88,166,255,0.28);
        }
        .connection-banner--ok {
            background: rgba(63,185,80,0.16);
            border-color: rgba(63,185,80,0.35);
        }
        .connection-banner--warning {
            background: rgba(242,204,96,0.18);
            border-color: rgba(242,204,96,0.4);
        }
        .connection-banner--error {
            background: rgba(255,123,114,0.18);
            border-color: rgba(255,123,114,0.4);
        }
        .connection-detail {
            color: var(--muted);
            font-size: 0.85rem;
        }
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 12px;
        }
        .status-card {
            border-radius: 14px;
            padding: 14px;
            background: rgba(13,17,23,0.72);
            border: 1px solid rgba(88,166,255,0.12);
        }
        .status-card.ok { border-color: rgba(63,185,80,0.32); }
        .status-card.fail { border-color: rgba(255,123,114,0.32); }
        .status-card.warn { border-color: rgba(242,204,96,0.35); }
        .header-top {
            display: flex;
            justify-content: space-between;
            gap: 18px;
            flex-wrap: wrap;
        }
        .header-signals {
            display: flex;
            align-items: center;
            gap: 10px;
            flex-wrap: wrap;
        }
        .signal-pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 0.78rem;
            letter-spacing: 0.05em;
            text-transform: uppercase;
            background: rgba(13,17,23,0.72);
            border: 1px solid rgba(88,166,255,0.2);
        }
        .signal-pill.env-dev { border-color: rgba(88,166,255,0.45); color: #58a6ff; }
        .signal-pill.env-STAGE, .signal-pill.env-stage { border-color: rgba(242,204,96,0.45); color: var(--warning); }
        .signal-pill.env-PROD, .signal-pill.env-prod { border-color: rgba(63,185,80,0.5); color: var(--success); }
        .signal-pill.toggle-on { border-color: rgba(63,185,80,0.5); color: var(--success); }
        .signal-pill.toggle-off { border-color: rgba(88,166,255,0.2); color: var(--muted); }
        .signal-pill.toggle-paused { border-color: rgba(255,123,114,0.55); color: var(--danger); }
        .header-actions {
            display: flex;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }
        .header-actions button {
            background: rgba(88,166,255,0.12);
            color: #e6edf3;
            border: 1px solid rgba(88,166,255,0.35);
            border-radius: 999px;
            padding: 8px 16px;
            font-size: 0.85rem;
            cursor: pointer;
            transition: background 0.2s ease;
        }
        .header-actions button:hover { background: rgba(88,166,255,0.22); }
        .signal-metrics {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
            gap: 12px;
        }
        .signal-metric {
            background: rgba(13,17,23,0.6);
            border-radius: 12px;
            padding: 10px 12px;
            border: 1px solid rgba(88,166,255,0.12);
        }
        .color-chip {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 6px;
        }
        .color-chip.ok { background: rgba(63,185,80,0.85); }
        .color-chip.warn { background: rgba(242,204,96,0.85); }
        .color-chip.danger { background: rgba(255,123,114,0.9); }
        .color-chip.idle { background: rgba(88,166,255,0.65); }
        .pill {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            border-radius: 999px;
            padding: 3px 10px;
            font-size: 0.75rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            border: 1px solid rgba(88,166,255,0.25);
            color: var(--muted);
        }
        .pill.stale { color: var(--danger); border-color: rgba(255,123,114,0.45); }
        .pill.fresh { color: var(--success); border-color: rgba(63,185,80,0.35); }
        .pill.pass { color: var(--success); border-color: rgba(63,185,80,0.35); }
        .pill.blocked { color: var(--danger); border-color: rgba(255,123,114,0.45); }
        .pill.neutral { color: var(--muted); border-color: rgba(110,118,129,0.35); }
        .grid-two {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
            gap: 24px;
        }
        .panel {
            background: var(--panel);
            border-radius: 18px;
            border: 1px solid var(--border);
            padding: 18px 20px 22px;
            box-shadow: 0 14px 38px rgba(0,0,0,0.35);
        }
        .panel.skeleton::after {
            content: "";
            display: block;
            height: 4px;
            margin-top: 12px;
            background: linear-gradient(90deg, rgba(88,166,255,0.1), rgba(88,166,255,0.35), rgba(88,166,255,0.1));
            background-size: 200% 100%;
            animation: shimmer 1.4s infinite;
            border-radius: 4px;
        }
        @keyframes shimmer {
            0% { background-position: 200% 0; }
            100% { background-position: -200% 0; }
        }
        .panel h2 {
            margin-top: 0;
            font-size: 1.25rem;
            letter-spacing: 0.02em;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 12px;
            font-size: 0.92rem;
        }
        th, td {
            padding: 8px 10px;
            border-bottom: 1px solid rgba(88,166,255,0.08);
        }
        th { text-align: left; font-weight: 500; color: rgba(230,237,243,0.75); }
        tbody tr:hover { background: rgba(88,166,255,0.06); }
        tbody tr.stale { background: rgba(255,123,114,0.06); }
        .muted { color: var(--muted); }
        .section-title {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
        }
        .metrics {
            display: flex;
            flex-wrap: wrap;
            gap: 12px;
            margin-top: 12px;
        }
        .metric {
            background: rgba(13,17,23,0.72);
            border-radius: 12px;
            padding: 10px 12px;
            border: 1px solid rgba(88,166,255,0.12);
            min-width: 140px;
        }
        .metric strong { display: block; font-size: 1.1rem; }
        .badge-grid {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
        }
        .filter-bar {
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
            margin-top: 12px;
        }
        .filter-bar input,
        .filter-bar select {
            background: rgba(13,17,23,0.6);
            border: 1px solid rgba(88,166,255,0.18);
            border-radius: 8px;
            padding: 6px 10px;
            color: #e6edf3;
        }
        .mint-chip {
            color: var(--accent);
            font-weight: 600;
            text-decoration: none;
        }
        .mint-chip:hover { text-decoration: underline; }
        .decision-card {
            position: relative;
        }
        .decision-card .badge {
            position: absolute;
            top: 8px;
            right: 10px;
            font-size: 0.7rem;
            padding: 3px 8px;
            border-radius: 999px;
            border: 1px solid rgba(88,166,255,0.25);
        }
        .badge.ok { color: var(--success); border-color: rgba(63,185,80,0.35); }
        .badge.warn { color: var(--warning); border-color: rgba(242,204,96,0.35); }
        .badge.danger { color: var(--danger); border-color: rgba(255,123,114,0.45); }
        .test-indicator {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 6px 12px;
            border-radius: 999px;
            background: rgba(13,17,23,0.72);
            border: 1px solid rgba(88,166,255,0.2);
            font-size: 0.78rem;
        }
        .test-indicator.ok { border-color: rgba(63,185,80,0.45); color: var(--success); }
        .test-indicator.waiting { color: var(--warning); border-color: rgba(242,204,96,0.45); }
        .preflight-result {
            font-size: 0.82rem;
            color: var(--muted);
        }
        .preflight-result.ok { color: var(--success); }
        .preflight-result.fail { color: var(--danger); }
        .control-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
            margin-top: 16px;
        }
        .control-card {
            border-radius: 14px;
            padding: 14px;
            background: rgba(13,17,23,0.72);
            border: 1px solid rgba(88,166,255,0.12);
        }
        .control-card strong { display: block; margin-bottom: 6px; }
        .stack {
            display: flex;
            flex-direction: column;
            gap: 6px;
        }
        .decision-tape {
            margin-top: 12px;
            display: grid;
            gap: 10px;
        }
        .decision-card {
            padding: 12px;
            border-radius: 12px;
            background: rgba(13,17,23,0.72);
            border: 1px solid rgba(88,166,255,0.1);
        }
        .decision-card.duplicate { border-color: rgba(255,123,114,0.4); }
        @media (max-width: 720px) {
            .layout { padding: 18px 14px 40px; }
            header { padding: 18px; }
            .panel { padding: 16px; }
        }
        .highlight {
            outline: 2px solid var(--accent);
            transition: outline 0.4s ease;
        }
    </style>
</head>
<body>
    <div class=\"layout\">
        <header>
            <div id=\"connection-banner\" class=\"connection-banner connection-banner--connecting\">
                <strong id=\"connection-status-label\">Connecting to runtime…</strong>
                <div id=\"connection-status-detail\" class=\"connection-detail\">Fetching runtime metadata…</div>
            </div>
            <div class=\"header-top\">
                <div class=\"section-title\">
                    <h1>SolHunter Swarm Lifecycle</h1>
                    <span class=\"pill {{ 'fresh' if not swarm_overall.get('stale') else 'stale' }}\">
                        Updated {{ swarm_overall.get('age_label', 'n/a') }}
                    </span>
                </div>
                <div class=\"header-actions\">
                    <div id=\"test-indicator\" class=\"test-indicator waiting\">
                        <span>Test Mode</span>
                        <strong id=\"test-status-label\">Waiting</strong>
                    </div>
                    <button id=\"test-mode-toggle\" type=\"button\">Toggle Test Mode</button>
                    <button id=\"preflight-button\" type=\"button\">Run Pre-flight</button>
                </div>
            </div>
            <div class=\"header-signals\">
                <span class=\"signal-pill env-{{ header_signals.environment|lower() }}\">🌎 Env {{ header_signals.environment }}</span>
                <span class=\"signal-pill {{ 'toggle-on' if header_signals.paper_mode else 'toggle-off' }}\">📝 Paper {{ 'ON' if header_signals.paper_mode else 'OFF' }}</span>
                <span class=\"signal-pill {{ 'toggle-paused' if header_signals.paused else 'toggle-on' }}\">⏸ Pause {{ 'ON' if header_signals.paused else 'OFF' }}</span>
                <span class=\"signal-pill {{ 'toggle-on' if header_signals.rl_mode == 'applied' else 'toggle-off' }}\">🤖 RL {{ header_signals.rl_mode|title }}</span>
            </div>
            <div class=\"signal-metrics\">
                <div class=\"signal-metric\">
                    <span class=\"muted\">Bus Latency</span>
                    <strong>{{ header_signals.bus_latency_ms | round(1) if header_signals.bus_latency_ms is not none else '—' }} ms</strong>
                </div>
                <div class=\"signal-metric\">
                    <span class=\"muted\">OHLCV Lag</span>
                    <strong>{{ header_signals.stream_lag.ohlcv | round(1) if header_signals.stream_lag.ohlcv is not none else '—' }} ms</strong>
                </div>
                <div class=\"signal-metric\">
                    <span class=\"muted\">Depth Lag</span>
                    <strong>{{ header_signals.stream_lag.depth | round(1) if header_signals.stream_lag.depth is not none else '—' }} ms</strong>
                </div>
                <div class=\"signal-metric\">
                    <span class=\"muted\">Golden Lag</span>
                    <strong>{{ header_signals.stream_lag.golden | round(1) if header_signals.stream_lag.golden is not none else '—' }} ms</strong>
                </div>
            </div>
            <div class=\"metrics\">
                <div class=\"metric\">
                    <span class=\"muted\">Suggestions / 5m</span>
                    <strong>{{ kpis.suggestions_per_5m | round(2) }}</strong>
                </div>
                <div class=\"metric\">
                    <span class=\"muted\">Acceptance</span>
                    <strong>{{ (kpis.acceptance_rate * 100) | round(1) }}%</strong>
                </div>
                <div class=\"metric\">
                    <span class=\"muted\">Golden Hashes</span>
                    <strong>{{ kpis.golden_hashes }}</strong>
                </div>
                <div class=\"metric\">
                    <span class=\"muted\">Open Vote Windows</span>
                    <strong>{{ kpis.open_windows }}</strong>
                </div>
                <div class=\"metric\">
                    <span class=\"muted\">Paper PnL (1D)</span>
                    <strong>{{ kpis.paper_pnl | round(2) if kpis.paper_pnl is not none else '—' }}</strong>
                </div>
                <div class=\"metric\">
                    <span class=\"muted\">Drawdown</span>
                    <strong>{{ (kpis.drawdown * 100) | round(2) if kpis.drawdown is not none else '—' }}%</strong>
                </div>
                <div class=\"metric\">
                    <span class=\"muted\">Turnover</span>
                    <strong>{{ kpis.turnover | round(2) if kpis.turnover is not none else '—' }}</strong>
                </div>
            </div>
            <div class=\"status-grid\">
                {% for card in status_cards %}
                <div class=\"status-card {{ card.state }}\">
                    <div>{{ card.label }}</div>
                    <div class=\"muted\">{{ card.caption }}</div>
                </div>
                {% endfor %}
            </div>
        </header>

        <section class=\"grid-two\">
            <article class=\"panel\" id=\"discovery-panel\">
                {% set discovery_stale = discovery_console.candidates | selectattr('stale') | list %}
                {% set discovery_state = 'idle' %}
                {% if discovery_console.candidates %}
                    {% if discovery_stale|length == discovery_console.candidates|length %}
                        {% set discovery_state = 'danger' %}
                    {% elif discovery_stale %}
                        {% set discovery_state = 'warn' %}
                    {% else %}
                        {% set discovery_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ discovery_state }}\"></span>Discovery Console</h2>
                    <span class=\"muted\">{{ discovery_console.stats.total }} candidates</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"discovery\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Score</th>
                            <th>Source</th>
                            <th>Observed</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for row in discovery_console.candidates %}
                        <tr class=\"{{ 'stale' if row.stale else '' }}\" data-mint=\"{{ row.mint }}\">
                            <td>{{ row.mint }}</td>
                            <td>{{ row.score if row.score is not none else '—' }}</td>
                            <td>{{ row.source or '—' }}</td>
                            <td>
                                {{ row.asof or '—' }}
                                <span class=\"pill {{ 'stale' if row.stale else 'fresh' }}\">{{ row.age_label }}</span>
                            </td>
                        </tr>
                        {% else %}
                        <tr><td colspan=\"4\" class=\"muted\">Waiting for discovery stream…</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </article>

            <article class=\"panel\" id=\"token-facts-panel\">
                {% set token_stale = token_facts.tokens.values() | selectattr('stale') | list %}
                {% set token_state = 'idle' %}
                {% if token_facts.tokens %}
                    {% if token_stale|length == token_facts.tokens|length %}
                        {% set token_state = 'danger' %}
                    {% elif token_stale %}
                        {% set token_state = 'warn' %}
                    {% else %}
                        {% set token_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ token_state }}\"></span>Token Facts Drawer</h2>
                    <span class=\"muted\">{{ token_facts.tokens | length }} loaded</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"token-facts\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Symbol</th>
                            <th>Venues</th>
                            <th>As Of</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for mint, info in token_facts.tokens.items() %}
                        <tr class=\"{{ 'stale' if info.stale else '' }}\" data-mint=\"{{ mint }}\">
                            <td>{{ mint }}</td>
                            <td>{{ info.symbol or '—' }}</td>
                            <td>{{ info.venues | join(', ') if info.venues else '—' }}</td>
                            <td>{{ info.asof or '—' }} <span class=\"pill {{ 'stale' if info.stale else 'fresh' }}\">{{ info.age_label }}</span></td>
                        </tr>
                        {% else %}
                        <tr><td colspan=\"4\" class=\"muted\">No token snapshots yet.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </article>
        </section>

        <section class=\"grid-two\">
            <article class=\"panel\" id=\"market-panel\">
                {% set market_stale = market_state.markets | selectattr('stale') | list %}
                {% set market_state_flag = 'idle' %}
                {% if market_state.markets %}
                    {% if market_stale|length == market_state.markets|length %}
                        {% set market_state_flag = 'danger' %}
                    {% elif market_stale %}
                        {% set market_state_flag = 'warn' %}
                    {% else %}
                        {% set market_state_flag = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ market_state_flag }}\"></span>Market · OHLCV & Depth</h2>
                    <span class=\"muted\">{{ market_state.markets | length }} tracked</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"market\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Close</th>
                            <th>Volume</th>
                            <th>Spread</th>
                            <th>Depth 1%</th>
                            <th>Lag (ms)</th>
                            <th>Updated</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for market in market_state.markets %}
                        <tr class=\"{{ 'stale' if market.stale else '' }}\" data-mint=\"{{ market.mint }}\">
                            <td>{{ market.mint }}</td>
                            <td>{{ market.close or '—' }}</td>
                            <td>{{ market.volume or '—' }}</td>
                            <td>{{ market.spread_bps or '—' }}</td>
                            <td>{{ market.depth_pct.get('1') if market.depth_pct else '—' }}</td>
                            <td>
                                <span class=\"pill {{ 'stale' if market.stale else 'fresh' }}\">OHLCV {{ market.lag_close_ms | round(0) if market.lag_close_ms is not none else '—' }}</span>
                                <span class=\"pill {{ 'stale' if market.stale else 'fresh' }}\">Depth {{ market.lag_depth_ms | round(0) if market.lag_depth_ms is not none else '—' }}</span>
                            </td>
                            <td>{{ market.updated_label }}</td>
                        </tr>
                        {% else %}
                        <tr class=\"skeleton-row\"><td colspan=\"7\" class=\"muted\">Waiting for market state…</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </article>

            <article class=\"panel\" id=\"golden-panel\">
                {% set golden_stale = golden_snapshots | selectattr('stale') | list %}
                {% set golden_state = 'idle' %}
                {% if golden_snapshots %}
                    {% if golden_stale|length == golden_snapshots|length %}
                        {% set golden_state = 'danger' %}
                    {% elif golden_stale %}
                        {% set golden_state = 'warn' %}
                    {% else %}
                        {% set golden_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ golden_state }}\"></span>Golden Snapshot Inspector</h2>
                    <span class=\"muted\">{{ golden_summary.count }} hashes</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"golden\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Hash</th>
                            <th>Price</th>
                            <th>Liquidity</th>
                            <th>Lag (ms)</th>
                            <th>Published</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for snap in golden_snapshots %}
                        <tr class=\"{{ 'stale' if snap.stale else '' }}\" data-mint=\"{{ snap.mint }}\">
                            <td>{{ snap.mint }}</td>
                            <td class=\"muted\">{{ snap.hash_short }}</td>
                            <td>{{ snap.px or '—' }}</td>
                            <td>{{ snap.liq or '—' }}</td>
                            <td>
                                <span class=\"pill {{ 'stale' if snap.stale else 'fresh' }}\">{{ snap.lag_ms | round(0) if snap.lag_ms is not none else '—' }}</span>
                            </td>
                            <td>{{ snap.age_label }}</td>
                        </tr>
                        {% else %}
                        <tr class=\"skeleton-row\"><td colspan=\"6\" class=\"muted\">Golden pipeline idle.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </article>
        </section>

        <section class=\"grid-two\">
            <article class=\"panel\" id=\"suggestions-panel\">
                {% set suggestion_stale = suggestions.suggestions | selectattr('stale') | list %}
                {% set mismatch = suggestions.suggestions | selectattr('hash_mismatch') | list %}
                {% set suggestions_state = 'idle' %}
                {% if suggestions.suggestions %}
                    {% if suggestion_stale|length == suggestions.suggestions|length %}
                        {% set suggestions_state = 'danger' %}
                    {% elif suggestion_stale %}
                        {% set suggestions_state = 'warn' %}
                    {% else %}
                        {% set suggestions_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ suggestions_state }}\"></span>Agent Suggestions</h2>
                    <span class=\"muted\">{{ suggestions.suggestions | length }} live</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"suggestions\">⏳</span>
                </div>
                <div class=\"filter-bar\">
                    <input type=\"search\" id=\"suggestion-filter-mint\" placeholder=\"Filter mint…\" aria-label=\"Filter by mint\" />
                    <select id=\"suggestion-filter-agent\" aria-label=\"Filter by agent\">
                        <option value=\"\">All agents</option>
                    </select>
                    <select id=\"suggestion-filter-side\" aria-label=\"Filter by side\">
                        <option value=\"\">Any side</option>
                        <option value=\"buy\">Buy</option>
                        <option value=\"sell\">Sell</option>
                    </select>
                    <label><input type=\"checkbox\" id=\"suggestion-filter-mismatch\" /> Hash mismatch only</label>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Agent</th>
                            <th>Mint</th>
                            <th>Side</th>
                            <th>Notional</th>
                            <th>Edge</th>
                            <th>Breakeven</th>
                            <th>Edge Buffer</th>
                            <th>Gate</th>
                            <th>TTL</th>
                            <th>Age</th>
                            <th>Inputs Hash</th>
                            <th>Golden Hash</th>
                            <th>Integrity</th>
                            <th>Must</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for entry in suggestions.suggestions %}
                        <tr class=\"{{ 'stale' if entry.stale else '' }}\" data-agent=\"{{ entry.agent or '' }}\" data-side=\"{{ entry.side or '' }}\" data-mint=\"{{ entry.mint }}\" data-mismatch=\"{{ '1' if entry.hash_mismatch else '0' }}\">
                            <td>{{ entry.agent or '—' }}</td>
                            <td><a href=\"#market-panel\" class=\"mint-chip\" data-mint=\"{{ entry.mint }}\">{{ entry.mint }}</a></td>
                            <td>{{ entry.side or '—' }}</td>
                            <td>{{ entry.notional_usd | round(2) if entry.notional_usd is not none else '—' }}</td>
                            <td>{{ entry.edge | round(4) if entry.edge is not none else '—' }}</td>
                            <td>{{ entry.breakeven_bps | round(2) if entry.breakeven_bps is not none else '—' }} bps</td>
                            <td>{{ entry.edge_buffer_bps | round(2) if entry.edge_buffer_bps is not none else '—' }} bps</td>
                            <td>
                                {% if entry.edge_pass is none %}
                                    <span class=\"pill neutral\">Unknown</span>
                                {% elif entry.edge_pass %}
                                    <span class=\"pill pass\">Pass</span>
                                {% else %}
                                    <span class=\"pill blocked\">Blocked</span>
                                {% endif %}
                            </td>
                            <td>{{ entry.ttl_label }}</td>
                            <td>{{ entry.age_label }}</td>
                            <td class=\"muted\">{{ entry.inputs_hash_short or '—' }}</td>
                            <td class=\"muted\">{{ entry.golden_hash_short or '—' }}</td>
                            <td>
                                {% if entry.hash_mismatch %}
                                    <span class=\"pill stale\">Mismatch</span>
                                {% else %}
                                    <span class=\"pill fresh\">Aligned</span>
                                {% endif %}
                            </td>
                            <td>{{ 'yes' if entry.must else 'no' }}</td>
                        </tr>
                        {% else %}
                        <tr class=\"skeleton-row\"><td colspan=\"14\" class=\"muted\">No active suggestions.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>

                <div class=\"exit-hot-watch\">
                    <h3>Hot Watch</h3>
                    {% if exit_panel.hot_watch %}
                    <table class=\"compact\">
                        <thead>
                            <tr>
                                <th>Mint</th>
                                <th>Reason</th>
                                <th>Breakeven</th>
                                <th>Trail</th>
                                <th>Time Left</th>
                                <th>Progress</th>
                                <th>Remaining</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for watch in exit_panel.hot_watch %}
                            <tr>
                                <td>{{ watch.token }}</td>
                                <td>{{ watch.reason }}</td>
                                <td>{{ watch.breakeven_bps | round(2) if watch.breakeven_bps is not none else '—' }} bps</td>
                                <td>{{ watch.trail_status }}</td>
                                <td>{{ watch.window_remaining | round(1) if watch.window_remaining is not none else '—' }}s</td>
                                <td>{{ (watch.progress * 100) | round(1) if watch.progress is not none else '—' }}%</td>
                                <td>{{ watch.remaining | round(4) if watch.remaining is not none else '—' }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                    {% else %}
                    <div class=\"muted\">No tokens flagged for exit.</div>
                    {% endif %}
                    <h3>Exit Queue</h3>
                    {% if exit_panel.queue %}
                    <table class=\"compact\">
                        <thead>
                            <tr>
                                <th>Mint</th>
                                <th>Reason</th>
                                <th>Must</th>
                                <th>Notional</th>
                                <th>Progress</th>
                                <th>Slices</th>
                                <th>Post-fill Qty</th>
                                <th>Age (s)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for item in exit_panel.queue %}
                            <tr>
                                <td>{{ item.token }}</td>
                                <td>{{ item.reason }}</td>
                                <td>{{ 'yes' if item.must else 'no' }}</td>
                                <td>{{ item.notional_usd | round(2) if item.notional_usd is not none else '—' }}</td>
                                <td>
                                    {% if item.progress is not none %}
                                        {{ (item.progress * 100) | round(1) }}%
                                        <span class=\"muted\">({{ item.filled_qty | round(4) }}/{{ item.initial_qty | round(4) }})</span>
                                    {% else %}
                                        —
                                    {% endif %}
                                </td>
                                <td>
                                    {% if item.slice_reasons %}
                                        {{ item.slice_reasons | join(', ') }}
                                    {% else %}
                                        —
                                    {% endif %}
                                </td>
                                <td>{{ item.post_fill_qty_preview | round(4) if item.post_fill_qty_preview is not none else '—' }}</td>
                                <td>{{ item.age_sec | round(1) if item.age_sec is not none else '—' }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                    {% else %}
                    <div class=\"muted\">Exit queue empty.</div>
                    {% endif %}
                    <h3>Exit Diagnostics</h3>
                    {% if exit_panel.diagnostics %}
                    <div class=\"exit-diagnostics-grid\">
                        {% for diag in exit_panel.diagnostics[:5] %}
                        <div class=\"exit-diagnostic-card\">
                            <header>
                                <strong>{{ diag.token }}</strong>
                                <span class=\"muted\">{% if diag.diagnostics.reason is defined and diag.diagnostics.reason %}{{ diag.diagnostics.reason }}{% elif diag.diagnostics is mapping and diag.diagnostics.get('reason') %}{{ diag.diagnostics.get('reason') }}{% else %}—{% endif %}</span>
                            </header>
                            <div class=\"micro-chart\">
                                {% set samples = diag.monitor %}
                                {% if samples and samples|length > 1 %}
                                {% set recent = samples[-12:] %}
                                {% set entry_line = 0.0 %}
                                {% set trail_line = diag.diagnostics.trail_line_bps if diag.diagnostics is mapping and diag.diagnostics.get('trail_line_bps') is not none else None %}
                                {% set base_vals = [] %}
                                {% for sample in recent %}
                                    {% set base_vals = base_vals + [sample.delta_mid_bps or 0.0] %}
                                {% endfor %}
                                {% set all_vals = base_vals + [entry_line] %}
                                {% if trail_line is not none %}
                                    {% set all_vals = all_vals + [trail_line] %}
                                {% endif %}
                                {% if all_vals %}
                                {% set ns = namespace(init=False, min=0.0, max=0.0) %}
                                {% for val in all_vals %}
                                    {% if not ns.init %}
                                        {% set ns.min = val %}
                                        {% set ns.max = val %}
                                        {% set ns.init = True %}
                                    {% else %}
                                        {% if val < ns.min %}{% set ns.min = val %}{% endif %}
                                        {% if val > ns.max %}{% set ns.max = val %}{% endif %}
                                    {% endif %}
                                {% endfor %}
                                {% if ns.max == ns.min %}
                                    {% set ns.max = ns.min + 1 %}
                                {% endif %}
                                {% set denom = (ns.max - ns.min) if ns.max != ns.min else 1.0 %}
                                {% set ns_points = namespace(points=[]) %}
                                {% for sample in recent %}
                                    {% set val = sample.delta_mid_bps or 0.0 %}
                                    {% set normalized = (val - ns.min) / denom %}
                                    {% set x = (loop.index0 / (recent|length - 1)) * 180 %}
                                    {% set y = 50 - (normalized * 40) %}
                                    {% set ns_points.points = ns_points.points + [x|string + ',' + y|string] %}
                                {% endfor %}
                                <svg viewBox=\"0 0 180 60\" class=\"sparkline\" preserveAspectRatio=\"none\">
                                    <polyline points=\"{{ ns_points.points|join(' ') }}\" fill=\"none\" stroke=\"#4f8cff\" stroke-width=\"2\" />
                                    {% set entry_y = 50 - (((entry_line - ns.min) / denom) * 40) %}
                                    <line x1=\"0\" y1=\"{{ entry_y }}\" x2=\"180\" y2=\"{{ entry_y }}\" stroke=\"#999\" stroke-dasharray=\"4 4\" />
                                    {% if trail_line is not none %}
                                    {% set trail_y = 50 - (((trail_line - ns.min) / denom) * 40) %}
                                    <line x1=\"0\" y1=\"{{ trail_y }}\" x2=\"180\" y2=\"{{ trail_y }}\" stroke=\"#ff6b6b\" stroke-dasharray=\"2 2\" />
                                    {% endif %}
                                </svg>
                                {% else %}
                                <div class=\"muted\">No micro-samples yet.</div>
                                {% endif %}
                                {% else %}
                                <div class=\"muted\">No micro-samples yet.</div>
                                {% endif %}
                                <button class=\"flatten-btn\" data-mint=\"{{ diag.token }}\">Flatten</button>
                            </div>
                        </div>
                        {% endfor %}
                    </div>
                    {% else %}
                    <div class=\"muted\">No exit diagnostics yet.</div>
                    {% endif %}
                    <h3>Missed Exits</h3>
                    {% if exit_panel.missed_exits %}
                    <ul class=\"missed-exits\">
                        {% for miss in exit_panel.missed_exits[:10] %}
                        <li><strong>{{ miss.token }}</strong> · {{ miss.reason }} · <span class=\"muted\">{{ miss.ts }}</span></li>
                        {% endfor %}
                    </ul>
                    {% else %}
                    <div class=\"muted\">No missed exits recorded.</div>
                    {% endif %}
                    <h3>Recently Closed</h3>
                    {% if exit_panel.closed %}
                    <ul>
                        {% for diag in exit_panel.closed[:5] %}
                        {% set total_pnl = diag.slices | sum(attribute='pnl') %}
                        <li><strong>{{ diag.token }}</strong> · {{ total_pnl | round(4) }} · {% if diag.diagnostics.reason is defined and diag.diagnostics.reason %}{{ diag.diagnostics.reason }}{% elif diag.diagnostics is mapping and diag.diagnostics.get('reason') %}{{ diag.diagnostics.get('reason') }}{% else %}—{% endif %}</li>
                        {% endfor %}
                    </ul>
                    {% else %}
                    <div class=\"muted\">No closed exits recorded.</div>
                    {% endif %}
                </div>
            </article>

            <article class=\"panel\" id=\"vote-panel\">
                {% set vote_state = 'idle' %}
                {% if vote_windows.windows %}
                    {% if vote_windows.windows | selectattr('expired') | list %}
                        {% set vote_state = 'warn' %}
                    {% else %}
                        {% set vote_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ vote_state }}\"></span>Vote Window Visualiser</h2>
                    <span class=\"muted\">{{ vote_windows.windows | length }} open</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"vote\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Side</th>
                            <th>Quorum</th>
                            <th>Score</th>
                            <th>Countdown</th>
                            <th>Idempotency</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for window in vote_windows.windows %}
                        <tr class=\"{{ 'stale' if window.expired else '' }}\">
                            <td>{{ window.mint }}</td>
                            <td>{{ window.side }}</td>
                            <td>{{ window.quorum }}</td>
                            <td>{{ window.score | round(3) if window.score is not none else '—' }}</td>
                            <td>{{ window.countdown_label }}</td>
                            <td><span class=\"pill {{ 'fresh' if window.idempotent else 'stale' }}\">{{ window.idempotency_label }}</span></td>
                        </tr>
                        {% else %}
                        <tr class=\"skeleton-row\"><td colspan=\"6\" class=\"muted\">No open vote windows.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
                <div class=\"decision-tape\">
                    {% for decision in vote_windows.decisions %}
                    <div class=\"decision-card {{ 'duplicate' if decision.duplicate else '' }}\">
                        <span class=\"badge {{ 'danger' if not decision.idempotent else 'ok' }}\">{{ decision.idempotency_label }}</span>
                        <strong>{{ decision.mint }} · {{ decision.side }}</strong>
                        <div class=\"muted\">clientOrderId {{ decision.client_order_id }}</div>
                        <div>Score {{ decision.score | round(3) if decision.score is not none else '—' }} · Notional {{ decision.notional_usd or '—' }}</div>
                        <div class=\"muted\">{{ decision.age_label }} · First seen {{ decision.first_seen_label or 'n/a' }}</div>
                    </div>
                    {% else %}
                    <div class=\"muted\">No decisions in tape.</div>
                    {% endfor %}
                </div>
            </article>
        </section>

        <section class=\"grid-two\">
            <article class=\"panel\" id=\"shadow-panel\">
                {% set shadow_state = 'idle' %}
                {% if shadow.virtual_fills %}
                    {% if shadow.virtual_fills | selectattr('stale') | list %}
                        {% set shadow_state = 'warn' %}
                    {% else %}
                        {% set shadow_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ shadow_state }}\"></span>Shadow Execution</h2>
                    <span class=\"muted\">{{ shadow.virtual_fills | length }} fills</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"shadow\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Side</th>
                            <th>Qty</th>
                            <th>Price</th>
                            <th>Slippage</th>
                            <th>Snapshot</th>
                            <th>Time</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for fill in shadow.virtual_fills %}
                        <tr class=\"{{ 'stale' if fill.stale else '' }}\">
                            <td>{{ fill.mint }}</td>
                            <td>{{ fill.side }}</td>
                            <td>{{ fill.qty_base or '—' }}</td>
                            <td>{{ fill.price_usd or '—' }}</td>
                            <td>{{ fill.slippage_bps or '—' }}</td>
                            <td><span class=\"muted\">{{ fill.snapshot_hash_short }}</span> {% if fill.hash_mismatch %}<span class=\"pill stale\">hash</span>{% endif %}</td>
                            <td>{{ fill.age_label }}</td>
                        </tr>
                        {% else %}
                        <tr><td colspan=\"7\" class=\"muted\">Waiting for virtual fills…</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
                <h3>Paper Positions</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Side</th>
                            <th>Qty</th>
                            <th>Avg Cost</th>
                            <th>Unrealized</th>
                            <th>Total PnL</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for pos in shadow.paper_positions %}
                        <tr>
                            <td>{{ pos.mint }}</td>
                            <td>{{ pos.side }}</td>
                            <td>{{ pos.qty_base }}</td>
                            <td>{{ pos.avg_cost }}</td>
                            <td>{{ pos.unrealized_usd }}</td>
                            <td>{{ pos.total_pnl_usd }}</td>
                        </tr>
                        {% else %}
                        <tr><td colspan=\"6\" class=\"muted\">No paper positions.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
            </article>

            <article class=\"panel\" id=\"rl-panel\">
                {% set rl_state = 'idle' %}
                {% if rl_panel.weights %}
                    {% if rl_panel.weights | selectattr('stale') | list %}
                        {% set rl_state = 'warn' %}
                    {% else %}
                        {% set rl_state = 'ok' %}
                    {% endif %}
                {% endif %}
                <div class=\"section-title\">
                    <h2><span class=\"color-chip {{ rl_state }}\"></span>RL Weights & Uplift</h2>
                    <span class=\"muted\">{{ rl_summary.weights_applied }} windows</span>
                    <span class=\"preflight-result\" data-preflight-panel=\"rl\">⏳</span>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Mint</th>
                            <th>Window Hash</th>
                            <th>Multiplier</th>
                            <th>Age</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for entry in rl_panel.weights %}
                        <tr class=\"{{ 'stale' if entry.stale else '' }}\">
                            <td>{{ entry.mint }}</td>
                            <td class=\"muted\">{{ entry.window_hash_short }}</td>
                            <td>{{ entry.multiplier }}</td>
                            <td>{{ entry.age_label }}</td>
                        </tr>
                        {% else %}
                        <tr class=\"skeleton-row\"><td colspan=\"4\" class=\"muted\">RL stream idle.</td></tr>
                        {% endfor %}
                    </tbody>
                </table>
                <div class=\"metrics\">
                    <div class=\"metric\">
                        <span class=\"muted\">Paper Uplift (5m)</span>
                        <strong>{{ rl_panel.uplift.get('rolling_5m', 0) | round(4) }}</strong>
                    </div>
                    <div class=\"metric\">
                        <span class=\"muted\">Last Decision Delta</span>
                        <strong>{{ rl_panel.uplift.get('last_decision_delta', 0) | round(4) }}</strong>
                    </div>
                    <div class=\"metric\">
                        <span class=\"muted\">Score Plain</span>
                        <strong>{{ rl_panel.uplift.get('score_plain', 0) | round(4) }}</strong>
                    </div>
                    <div class=\"metric\">
                        <span class=\"muted\">Score RL</span>
                        <strong>{{ rl_panel.uplift.get('score_rl', 0) | round(4) }}</strong>
                    </div>
                    <div class=\"metric\">
                        <span class=\"muted\">Uplift % (5m)</span>
                        <strong>{{ rl_panel.uplift.get('uplift_pct', 0) | round(2) }}%</strong>
                    </div>
                </div>
            </article>
        </section>

        <section class=\"panel\" id=\"settings-panel\">
            <div class=\"section-title\">
                <h2><span class=\"color-chip idle\"></span>Settings & Controls</h2>
                <span class=\"muted\">{{ settings.controls | length }} controls</span>
            </div>
            <div class=\"control-grid\">
                {% for control in settings.controls %}
                <div class=\"control-card\">
                    <strong>{{ control.label }}</strong>
                    <div class=\"muted\">Endpoint {{ control.endpoint }}</div>
                    <div>Status: {{ control.state }}</div>
                    <div>TTL: {{ control.ttl_label }}</div>
                </div>
                {% else %}
                <div class=\"muted\">No controls available.</div>
                {% endfor %}
            </div>
        </section>

        <script>
            (function () {
                const connectionBanner = document.getElementById('connection-banner');
                const connectionLabel = document.getElementById('connection-status-label');
                const connectionDetail = document.getElementById('connection-status-detail');
                const channelOrder = ['events', 'rl', 'logs'];
                const channelLabels = {events: 'Events', rl: 'RL', logs: 'Logs'};
                const channelState = {
                    events: {status: 'idle', detail: 'Waiting for metadata…', socket: null, retryTimer: null, retries: 0, url: ''},
                    rl: {status: 'idle', detail: 'Waiting for metadata…', socket: null, retryTimer: null, retries: 0, url: ''},
                    logs: {status: 'idle', detail: 'Waiting for metadata…', socket: null, retryTimer: null, retries: 0, url: ''}
                };

                function renderConnectionSummary() {
                    if (!connectionBanner || !connectionLabel || !connectionDetail) {
                        return;
                    }
                    let anyError = false;
                    let anyWarn = false;
                    let allOpen = true;
                    const parts = [];
                    channelOrder.forEach((name) => {
                        const info = channelState[name];
                        if (!info) {
                            return;
                        }
                        const label = channelLabels[name] || name;
                        parts.push(label + ': ' + info.detail);
                        if (info.status === 'error') {
                            anyError = true;
                        }
                        if (info.status === 'warn') {
                            anyWarn = true;
                        }
                        if (info.status !== 'open') {
                            allOpen = false;
                        }
                    });
                    let overall = 'connecting';
                    if (anyError) {
                        overall = 'error';
                    } else if (anyWarn) {
                        overall = 'warning';
                    } else if (allOpen && parts.length) {
                        overall = 'ok';
                    }
                    const messages = {
                        ok: 'Connected to runtime',
                        error: 'Realtime stream disconnected',
                        warning: 'Realtime stream degraded',
                        connecting: 'Connecting to runtime…'
                    };
                    connectionLabel.textContent = messages[overall] || messages.connecting;
                    connectionBanner.className = 'connection-banner connection-banner--' + overall;
                    connectionDetail.textContent = parts.join(' · ');
                }

                function setChannelState(name, status, detail) {
                    const info = channelState[name];
                    if (!info) {
                        return;
                    }
                    info.status = status;
                    if (typeof detail === 'string' && detail) {
                        info.detail = detail;
                    }
                    if (status === 'open') {
                        info.retries = 0;
                    }
                    renderConnectionSummary();
                }

                function shortUrl(url) {
                    if (typeof url !== 'string') {
                        return '';
                    }
                    if (url.startsWith('wss://')) {
                        return url.slice(6);
                    }
                    if (url.startsWith('ws://')) {
                        return url.slice(5);
                    }
                    return url;
                }

                function scheduleReconnect(name, url, reasonText) {
                    const info = channelState[name];
                    if (!info) {
                        return;
                    }
                    if (info.retryTimer) {
                        return;
                    }
                    const attempt = info.retries || 0;
                    const delay = Math.min(10000, 1000 * Math.pow(2, attempt));
                    info.retryTimer = window.setTimeout(() => {
                        info.retryTimer = null;
                        setChannelState(name, 'connecting', 'Connecting…');
                        connectChannel(name, url);
                    }, delay);
                    info.retries = attempt + 1;
                    const retryLabel = 'retrying in ' + Math.round(delay / 1000) + 's';
                    const prefix = reasonText ? reasonText + ' — ' : '';
                    const suffix = url ? ' (' + shortUrl(url) + ')' : '';
                    setChannelState(name, 'error', prefix + retryLabel + suffix);
                }

                function connectChannel(name, url) {
                    const info = channelState[name];
                    if (!info) {
                        return;
                    }
                    if (info.retryTimer) {
                        window.clearTimeout(info.retryTimer);
                        info.retryTimer = null;
                    }
                    if (info.socket) {
                        try {
                            info.socket.close(1000, 'reconnecting');
                        } catch (err) {
                            // ignore
                        }
                        info.socket = null;
                    }
                    if (!window.WebSocket) {
                        setChannelState(name, 'error', 'WebSocket unsupported');
                        return;
                    }
                    if (!url) {
                        setChannelState(name, 'warn', 'No endpoint provided');
                        return;
                    }
                    info.url = url;
                    setChannelState(name, 'connecting', 'Connecting…');
                    let socket;
                    try {
                        socket = new WebSocket(url);
                    } catch (err) {
                        const message = err && err.message ? err.message : 'failed to open socket';
                        setChannelState(name, 'error', 'Failed to open — ' + message);
                        scheduleReconnect(name, url, 'connect error');
                        return;
                    }
                    info.socket = socket;
                    socket.addEventListener('open', () => {
                        setChannelState(name, 'open', 'Connected');
                    });
                    socket.addEventListener('message', (event) => {
                        if (!event || typeof event.data === 'undefined') {
                            return;
                        }
                        const data = event.data;
                        if (typeof data === 'string') {
                            const trimmed = data.trim();
                            if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
                                try {
                                    JSON.parse(trimmed);
                                } catch (err) {
                                    console.error('ui-msg-error', err, {channel: name, payload: data});
                                    setChannelState(name, 'warn', 'Bad frame skipped');
                                    return;
                                }
                            }
                        }
                        if (info.status !== 'open' || info.detail === 'Connected') {
                            setChannelState(name, 'open', 'Streaming');
                        }
                    });
                    socket.addEventListener('close', (event) => {
                        const code = event && typeof event.code === 'number' ? event.code : 1000;
                        const reason = event && event.reason ? event.reason : '';
                        info.socket = null;
                        const reasonText = 'closed (' + code + (reason ? ' ' + reason : '') + ')';
                        scheduleReconnect(name, url, reasonText);
                    });
                    socket.addEventListener('error', (event) => {
                        const detail = event && event.message ? event.message : 'socket error';
                        const suffix = info.url ? ' (' + shortUrl(info.url) + ')' : '';
                        setChannelState(name, 'error', 'Error — ' + detail + suffix);
                    });
                }

                async function bootRealtime() {
                    renderConnectionSummary();
                    let meta;
                    try {
                        const response = await fetch('/ui/meta', {cache: 'no-store'});
                        if (!response.ok) {
                            throw new Error('HTTP ' + response.status);
                        }
                        meta = await response.json();
                    } catch (err) {
                        const message = err && err.message ? err.message : 'failed to load meta';
                        channelOrder.forEach((name) => {
                            setChannelState(name, 'error', 'Meta fetch failed — ' + message);
                        });
                        return;
                    }
                    const mapping = {
                        events: meta && typeof meta.events_ws === 'string' ? meta.events_ws : '',
                        rl: meta && typeof meta.rl_ws === 'string' ? meta.rl_ws : '',
                        logs: meta && typeof meta.logs_ws === 'string' ? meta.logs_ws : ''
                    };
                    channelOrder.forEach((name) => {
                        const url = mapping[name] || '';
                        if (url) {
                            connectChannel(name, url);
                        } else {
                            setChannelState(name, 'warn', 'No endpoint provided');
                        }
                    });
                }

                renderConnectionSummary();
                bootRealtime();

                const suggestionRows = Array.from(document.querySelectorAll('#suggestions-panel tbody tr'));
                const agentSelect = document.getElementById('suggestion-filter-agent');
                const sideSelect = document.getElementById('suggestion-filter-side');
                const mintInput = document.getElementById('suggestion-filter-mint');
                const mismatchOnly = document.getElementById('suggestion-filter-mismatch');
                const preflightButton = document.getElementById('preflight-button');
                const preflightBadges = Array.from(document.querySelectorAll('.preflight-result'));
                const testToggle = document.getElementById('test-mode-toggle');
                const testIndicator = document.getElementById('test-indicator');
                const testLabel = document.getElementById('test-status-label');
                let testTimer = null;
                let testActive = false;

                function populateAgentOptions() {
                    if (!agentSelect) {
                        return;
                    }
                    const agents = new Set();
                    suggestionRows.forEach((row) => {
                        const agent = row.dataset.agent;
                        if (agent) {
                            agents.add(agent);
                        }
                    });
                    agents.forEach((agent) => {
                        const option = document.createElement('option');
                        option.value = agent;
                        option.textContent = agent;
                        agentSelect.appendChild(option);
                    });
                }

                function applySuggestionFilters() {
                    suggestionRows.forEach((row) => {
                        if (row.classList.contains('skeleton-row')) {
                            return;
                        }
                        const agent = row.dataset.agent || '';
                        const side = row.dataset.side || '';
                        const mint = row.dataset.mint || '';
                        const mismatch = row.dataset.mismatch === '1';
                        let visible = true;
                        if (agentSelect && agentSelect.value && agent !== agentSelect.value) {
                            visible = false;
                        }
                        if (visible && sideSelect && sideSelect.value && side !== sideSelect.value) {
                            visible = false;
                        }
                        if (visible && mintInput && mintInput.value) {
                            const needle = mintInput.value.toLowerCase();
                            if (!mint.toLowerCase().includes(needle)) {
                                visible = false;
                            }
                        }
                        if (visible && mismatchOnly && mismatchOnly.checked && !mismatch) {
                            visible = false;
                        }
                        row.style.display = visible ? '' : 'none';
                    });
                }

                function highlightMint(mint) {
                    if (!mint) {
                        return;
                    }
                    const panels = ['#token-facts-panel', '#market-panel', '#golden-panel'];
                    panels.forEach((selector) => {
                        const row = document.querySelector(selector + ' tr[data-mint="' + mint + '"]');
                        if (row) {
                            row.classList.add('highlight');
                            row.scrollIntoView({behavior: 'smooth', block: 'center'});
                            window.setTimeout(() => row.classList.remove('highlight'), 1500);
                        }
                    });
                }

                function setPreflightStatus(panel, status, symbol) {
                    const badge = preflightBadges.find((el) => el.dataset.preflightPanel === panel);
                    if (!badge) {
                        return;
                    }
                    badge.textContent = symbol;
                    badge.classList.remove('ok', 'fail');
                    if (status === 'ok') {
                        badge.classList.add('ok');
                    } else if (status === 'fail') {
                        badge.classList.add('fail');
                    }
                }

                async function runPreflight() {
                    if (!preflightButton) {
                        return;
                    }
                    preflightButton.disabled = true;
                    preflightButton.textContent = 'Running…';
                    preflightBadges.forEach((badge) => {
                        badge.textContent = '⏳';
                        badge.classList.remove('ok', 'fail');
                    });
                    const steps = [
                        {panel: 'discovery', url: '/swarm/discovery'},
                        {panel: 'token-facts', url: '/tokens'},
                        {panel: 'market', url: '/swarm/market'},
                        {panel: 'golden', url: '/swarm/golden'},
                        {panel: 'suggestions', url: '/swarm/suggestions'},
                        {panel: 'vote', url: '/swarm/votes'},
                        {panel: 'shadow', url: '/swarm/shadow'},
                        {panel: 'rl', url: '/swarm/rl'}
                    ];
                    for (const step of steps) {
                        try {
                            const response = await fetch(step.url, {cache: 'no-store'});
                            if (response.ok) {
                                setPreflightStatus(step.panel, 'ok', '✅');
                            } else {
                                setPreflightStatus(step.panel, 'fail', '❌');
                            }
                        } catch (err) {
                            setPreflightStatus(step.panel, 'fail', '❌');
                        }
                    }
                    preflightButton.disabled = false;
                    preflightButton.textContent = 'Run Pre-flight';
                }

                const panelsToCheck = [
                    'discovery-panel',
                    'token-facts-panel',
                    'market-panel',
                    'golden-panel',
                    'suggestions-panel',
                    'vote-panel',
                    'shadow-panel',
                    'rl-panel'
                ];

                function panelHasData(panelId) {
                    const panel = document.getElementById(panelId);
                    if (!panel) {
                        return false;
                    }
                    const rows = panel.querySelectorAll('tbody tr');
                    for (const row of rows) {
                        if (row.classList.contains('skeleton-row')) {
                            continue;
                        }
                        if (row.style.display === 'none') {
                            continue;
                        }
                        if (row.dataset && row.dataset.mint) {
                            return true;
                        }
                        if (row.textContent && row.textContent.trim() && !row.classList.contains('muted')) {
                            return true;
                        }
                    }
                    return false;
                }

                function updateTestIndicator() {
                    if (!testIndicator || !testLabel) {
                        return;
                    }
                    const ready = panelsToCheck.every((panelId) => panelHasData(panelId));
                    if (ready) {
                        testIndicator.classList.remove('waiting');
                        testIndicator.classList.add('ok');
                        testLabel.textContent = 'UI OK';
                    } else {
                        testIndicator.classList.remove('ok');
                        testIndicator.classList.add('waiting');
                        testLabel.textContent = 'Waiting…';
                    }
                }

                function setTestMode(active) {
                    testActive = active;
                    if (!testIndicator || !testLabel) {
                        return;
                    }
                    if (testActive) {
                        testIndicator.classList.add('waiting');
                        testIndicator.classList.remove('ok');
                        testLabel.textContent = 'Checking…';
                        updateTestIndicator();
                        if (testTimer) {
                            window.clearInterval(testTimer);
                        }
                        testTimer = window.setInterval(updateTestIndicator, 2000);
                    } else {
                        if (testTimer) {
                            window.clearInterval(testTimer);
                            testTimer = null;
                        }
                        testIndicator.classList.remove('ok');
                        testIndicator.classList.add('waiting');
                        testLabel.textContent = 'Off';
                    }
                }

                populateAgentOptions();
                applySuggestionFilters();

                if (agentSelect) {
                    agentSelect.addEventListener('change', applySuggestionFilters);
                }
                if (sideSelect) {
                    sideSelect.addEventListener('change', applySuggestionFilters);
                }
                if (mintInput) {
                    mintInput.addEventListener('input', applySuggestionFilters);
                }
                if (mismatchOnly) {
                    mismatchOnly.addEventListener('change', applySuggestionFilters);
                }

                if (preflightButton) {
                    preflightButton.addEventListener('click', runPreflight);
                }
                if (testToggle) {
                    testToggle.addEventListener('click', function () {
                        setTestMode(!testActive);
                    });
                }

                document.addEventListener('click', function (event) {
                    const btn = event.target.closest('.flatten-btn');
                    if (btn) {
                        event.preventDefault();
                        const mint = btn.dataset.mint;
                        if (!mint) {
                            return;
                        }
                        fetch('/actions/flatten', {
                            method: 'POST',
                            headers: {'Content-Type': 'application/json'},
                            body: JSON.stringify({mint: mint, must: true, must_exit: true})
                        }).catch(() => {});
                        return;
                    }
                    const mintChip = event.target.closest('.mint-chip');
                    if (mintChip) {
                        event.preventDefault();
                        highlightMint(mintChip.dataset.mint || '');
                        return;
                    }
                    const discoveryRow = event.target.closest('#discovery-panel tbody tr[data-mint]');
                    if (discoveryRow) {
                        const mint = discoveryRow.dataset.mint;
                        if (mint) {
                            highlightMint(mint);
                        }
                    }
                });

                // Initialise indicator state
                setTestMode(false);
            })();
        </script>
    </div>
</body>
</html>


"""


def _json_ready(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_ready(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_ready(v) for v in obj]
    return obj


def create_app(state: UIState | None = None) -> Flask:
    """Return a configured Flask application bound to *state*."""

    if state is None:
        state = UIState()

    app = Flask(__name__)  # type: ignore[arg-type]

    @app.after_request
    def _no_store(response: Response) -> Response:
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Frame-Options", "DENY")
        content_type = response.content_type or ""
        if "application/json" in content_type.lower():
            response.headers["Cache-Control"] = "no-store"
        return response

    def _status_cards(status: Dict[str, Any]) -> List[Dict[str, Any]]:
        cards: List[Dict[str, Any]] = []
        cards.append(
            {
                "label": "Event Bus",
                "state": "ok" if status.get("event_bus") else "fail",
                "caption": "connected" if status.get("event_bus") else "offline",
            }
        )
        cards.append(
            {
                "label": "Trading Loop",
                "state": "ok" if status.get("trading_loop") else "fail",
                "caption": "running" if status.get("trading_loop") else "stopped",
            }
        )
        if status.get("depth_service") is not None:
            cards.append(
                {
                    "label": "Depth",
                    "state": "ok" if status.get("depth_service") else "warn",
                    "caption": "streaming" if status.get("depth_service") else "idle",
                }
            )
        if status.get("rl_daemon") is not None:
            cards.append(
                {
                    "label": "RL Daemon",
                    "state": "ok" if status.get("rl_daemon") else "warn",
                    "caption": "healthy" if status.get("rl_daemon") else "degraded",
                }
            )
        heartbeat = status.get("heartbeat") or status.get("heartbeat_ts")
        if heartbeat:
            cards.append(
                {
                    "label": "Heartbeat",
                    "state": "ok",
                    "caption": str(heartbeat),
                }
            )
        return cards

    @app.get("/")
    def index() -> Any:
        if request.args.get("format", "").lower() == "json":
            payload = {
                "message": "SolHunter Zero Swarm UI",
                "status": state.snapshot_status(),
                "summary": state.snapshot_summary(),
                "discovery": state.snapshot_discovery_console(),
                "token_facts": state.snapshot_token_facts(),
                "market": state.snapshot_market_state(),
                "golden": state.snapshot_golden_snapshots(),
                "suggestions": state.snapshot_suggestions(),
                "votes": state.snapshot_vote_windows(),
                "shadow": state.snapshot_shadow(),
                "rl": state.snapshot_rl_panel(),
                "settings": state.snapshot_settings(),
                "activity": state.snapshot_activity(),
                "logs": state.snapshot_logs(),
                "weights": state.snapshot_weights(),
                "config_overview": state.snapshot_config(),
                "history": state.snapshot_history(),
                "exits": state.snapshot_exit(),
            }
            return jsonify(_json_ready(payload))

        status = state.snapshot_status()
        status_cards = _status_cards(status)
        discovery_console = state.snapshot_discovery_console()
        token_facts = state.snapshot_token_facts()
        market_state = state.snapshot_market_state()
        golden_detail = state.snapshot_golden_snapshots()
        suggestions = state.snapshot_suggestions()
        exit_panel = state.snapshot_exit()
        vote_windows = state.snapshot_vote_windows()
        shadow = state.snapshot_shadow()
        rl_panel = state.snapshot_rl_panel()
        settings = state.snapshot_settings()
        summary = state.snapshot_summary()

        golden_snapshots = golden_detail.get("snapshots", [])
        golden_summary = {
            "count": len(golden_snapshots),
        }
        suggestion_metrics = suggestions.get("metrics", {})
        rl_summary = {
            "weights_applied": len(rl_panel.get("weights", [])),
        }
        swarm_overall = {
            "stale": suggestions.get("metrics", {}).get("stale", False),
            "age_label": suggestions.get("metrics", {}).get("updated_label", "n/a"),
        }
        stream_lag = {
            "ohlcv": (market_state.get("lag_ms") or {}).get("ohlcv_ms"),
            "depth": (market_state.get("lag_ms") or {}).get("depth_ms"),
            "golden": golden_detail.get("lag_ms"),
        }
        header_signals = {
            "environment": (status.get("environment") or "dev").upper(),
            "paper_mode": bool(status.get("paper_mode")),
            "paused": bool(status.get("paused")),
            "rl_mode": status.get("rl_mode", "shadow"),
            "bus_latency_ms": status.get("bus_latency_ms"),
            "stream_lag": stream_lag,
        }
        kpis = {
            "suggestions_per_5m": suggestions.get("metrics", {}).get("rate_per_min", 0)
            * 5.0,
            "acceptance_rate": suggestions.get("metrics", {}).get("acceptance_rate", 0),
            "golden_hashes": golden_summary.get("count", 0),
            "open_windows": len(vote_windows.get("windows", [])),
            "paper_pnl": summary.get("execution", {}).get("pnl_1d"),
            "drawdown": summary.get("execution", {}).get("drawdown"),
            "turnover": summary.get("execution", {}).get("turnover"),
        }

        return render_template_string(
            _PAGE_TEMPLATE,
            status_cards=status_cards,
            discovery_console=discovery_console,
            token_facts=token_facts,
            market_state=market_state,
            golden_snapshots=golden_snapshots,
            golden_summary=golden_summary,
            suggestions=suggestions,
            suggestion_metrics=suggestion_metrics,
            exit_panel=exit_panel,
            vote_windows=vote_windows,
            shadow=shadow,
            rl_panel=rl_panel,
            rl_summary=rl_summary,
            settings=settings,
            swarm_overall=swarm_overall,
            header_signals=header_signals,
            kpis=kpis,
        )

    def _ws_config_payload() -> Dict[str, str]:
        manifest = build_ui_manifest(request)
        return {
            "rl_ws": manifest["rl_ws"],
            "events_ws": manifest["events_ws"],
            "logs_ws": manifest["logs_ws"],
        }

    def _ui_meta_payload() -> Dict[str, Any]:
        manifest = build_ui_manifest(request)
        base_url = (request.url_root or "").rstrip("/")
        if not base_url:
            scheme = getattr(request, "scheme", "http") or "http"
            host = request.host or ""
            if not host:
                host = f"127.0.0.1:{manifest.get('ui_port', 5000)}"
            base_url = f"{scheme}://{host}"
        return {
            "url": base_url,
            "rl_ws": manifest["rl_ws"],
            "events_ws": manifest["events_ws"],
            "logs_ws": manifest["logs_ws"],
        }

    def _probe_ws(url: str | None, *, timeout: float = 1.5) -> tuple[str, Optional[str]]:
        if not url:
            return "fail", "missing endpoint"
        if websockets is None:
            return "fail", "websockets module unavailable"

        async def _check() -> tuple[str, Optional[str]]:
            try:
                async with websockets.connect(url, ping_timeout=timeout, close_timeout=timeout):
                    return "ok", None
            except Exception as exc:  # pragma: no cover - network probe failures
                log.debug("UI websocket probe failed for %s: %s", url, exc)
                return "fail", f"{type(exc).__name__}: {exc}"

        try:
            return asyncio.run(_check())
        except RuntimeError as exc:  # pragma: no cover - unexpected event loop state
            log.debug("UI websocket probe unavailable for %s: %s", url, exc)
            return "fail", f"RuntimeError: {exc}"
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("UI websocket probe crashed for %s: %s", url, exc)
            return "fail", f"{type(exc).__name__}: {exc}"

    @app.get("/api/manifest")
    def api_manifest() -> Any:
        return jsonify(build_ui_manifest(request))

    @app.get("/ui/meta")
    def ui_meta() -> Any:
        return jsonify(_ui_meta_payload())

    @app.get("/ui/ws-config")
    def ui_ws_config() -> Any:
        return jsonify(_ws_config_payload())

    @app.get("/ws-config")
    def ws_config() -> Any:
        return jsonify(_ws_config_payload())

    @app.get("/ui/health")
    def ui_health() -> Any:
        urls = get_ws_urls()
        rl_status, rl_detail = _probe_ws(urls.get("rl"))
        events_status, events_detail = _probe_ws(urls.get("events"))
        logs_status, logs_detail = _probe_ws(urls.get("logs"))
        payload: Dict[str, Any] = {
            "ui": "ok",
            "rl_ws": rl_status,
            "events_ws": events_status,
            "logs_ws": logs_status,
        }
        details = {
            key: detail
            for key, detail in {
                "rl_ws": rl_detail,
                "events_ws": events_detail,
                "logs_ws": logs_detail,
            }.items()
            if detail
        }
        if details:
            payload["details"] = details
        return jsonify(payload)

    @app.get("/health")
    def health() -> Any:
        status = state.snapshot_status()
        ok = bool(status.get("event_bus")) and bool(status.get("trading_loop"))
        return jsonify({"ok": ok, "status": status})

    @app.get("/status")
    def status_view() -> Any:
        return jsonify(state.snapshot_status())

    @app.get("/summary")
    def summary() -> Any:
        return jsonify(state.snapshot_summary())

    @app.get("/tokens")
    def tokens() -> Any:
        return jsonify(state.snapshot_token_facts())

    @app.get("/actions")
    def actions() -> Any:
        return jsonify({"actions": state.snapshot_actions()})

    @app.get("/activity")
    def activity() -> Any:
        return jsonify({"entries": state.snapshot_activity()})

    @app.get("/trades")
    def trades() -> Any:
        return jsonify(list(state.snapshot_trades()))

    @app.get("/weights")
    def weights() -> Any:
        return jsonify(state.snapshot_weights())

    @app.get("/rl/status")
    def rl_status() -> Any:
        return jsonify(state.snapshot_rl())

    @app.get("/config")
    def config() -> Any:
        return jsonify(state.snapshot_config())

    @app.get("/logs")
    def logs() -> Any:
        return jsonify({"entries": state.snapshot_logs()})

    @app.get("/discovery")
    def discovery_settings() -> Any:
        method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        if method is None:
            method = DEFAULT_DISCOVERY_METHOD
        return jsonify(
            {
                "method": method,
                "allowed_methods": sorted(DISCOVERY_METHODS),
            }
        )

    @app.post("/discovery")
    def update_discovery() -> Any:
        payload = request.get_json(silent=True) or {}
        raw_method = payload.get("method")
        if not isinstance(raw_method, str) or not raw_method.strip():
            return (
                jsonify(
                    {
                        "error": "method must be a non-empty string",
                        "allowed_methods": sorted(DISCOVERY_METHODS),
                    }
                ),
                400,
            )
        method = resolve_discovery_method(raw_method)
        if method is None:
            return (
                jsonify(
                    {
                        "error": f"Invalid discovery method: {raw_method}",
                        "allowed_methods": sorted(DISCOVERY_METHODS),
                    }
                ),
                400,
            )
        os.environ["DISCOVERY_METHOD"] = method
        return jsonify(
            {
                "status": "ok",
                "method": method,
                "allowed_methods": sorted(DISCOVERY_METHODS),
            }
        )

    @app.get("/swarm/discovery")
    def swarm_discovery() -> Any:
        return jsonify(_json_ready(state.snapshot_discovery_console()))

    @app.get("/swarm/market")
    def swarm_market() -> Any:
        return jsonify(_json_ready(state.snapshot_market_state()))

    @app.get("/swarm/golden")
    def swarm_golden() -> Any:
        return jsonify(_json_ready(state.snapshot_golden_snapshots()))

    @app.get("/swarm/suggestions")
    def swarm_suggestions() -> Any:
        return jsonify(_json_ready(state.snapshot_suggestions()))

    @app.get("/swarm/exits")
    def swarm_exits() -> Any:
        return jsonify(_json_ready(state.snapshot_exit()))

    @app.get("/swarm/votes")
    def swarm_votes() -> Any:
        return jsonify(_json_ready(state.snapshot_vote_windows()))

    @app.get("/swarm/shadow")
    def swarm_shadow() -> Any:
        return jsonify(_json_ready(state.snapshot_shadow()))

    @app.get("/swarm/rl")
    def swarm_rl() -> Any:
        return jsonify(_json_ready(state.snapshot_rl_panel()))

    @app.post("/actions/flatten")
    def action_flatten() -> Any:
        payload = request.get_json(silent=True) or {}
        mint = payload.get("mint")
        if not isinstance(mint, str) or not mint.strip():
            return jsonify({"ok": False, "error": "mint must be a non-empty string"}), 400

        must = bool(payload.get("must", False))
        must_exit = bool(payload.get("must_exit", False))
        action = {
            "type": "flatten",
            "mint": mint,
            "must": must,
            "must_exit": must_exit,
        }
        log.info(
            "UI flatten requested mint=%s must=%s must_exit=%s",
            mint,
            must,
            must_exit,
        )
        push_event({"ui_action": action})
        return jsonify({"ok": True, "action": action})

    @app.get("/__shutdown__")
    def _shutdown() -> Any:  # pragma: no cover - invoked via HTTP
        func = request.environ.get("werkzeug.server.shutdown")
        if func is None:
            raise RuntimeError("Not running with the Werkzeug Server")
        func()
        return {"ok": True}

    return app


class UIServer:
    """Utility wrapper that runs the Flask app in a background thread."""

    def __init__(
        self,
        state: UIState,
        *,
        host: str = "127.0.0.1",
        port: int = 5000,
    ) -> None:
        self.state = state
        self.host = host
        self.port = int(port)
        self.app = create_app(state)
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[BaseWSGIServer] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _serve() -> None:
            try:
                server = make_server(self.host, self.port, self.app)
                server.daemon_threads = True
                self._server = server
                server.serve_forever()
            except Exception:  # pragma: no cover - best effort logging
                log.exception("UI server crashed")
            finally:
                self._server = None

        self._thread = threading.Thread(target=_serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        server = self._server
        if server is not None:
            with contextlib.suppress(Exception):
                server.shutdown()
            with contextlib.suppress(Exception):
                server.server_close()
        if self._thread:
            self._thread.join(timeout=2)
        self._thread = None
        self._server = None
        stop_websockets()
