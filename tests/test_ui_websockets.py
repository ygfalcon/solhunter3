import asyncio
import errno
import builtins
import importlib
import logging
import os
import socket
import threading
import sys
import time
import types
from queue import Empty
from typing import Any

import pytest


def test_start_websockets_requires_dependency(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)

    real_import = builtins.__import__

    def _missing_websockets(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("websockets"):
            raise ImportError("websockets dependency not installed")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _missing_websockets)

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )
    production_stub = types.ModuleType("solhunter_zero.production")
    production_stub.load_production_env = lambda: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.production", production_stub)

    sys.modules.pop("solhunter_zero.ui", None)
    ui = importlib.import_module("solhunter_zero.ui")
    importlib.reload(ui)
    assert ui.websockets is None

    with pytest.raises(RuntimeError, match="UI websockets require"):
        ui.start_websockets()

@pytest.mark.timeout(30)
def test_websocket_threads_bind():
    # ``tests.stubs`` replaces ``websockets`` with a lightweight stub. Remove the
    # stub modules before checking for the real dependency so this test is
    # skipped when the package isn't installed.
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    pytest.importorskip("websockets")

    # Reload the real implementation so the websocket servers can be created.
    ws = importlib.import_module("websockets")
    assert ws.__file__ is not None

    # ``ui`` depends on a few optional packages; provide simple stubs when the
    # real packages aren't available so the module can be imported.
    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    from solhunter_zero import ui
    importlib.reload(ui)

    ui.start_websockets()
    try:
        ports_to_check = (ui._RL_WS_PORT, ui._EVENT_WS_PORT, ui._LOG_WS_PORT)
        assert all(port not in (None, 0) for port in ports_to_check)
        for port in ports_to_check:
            for _ in range(50):
                try:
                    with socket.create_connection(("localhost", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                pytest.fail(f"port {port} did not bind")
    finally:
        ui.stop_websockets()


@pytest.mark.timeout(30)
def test_websocket_port_in_use():
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    pytest.importorskip("websockets")

    importlib.import_module("websockets")  # ensure real implementation

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    from solhunter_zero import ui
    importlib.reload(ui)

    sock = socket.socket()
    sock.bind(("localhost", 8767))
    sock.listen(1)
    try:
        threads = ui.start_websockets()
        assert ui._RL_WS_PORT != 8767
        for _ in range(50):
            try:
                with socket.create_connection(("localhost", ui._RL_WS_PORT), timeout=0.1):
                    break
            except OSError:
                time.sleep(0.1)
        else:
            pytest.fail("fallback websocket port did not bind")
    finally:
        sock.close()
        for loop in (ui.rl_ws_loop, ui.event_ws_loop, ui.log_ws_loop):
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
        for t in threads.values() if 'threads' in locals() else []:
            t.join(timeout=1)


@pytest.mark.timeout(30)
def test_websocket_env_updates_after_rebind(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    pytest.importorskip("websockets")

    importlib.import_module("websockets")  # ensure real implementation

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    from solhunter_zero import ui
    importlib.reload(ui)

    stale_urls = {
        "UI_WS_URL": "ws://stale/events",
        "UI_EVENTS_WS_URL": "ws://stale/events",
        "UI_EVENTS_WS": "ws://stale/events",
        "UI_RL_WS_URL": "ws://stale/rl",
        "UI_RL_WS": "ws://stale/rl",
        "UI_LOG_WS_URL": "ws://stale/logs",
        "UI_LOGS_WS": "ws://stale/logs",
    }
    channel_hints = {
        "UI_WS_URL": "events",
        "UI_EVENTS_WS_URL": "events",
        "UI_EVENTS_WS": "events",
        "UI_RL_WS_URL": "rl",
        "UI_RL_WS": "rl",
        "UI_LOG_WS_URL": "logs",
        "UI_LOGS_WS": "logs",
        "UI_RL_WS_PORT": "rl",
        "UI_EVENT_WS_PORT": "events",
        "UI_LOG_WS_PORT": "logs",
    }
    for key, value in stale_urls.items():
        monkeypatch.setenv(key, value)
        ui._AUTO_WS_ENV_VALUES[key] = value
        ui._AUTO_WS_ENV_CHANNELS[key] = channel_hints[key]

    stale_ports = {
        "UI_RL_WS_PORT": "1234",
        "UI_EVENT_WS_PORT": "2345",
        "UI_LOG_WS_PORT": "3456",
    }
    for key, value in stale_ports.items():
        monkeypatch.setenv(key, value)
        ui._AUTO_WS_ENV_VALUES[key] = value
        ui._AUTO_WS_ENV_CHANNELS[key] = channel_hints[key]

    sock = socket.socket()
    sock.bind(("localhost", 8767))
    sock.listen(1)
    try:
        ui.start_websockets()
        assert ui._RL_WS_PORT != 8767

        host = ui._WS_CHANNELS["events"].host or ui._resolve_host()
        url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host

        expected_events = f"ws://{url_host}:{ui._EVENT_WS_PORT}{ui._channel_path('events')}"
        expected_rl = f"ws://{url_host}:{ui._RL_WS_PORT}{ui._channel_path('rl')}"
        expected_logs = f"ws://{url_host}:{ui._LOG_WS_PORT}{ui._channel_path('logs')}"

        assert os.environ["UI_WS_URL"] == expected_events
        assert os.environ["UI_EVENTS_WS_URL"] == expected_events
        assert os.environ["UI_EVENTS_WS"] == expected_events
        assert os.environ["UI_RL_WS_URL"] == expected_rl
        assert os.environ["UI_RL_WS"] == expected_rl
        assert os.environ["UI_LOG_WS_URL"] == expected_logs
        assert os.environ["UI_LOGS_WS"] == expected_logs

        assert os.environ["UI_RL_WS_PORT"] == str(ui._RL_WS_PORT)
        assert os.environ["UI_EVENT_WS_PORT"] == str(ui._EVENT_WS_PORT)
        assert os.environ["UI_LOG_WS_PORT"] == str(ui._LOG_WS_PORT)

        assert ui._AUTO_WS_ENV_VALUES["UI_RL_WS_PORT"] == str(ui._RL_WS_PORT)
        assert ui._AUTO_WS_ENV_VALUES["UI_EVENT_WS_PORT"] == str(ui._EVENT_WS_PORT)
        assert ui._AUTO_WS_ENV_VALUES["UI_LOG_WS_PORT"] == str(ui._LOG_WS_PORT)
    finally:
        sock.close()
        ui.stop_websockets()

    for key in list(stale_urls) + list(stale_ports):
        assert key not in os.environ
    assert not ui._AUTO_WS_ENV_VALUES
    assert not ui._AUTO_WS_ENV_CHANNELS


def test__start_channel_failure_clears_auto_env(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)

    failing_ws = types.ModuleType("websockets")

    def _serve(*args: Any, **kwargs: Any):
        raise OSError(errno.EADDRINUSE, "port in use")

    failing_ws.serve = _serve
    monkeypatch.setitem(sys.modules, "websockets", failing_ws)
    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    sys.modules.pop("solhunter_zero.ui", None)
    ui = importlib.import_module("solhunter_zero.ui")
    importlib.reload(ui)

    try:
        auto_values = {
            "UI_RL_WS_URL": "ws://previous/rl",
            "UI_RL_WS": "ws://previous/rl",
            "UI_RL_WS_PORT": "8765",
        }
        for key, value in auto_values.items():
            monkeypatch.setenv(key, value)
            ui._AUTO_WS_ENV_VALUES[key] = value
            ui._AUTO_WS_ENV_CHANNELS[key] = ui._WS_ENV_KEY_CHANNEL_HINTS.get(
                key, "rl"
            )

        with pytest.raises(RuntimeError, match="rl websocket failed to bind"):
            ui._start_channel(
                "rl",
                host="127.0.0.1",
                port=8765,
                queue_size=1,
                ping_interval=1.0,
                ping_timeout=1.0,
            )

        for key in auto_values:
            assert key not in os.environ
            assert key not in ui._AUTO_WS_ENV_VALUES
            assert key not in ui._AUTO_WS_ENV_CHANNELS
    finally:
        sys.modules.pop("solhunter_zero.ui", None)


def test_start_websockets_failure_clears_auto_env(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)

    stub_ws = types.ModuleType("websockets")

    def _unexpected_serve(*args: Any, **kwargs: Any):
        raise AssertionError("websockets.serve should not be called")

    stub_ws.serve = _unexpected_serve
    monkeypatch.setitem(sys.modules, "websockets", stub_ws)
    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    sys.modules.pop("solhunter_zero.ui", None)
    ui = importlib.import_module("solhunter_zero.ui")
    importlib.reload(ui)

    try:
        auto_values = {
            "UI_WS_URL": ("events", "ws://previous/events"),
            "UI_EVENTS_WS_URL": ("events", "ws://previous/events"),
            "UI_EVENTS_WS": ("events", "ws://previous/events"),
            "UI_EVENT_WS_PORT": ("events", "8100"),
            "UI_RL_WS_URL": ("rl", "ws://previous/rl"),
            "UI_RL_WS": ("rl", "ws://previous/rl"),
            "UI_RL_WS_PORT": ("rl", "8101"),
            "UI_LOG_WS_URL": ("logs", "ws://previous/logs"),
            "UI_LOGS_WS": ("logs", "ws://previous/logs"),
            "UI_LOG_WS_PORT": ("logs", "8102"),
        }
        for key, (channel, value) in auto_values.items():
            monkeypatch.setenv(key, value)
            ui._AUTO_WS_ENV_VALUES[key] = value
            ui._AUTO_WS_ENV_CHANNELS[key] = channel

        def fake_start_channel(channel: str, **kwargs: Any):
            if channel == "rl":
                return threading.Thread()
            raise RuntimeError("boom")

        monkeypatch.setattr(ui, "_start_channel", fake_start_channel)

        with pytest.raises(RuntimeError, match="boom"):
            ui.start_websockets()

        for key in auto_values:
            assert key not in os.environ
            assert key not in ui._AUTO_WS_ENV_VALUES
            assert key not in ui._AUTO_WS_ENV_CHANNELS
    finally:
        sys.modules.pop("solhunter_zero.ui", None)


def test_start_websockets_logs_resolved_ports(monkeypatch, caplog):
    ui = _reload_ui_module()

    monkeypatch.setenv("UI_RL_WS_PORT", "7123")
    monkeypatch.setenv("UI_EVENT_WS_PORT", "7124")
    monkeypatch.setenv("UI_LOG_WS_PORT", "7125")

    class DummyThread:
        def __init__(self) -> None:
            self._alive = True

        def is_alive(self) -> bool:
            return self._alive

        def join(self, timeout=None):  # pragma: no cover - no thread work to join
            return None

    captured_ports: dict[str, int] = {}

    def _fake_start_channel(channel: str, *, host: str, port: int, **kwargs: Any):
        captured_ports[channel] = port
        state = ui._WS_CHANNELS[channel]
        state.host = host
        state.port = port
        state.thread = DummyThread()
        state.ready.set()
        state.ready_status = "ok"
        return state.thread

    monkeypatch.setattr(ui, "_start_channel", _fake_start_channel)

    with caplog.at_level(logging.INFO):
        ui.start_websockets()

    try:
        assert captured_ports == {"rl": 7123, "events": 7124, "logs": 7125}
        assert any(
            "UI websockets resolved ports rl=7123 events=7124 logs=7125"
            in record.getMessage()
            for record in caplog.records
        )
        readiness_metadata = ui.get_ws_readiness_metadata()
        assert readiness_metadata.get("resolved_ports") == {
            "rl": 7123,
            "events": 7124,
            "logs": 7125,
        }
    finally:
        ui.stop_websockets()


@pytest.mark.timeout(30)
def test_websocket_env_preserves_preconfigured_url(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    pytest.importorskip("websockets")

    importlib.import_module("websockets")  # ensure real implementation

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    from solhunter_zero import ui
    importlib.reload(ui)

    custom_url = "wss://example.com/custom"
    monkeypatch.setenv("UI_WS_URL", custom_url)

    threads: dict[str, threading.Thread] = {}
    try:
        threads = ui.start_websockets()
        assert os.environ["UI_WS_URL"] == custom_url
    finally:
        for loop in (ui.rl_ws_loop, ui.event_ws_loop, ui.log_ws_loop):
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
        for t in threads.values():
            t.join(timeout=1)


@pytest.mark.timeout(30)
def test_websocket_parallel_start_calls():
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    pytest.importorskip("websockets")

    importlib.import_module("websockets")

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    from solhunter_zero import ui
    importlib.reload(ui)

    barrier = threading.Barrier(2)
    results: list[dict[str, threading.Thread]] = []
    errors: list[BaseException] = []

    def _start_worker() -> None:
        try:
            barrier.wait(timeout=5)
            results.append(ui.start_websockets())
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(exc)

    thread_a = threading.Thread(target=_start_worker)
    thread_b = threading.Thread(target=_start_worker)
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    try:
        assert not errors, f"start_websockets raised: {errors}"
        assert len(results) == 2
        assert set(results[0]) == {"rl", "events", "logs"}
        assert results[0] == results[1]
    finally:
        ui.stop_websockets()
        for state in ui._WS_CHANNELS.values():
            assert state.loop is None


@pytest.mark.timeout(30)
def test_websocket_parallel_start_stop_sequences():
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    pytest.importorskip("websockets")

    importlib.import_module("websockets")

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )

    from solhunter_zero import ui
    importlib.reload(ui)

    initial_threads = ui.start_websockets()

    barrier = threading.Barrier(2)
    start_results: list[dict[str, threading.Thread]] = []
    errors: list[tuple[str, BaseException]] = []

    def _start_worker() -> None:
        try:
            barrier.wait(timeout=5)
            start_results.append(ui.start_websockets())
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(("start", exc))

    def _stop_worker() -> None:
        try:
            barrier.wait(timeout=5)
            ui.stop_websockets()
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(("stop", exc))

    thread_a = threading.Thread(target=_start_worker)
    thread_b = threading.Thread(target=_stop_worker)
    thread_a.start()
    thread_b.start()
    thread_a.join()
    thread_b.join()

    try:
        assert not errors, f"parallel start/stop failed: {errors}"
        assert len(start_results) == 1
        assert set(start_results[0]) == {"rl", "events", "logs"}
    finally:
        ui.stop_websockets()
        for state in ui._WS_CHANNELS.values():
            assert state.loop is None

    for t in initial_threads.values():
        t.join(timeout=1)


def test_websocket_invalid_ping_values_use_defaults(monkeypatch, caplog):
    ui = _reload_ui_module()

    monkeypatch.setenv("UI_WS_PING_INTERVAL", "oops")
    monkeypatch.setenv("UI_WS_PING_TIMEOUT", "bad")

    captured: list[tuple[str, float, float]] = []
    fallback_ports = {"rl": 8765, "events": 8766, "logs": 8767}

    def _fake_start_channel(
        channel: str,
        *,
        host: str,
        port: int,
        queue_size: int,
        ping_interval: float,
        ping_timeout: float,
    ) -> Any:
        captured.append((channel, ping_interval, ping_timeout))
        assigned_port = port or fallback_ports[channel]
        state = ui._WS_CHANNELS[channel]
        state.loop = object()
        state.host = host
        state.port = assigned_port
        if channel == "rl":
            ui._RL_WS_PORT = assigned_port
        elif channel == "events":
            ui._EVENT_WS_PORT = assigned_port
        else:
            ui._LOG_WS_PORT = assigned_port
        dummy_thread = types.SimpleNamespace(join=lambda timeout=None: None)
        state.thread = dummy_thread
        return dummy_thread

    monkeypatch.setattr(ui, "_start_channel", _fake_start_channel)

    with caplog.at_level(logging.WARNING):
        threads = ui.start_websockets()

    assert set(threads) == {"rl", "events", "logs"}
    assert len(captured) == 3
    for channel, interval, timeout in captured:
        assert interval == ui._WS_PING_INTERVAL_DEFAULT
        assert timeout == ui._WS_PING_TIMEOUT_DEFAULT

    warning_messages = [record.getMessage() for record in caplog.records]
    assert any("Invalid float value" in message for message in warning_messages)

    for state in ui._WS_CHANNELS.values():
        state.loop = None
        state.thread = None
        state.host = None
        state.port = 0

    ui._RL_WS_PORT = ui._RL_WS_PORT_DEFAULT
    ui._EVENT_WS_PORT = ui._EVENT_WS_PORT_DEFAULT
    ui._LOG_WS_PORT = ui._LOG_WS_PORT_DEFAULT

    for key in list(ui._AUTO_WS_ENV_VALUES):
        os.environ.pop(key, None)
    ui._AUTO_WS_ENV_VALUES.clear()
    ui._AUTO_WS_ENV_CHANNELS.clear()


def test_start_channel_allows_slow_ready(monkeypatch):
    ui = _reload_ui_module()

    class DummyThread:
        def __init__(self, target=None, name=None, daemon=None):
            self._target = target
            self.name = name
            self.daemon = daemon
            self.started = False

        def start(self):
            self.started = True

        def join(self, timeout=None):
            return None

    class SlowReadyQueue:
        required_timeout = 8.5
        last_timeout = None

        def __init__(self, *args, **kwargs):
            self.value = None

        def put(self, value):
            self.value = value

        def get(self, timeout=None):
            type(self).last_timeout = timeout
            if timeout is not None and timeout < self.required_timeout:
                raise Empty
            return self.value

    monkeypatch.setattr(ui, "Queue", SlowReadyQueue)
    monkeypatch.setattr(ui.threading, "Thread", DummyThread)
    monkeypatch.delenv("UI_WS_READY_TIMEOUT", raising=False)

    state = ui._WS_CHANNELS["events"]
    prev_thread = state.thread
    prev_loop = state.loop
    prev_queue = state.queue
    prev_server = state.server
    prev_task = state.task
    prev_port = state.port
    prev_host = state.host
    try:
        thread = ui._start_channel(
            "events",
            host="127.0.0.1",
            port=0,
            queue_size=1,
            ping_interval=1.0,
            ping_timeout=1.0,
        )
        assert isinstance(thread, DummyThread)
        assert thread.started is True
        assert SlowReadyQueue.last_timeout is not None
        assert SlowReadyQueue.last_timeout >= SlowReadyQueue.required_timeout
        assert SlowReadyQueue.last_timeout == ui._WS_READY_TIMEOUT_DEFAULT
    finally:
        state.thread = prev_thread
        state.loop = prev_loop
        state.queue = prev_queue
        state.server = prev_server
        state.task = prev_task
        state.port = prev_port
        state.host = prev_host


@pytest.mark.parametrize("env_value", ["0", "-3.5"])
def test_start_channel_non_positive_ready_timeout_uses_default(
    monkeypatch, caplog, env_value
) -> None:
    ui = _reload_ui_module()

    class DummyThread:
        def __init__(self, target=None, name=None, daemon=None):
            self._target = target
            self.name = name
            self.daemon = daemon
            self.started = False

        def start(self):
            self.started = True

        def join(self, timeout=None):
            return None

    class RecordingQueue:
        last_timeout = None

        def __init__(self, *args, **kwargs):
            self.value = None

        def put(self, value):
            self.value = value

        def get(self, timeout=None):
            type(self).last_timeout = timeout
            return self.value

    monkeypatch.setattr(ui, "Queue", RecordingQueue)
    monkeypatch.setattr(ui.threading, "Thread", DummyThread)
    monkeypatch.setenv("UI_WS_READY_TIMEOUT", env_value)

    state = ui._WS_CHANNELS["events"]
    prev_thread = state.thread
    prev_loop = state.loop
    prev_queue = state.queue
    prev_server = state.server
    prev_task = state.task
    prev_port = state.port
    prev_host = state.host

    try:
        with caplog.at_level(logging.WARNING):
            thread = ui._start_channel(
                "events",
                host="127.0.0.1",
                port=0,
                queue_size=1,
                ping_interval=1.0,
                ping_timeout=1.0,
            )

        assert isinstance(thread, DummyThread)
        assert RecordingQueue.last_timeout == ui._WS_READY_TIMEOUT_DEFAULT
        assert any(
            "must be greater than zero" in record.getMessage()
            for record in caplog.records
        )
    finally:
        state.thread = prev_thread
        state.loop = prev_loop
        state.queue = prev_queue
        state.server = prev_server
        state.task = prev_task
        state.port = prev_port
        state.host = prev_host


def test_enqueue_message_logs_warning_when_queue_full(caplog):
    ui = _reload_ui_module()
    state = ui._WS_CHANNELS["events"]

    class ImmediateLoop:
        def call_soon_threadsafe(self, callback, *args, **kwargs):
            callback(*args, **kwargs)

    class FullQueue:
        def __init__(self) -> None:
            self.maxsize = 1
            self.items = [("stale", None)]

        def put_nowait(self, item):
            if len(self.items) >= self.maxsize:
                raise asyncio.QueueFull
            self.items.append(item)

        def get_nowait(self):
            if not self.items:
                raise asyncio.QueueEmpty
            return self.items.pop(0)

        def task_done(self):
            return None

        def qsize(self):
            return len(self.items)

    class CounterStub:
        def __init__(self) -> None:
            self.calls = []
            self.incremented = 0

        def labels(self, **labels):
            self.calls.append(labels)
            return self

        def inc(self):
            self.incremented += 1

    loop = ImmediateLoop()
    queue = FullQueue()
    counter_stub = CounterStub()
    original_loop = state.loop
    original_queue = state.queue
    previous_counter = getattr(ui, "_WS_QUEUE_DROP_TOTAL", None)
    with state.lock:
        prev_queue_max = state.queue_max
        prev_queue_depth = state.queue_depth
        prev_queue_high = state.queue_high
        prev_drop_count = state.drop_count
        prev_last_warning = state.last_drop_warning_at
        state.queue_max = queue.maxsize
        state.queue_depth = queue.qsize()
        state.queue_high = queue.qsize()
        state.drop_count = 0
        state.last_drop_warning_at = 0.0
    state.loop = loop
    state.queue = queue
    ui._WS_QUEUE_DROP_TOTAL = counter_stub

    caplog.set_level(logging.WARNING)
    caplog.clear()

    try:
        assert ui._enqueue_message("events", {"value": 1}) is True
        with state.lock:
            assert state.drop_count == 1
        assert counter_stub.incremented == 1
        assert counter_stub.calls == [{"channel": "events"}]
        warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "queue overflow" in record.getMessage()
        ]
        assert warnings, "expected queue overflow warning when queue is forced full"
        warning = warnings[-1]
        assert "events" in warning.getMessage()
        assert f"max={queue.maxsize}" in warning.getMessage()
    finally:
        state.loop = original_loop
        state.queue = original_queue
        with state.lock:
            state.queue_max = prev_queue_max
            state.queue_depth = prev_queue_depth
            state.queue_high = prev_queue_high
            state.drop_count = prev_drop_count
            state.last_drop_warning_at = prev_last_warning
        ui._WS_QUEUE_DROP_TOTAL = previous_counter


def test_enqueue_message_handles_closed_loop(caplog):
    ui = _reload_ui_module()
    state = ui._WS_CHANNELS["events"]

    class ClosedLoop:
        def call_soon_threadsafe(self, callback, *args, **kwargs):
            raise RuntimeError("Event loop is closed")

    class CounterStub:
        def __init__(self) -> None:
            self.incremented = 0
            self.last_labels = None

        def labels(self, **labels):
            self.last_labels = labels
            return self

        def inc(self):
            self.incremented += 1

    original_loop = state.loop
    original_queue = state.queue
    previous_counter = getattr(ui, "_WS_QUEUE_DROP_TOTAL", None)
    with state.lock:
        prev_drop_count = state.drop_count
        prev_last_warning = state.last_drop_warning_at
        state.drop_count = 0
        state.last_drop_warning_at = 0.0
    state.loop = ClosedLoop()
    state.queue = asyncio.Queue()
    counter_stub = CounterStub()
    ui._WS_QUEUE_DROP_TOTAL = counter_stub

    caplog.set_level(logging.WARNING)
    caplog.clear()

    try:
        assert ui._enqueue_message("events", {"value": 1}) is False
        with state.lock:
            assert state.drop_count == 1
        assert counter_stub.incremented == 1
        assert counter_stub.last_labels == {"channel": "events"}
        warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING
            and "event loop is closed" in record.getMessage()
        ]
        assert warnings, "expected warning when loop is closed"
    finally:
        state.loop = original_loop
        state.queue = original_queue
        with state.lock:
            state.drop_count = prev_drop_count
            state.last_drop_warning_at = prev_last_warning
        ui._WS_QUEUE_DROP_TOTAL = previous_counter


def _reload_ui_module():
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    sys.modules.setdefault("websockets", types.ModuleType("websockets"))
    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet"))
    production_stub = types.ModuleType("solhunter_zero.production")
    production_stub.load_production_env = lambda: None
    previous_production = sys.modules.get("solhunter_zero.production")
    sys.modules["solhunter_zero.production"] = production_stub
    try:
        from solhunter_zero import ui
        importlib.reload(ui)
    finally:
        if previous_production is not None:
            sys.modules["solhunter_zero.production"] = previous_production
        else:
            sys.modules.pop("solhunter_zero.production", None)
    return ui


def test_manifest_omits_zero_ports():
    ui = _reload_ui_module()

    for key in (
        "UI_EVENTS_WS",
        "UI_EVENTS_WS_URL",
        "UI_WS_URL",
        "UI_RL_WS",
        "UI_RL_WS_URL",
        "UI_LOGS_WS",
        "UI_LOG_WS_URL",
        "UI_EVENTS_WS_PORT",
        "EVENTS_WS_PORT",
        "UI_RL_WS_PORT",
        "RL_WS_PORT",
        "UI_LOG_WS_PORT",
        "LOG_WS_PORT",
    ):
        os.environ.pop(key, None)
    for key in (
        "EVENT_BUS_URL",
        "BROKER_WS_URLS",
        "EVENT_BUS_PEERS",
        "BROKER_URLS",
        "BROKER_URLS_JSON",
        "BROKER_URL",
    ):
        os.environ.pop(key, None)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None
    ui._RL_WS_PORT = ui._RL_WS_PORT_DEFAULT
    ui._EVENT_WS_PORT = ui._EVENT_WS_PORT_DEFAULT
    ui._LOG_WS_PORT = ui._LOG_WS_PORT_DEFAULT

    urls = ui.get_ws_urls()
    for value in urls.values():
        assert value is None

    manifest = ui.build_ui_manifest(None)
    for channel in ("rl", "events", "logs"):
        ws_key = f"{channel}_ws"
        available_key = f"{channel}_ws_available"
        assert manifest[ws_key] is None
        assert manifest[available_key] is False

    app = ui.create_app()
    client = app.test_client()
    response = client.get("/ui/ws-config")
    assert response.status_code == 200
    payload = response.get_json()
    for channel in ("rl", "events", "logs"):
        ws_key = f"{channel}_ws"
        available_key = f"{channel}_ws_available"
        assert payload[ws_key] is None
        assert payload[available_key] is False


def test_manifest_public_host_full_url(monkeypatch):
    ui = _reload_ui_module()

    for key in (
        "UI_EVENTS_WS",
        "UI_EVENTS_WS_URL",
        "UI_WS_URL",
        "UI_RL_WS",
        "UI_RL_WS_URL",
        "UI_LOGS_WS",
        "UI_LOG_WS_URL",
        "UI_PUBLIC_HOST",
        "PUBLIC_URL_HOST",
        "UI_EXTERNAL_HOST",
    ):
        monkeypatch.delenv(key, raising=False)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None

    ui._RL_WS_PORT = 9101
    ui._EVENT_WS_PORT = 9100
    ui._LOG_WS_PORT = 9102

    monkeypatch.setenv("UI_PUBLIC_HOST", "https://public.example:8443")

    manifest = ui.build_ui_manifest(None)

    assert manifest["events_ws"] == "wss://public.example:8443/ws/events"
    assert manifest["rl_ws"] == "wss://public.example:8443/ws/rl"
    assert manifest["logs_ws"] == "wss://public.example:8443/ws/logs"
    for channel in ("rl", "events", "logs"):
        assert manifest[f"{channel}_ws_available"] is True


def test_manifest_public_host_with_port(monkeypatch):
    ui = _reload_ui_module()

    _clear_ws_env(monkeypatch)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None

    ui._RL_WS_PORT = 9101
    ui._EVENT_WS_PORT = 9100
    ui._LOG_WS_PORT = 9102

    monkeypatch.setenv("UI_PUBLIC_HOST", "example.com:8443")

    urls = ui.get_ws_urls()
    assert urls["events"] == "ws://example.com:8443/ws/events"
    assert urls["rl"] == "ws://example.com:8443/ws/rl"
    assert urls["logs"] == "ws://example.com:8443/ws/logs"

    manifest = ui.build_ui_manifest(None)
    assert manifest["events_ws"] == "ws://example.com:8443/ws/events"
    assert manifest["rl_ws"] == "ws://example.com:8443/ws/rl"
    assert manifest["logs_ws"] == "ws://example.com:8443/ws/logs"
    for channel in ("rl", "events", "logs"):
        assert manifest[f"{channel}_ws_available"] is True


def test_manifest_public_host_ipv6(monkeypatch):
    ui = _reload_ui_module()

    _clear_ws_env(monkeypatch)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None

    ui._RL_WS_PORT = 9101
    ui._EVENT_WS_PORT = 9100
    ui._LOG_WS_PORT = 9102

    monkeypatch.setenv("UI_PUBLIC_HOST", "http://[2001:db8::1]")

    urls = ui.get_ws_urls()
    assert urls["events"] == "ws://[2001:db8::1]:9100/ws/events"
    assert urls["rl"] == "ws://[2001:db8::1]:9101/ws/rl"
    assert urls["logs"] == "ws://[2001:db8::1]:9102/ws/logs"

    manifest = ui.build_ui_manifest(None)
    assert manifest["events_ws"] == "ws://[2001:db8::1]:9100/ws/events"
    assert manifest["rl_ws"] == "ws://[2001:db8::1]:9101/ws/rl"
    assert manifest["logs_ws"] == "ws://[2001:db8::1]:9102/ws/logs"
    for channel in ("rl", "events", "logs"):
        assert manifest[f"{channel}_ws_available"] is True


def _clear_ws_env(monkeypatch):
    for key in (
        "UI_EVENTS_WS",
        "UI_EVENTS_WS_URL",
        "UI_WS_URL",
        "UI_RL_WS",
        "UI_RL_WS_URL",
        "UI_LOGS_WS",
        "UI_LOG_WS_URL",
        "UI_PUBLIC_HOST",
        "PUBLIC_URL_HOST",
        "UI_EXTERNAL_HOST",
        "EVENT_BUS_URL",
        "BROKER_WS_URLS",
        "EVENT_BUS_PEERS",
        "BROKER_URLS",
        "BROKER_URLS_JSON",
        "BROKER_URL",
    ):
        monkeypatch.delenv(key, raising=False)


def test_manifest_prefers_event_bus_url(monkeypatch):
    ui = _reload_ui_module()

    _clear_ws_env(monkeypatch)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None

    ui._RL_WS_PORT = ui._RL_WS_PORT_DEFAULT
    ui._EVENT_WS_PORT = ui._EVENT_WS_PORT_DEFAULT
    ui._LOG_WS_PORT = ui._LOG_WS_PORT_DEFAULT

    remote_url = "wss://bus.example:9443/ws"
    monkeypatch.setenv("EVENT_BUS_URL", remote_url)

    urls = ui.get_ws_urls()
    assert urls["events"] == remote_url

    manifest = ui.build_ui_manifest(None)
    assert manifest["events_ws"] == "wss://bus.example:9443/ws/events"
    assert manifest["events_ws_available"] is True


def test_manifest_preserves_query_and_fragment(monkeypatch):
    ui = _reload_ui_module()

    _clear_ws_env(monkeypatch)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None

    ui._RL_WS_PORT = ui._RL_WS_PORT_DEFAULT
    ui._EVENT_WS_PORT = ui._EVENT_WS_PORT_DEFAULT
    ui._LOG_WS_PORT = ui._LOG_WS_PORT_DEFAULT

    remote_url = "wss://bus.example:9443/ws?token=abc#frag"
    monkeypatch.setenv("EVENT_BUS_URL", remote_url)

    urls = ui.get_ws_urls()
    assert urls["events"] == remote_url

    manifest = ui.build_ui_manifest(None)
    assert (
        manifest["events_ws"]
        == "wss://bus.example:9443/ws/events?token=abc#frag"
    )
    assert manifest["events_ws_available"] is True


def test_manifest_uses_broker_ws_urls(monkeypatch):
    ui = _reload_ui_module()

    _clear_ws_env(monkeypatch)

    for state in ui._WS_CHANNELS.values():
        state.port = 0
        state.host = None

    ui._RL_WS_PORT = ui._RL_WS_PORT_DEFAULT
    ui._EVENT_WS_PORT = ui._EVENT_WS_PORT_DEFAULT
    ui._LOG_WS_PORT = ui._LOG_WS_PORT_DEFAULT

    monkeypatch.setenv("BROKER_WS_URLS", " wss://bus.example:9443/ws , ws://backup:8779 ")

    urls = ui.get_ws_urls()
    assert urls["events"] == "wss://bus.example:9443/ws"

    manifest = ui.build_ui_manifest(None)
    assert manifest["events_ws"] == "wss://bus.example:9443/ws/events"
    assert manifest["events_ws_available"] is True
