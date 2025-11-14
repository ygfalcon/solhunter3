import errno
import importlib
import socket
import sys
import threading

import pytest

import solhunter_zero.ui as ui

from solhunter_zero.ui import UIState, UIServer


def test_ui_server_start_port_conflict_raises() -> None:
    state = UIState()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        host, port = sock.getsockname()

        server = UIServer(state, host=host, port=port)

        with pytest.raises(RuntimeError) as excinfo:
            server.start()

        message = str(excinfo.value)
        assert f"{host}:{port}" in message
        assert f"errno {errno.EADDRINUSE}" in message

        assert server._server is None
        assert not (server._thread and server._thread.is_alive())


def test_ui_server_start_success_sets_server_and_thread() -> None:
    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        assert server._server is not None
        assert server._thread is not None
        assert server._thread.is_alive()
        assert server.port == server._server.server_port
        assert server.port != 0
        assert server.serve_forever_started.wait(timeout=server.ready_timeout)
    finally:
        server.stop()


def _install_stub_server(monkeypatch, *, server_port: int = 4321):
    stop_event = threading.Event()

    class _StubServer:
        def __init__(self) -> None:
            self.server_port = server_port
            self.socket = None

        def serve_forever(self) -> None:
            stop_event.wait(timeout=1)

        def shutdown(self) -> None:
            stop_event.set()

        def server_close(self) -> None:
            pass

    def _make_server(host, port, app):
        return _StubServer()

    monkeypatch.setattr(ui, "make_server", _make_server)

    return stop_event


def test_ui_server_extended_ready_timeout_default(monkeypatch) -> None:
    monkeypatch.delenv("UI_HTTP_READY_TIMEOUT", raising=False)
    stop_event = _install_stub_server(monkeypatch, server_port=54321)

    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        assert server.ready_timeout == pytest.approx(ui.DEFAULT_UI_HTTP_READY_TIMEOUT)
        assert server.port == 54321
    finally:
        stop_event.set()
        server.stop()


def test_ui_server_ready_timeout_override_handles_slow_start(monkeypatch) -> None:
    monkeypatch.setenv("UI_HTTP_READY_TIMEOUT", "42")
    stop_event = _install_stub_server(monkeypatch, server_port=65432)

    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        assert server.ready_timeout == pytest.approx(42.0)
        assert server.port == 65432
    finally:
        stop_event.set()
        server.stop()


def test_ui_server_ready_timeout_zero_uses_default(monkeypatch) -> None:
    monkeypatch.setenv("UI_HTTP_READY_TIMEOUT", "0")
    stop_event = _install_stub_server(monkeypatch, server_port=65432)

    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        assert server.ready_timeout == pytest.approx(ui.DEFAULT_UI_HTTP_READY_TIMEOUT)
    finally:
        stop_event.set()
        server.stop()


def test_ui_server_ready_timeout_negative_uses_default(monkeypatch) -> None:
    monkeypatch.setenv("UI_HTTP_READY_TIMEOUT", "-5")
    stop_event = _install_stub_server(monkeypatch, server_port=65432)

    state = UIState()
    server = UIServer(state, host="127.0.0.1", port=0)

    server.start()
    try:
        assert server.ready_timeout == pytest.approx(ui.DEFAULT_UI_HTTP_READY_TIMEOUT)
    finally:
        stop_event.set()
        server.stop()


def test_ui_server_worker_bind_failure_surfaces_exception(monkeypatch) -> None:
    state = UIState()

    error = OSError(errno.EACCES, "permission denied")

    def _failing_make_server(*args, **kwargs):
        raise error

    monkeypatch.setattr(ui, "make_server", _failing_make_server)

    server = UIServer(state, host="127.0.0.1", port=0)

    with pytest.raises(RuntimeError) as excinfo:
        server.start()

    message = str(excinfo.value)
    assert "permission denied" in message
    assert f"errno {errno.EACCES}" in message

    assert server._server is None
    assert server._thread is None or not server._thread.is_alive()


def test_ui_server_stop_resets_environment_state() -> None:
    state1 = UIState()
    state1.discovery_recent_provider = lambda limit: [{"mint": "first"}]
    server1 = UIServer(state1, host="127.0.0.1", port=0)

    server1.start()
    try:
        assert ui._get_active_ui_state() is state1
        assert ui._ENV_BOOTSTRAPPED is True
        assert state1.snapshot_discovery_recent(1) == [{"mint": "first"}]
    finally:
        server1.stop()

    assert ui._get_active_ui_state() is None
    assert ui._ENV_BOOTSTRAPPED is False

    state2 = UIState()
    state2.discovery_recent_provider = lambda limit: [{"mint": "second"}]
    server2 = UIServer(state2, host="127.0.0.1", port=0)

    server2.start()
    try:
        active_state = ui._get_active_ui_state()
        assert active_state is state2
        assert active_state.snapshot_discovery_recent(1) == [{"mint": "second"}]
        assert ui._ENV_BOOTSTRAPPED is True
    finally:
        server2.stop()

    assert ui._get_active_ui_state() is None
    assert ui._ENV_BOOTSTRAPPED is False


@pytest.mark.timeout(30)
def test_ui_server_stop_stops_websockets_and_clears_registry() -> None:
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)

    pytest.importorskip("websockets")
    websockets_module = importlib.import_module("websockets")
    assert getattr(websockets_module, "__file__", None)

    reloaded_ui = importlib.reload(ui)
    server = reloaded_ui.UIServer(reloaded_ui.UIState(), host="127.0.0.1", port=0)

    server.start()
    threads: dict[str, threading.Thread] = {}
    try:
        assert server in reloaded_ui._ACTIVE_HTTP_SERVERS
        threads = reloaded_ui.start_websockets()
        for state in reloaded_ui._WS_CHANNELS.values():
            assert state.loop is not None
            assert state.thread is not None
            assert state.thread.is_alive()
    finally:
        server.stop()

    assert not reloaded_ui._ACTIVE_HTTP_SERVERS

    for thread in threads.values():
        thread.join(timeout=2)

    for state in reloaded_ui._WS_CHANNELS.values():
        assert state.loop is None
        assert state.thread is None
        assert state.ready_status == "stopped"
