import importlib
import os
import socket
import threading
import sys
import time
import types

import pytest


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

    threads = ui.start_websockets()
    try:
        for port in (8767, ui._EVENT_WS_PORT, 8768):
            for _ in range(50):
                try:
                    with socket.create_connection(("localhost", port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                pytest.fail(f"port {port} did not bind")
    finally:
        for loop in (ui.rl_ws_loop, ui.event_ws_loop, ui.log_ws_loop):
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
        for t in threads.values():
            t.join(timeout=1)


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
    for key, value in stale_urls.items():
        monkeypatch.setenv(key, value)
        ui._AUTO_WS_ENV_VALUES[key] = value

    sock = socket.socket()
    sock.bind(("localhost", 8767))
    sock.listen(1)
    threads: dict[str, threading.Thread] = {}
    try:
        threads = ui.start_websockets()
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
    finally:
        sock.close()
        for loop in (ui.rl_ws_loop, ui.event_ws_loop, ui.log_ws_loop):
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
        for t in threads.values():
            t.join(timeout=1)


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


def _reload_ui_module():
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    sys.modules.setdefault("websockets", types.ModuleType("websockets"))
    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet"))
    from solhunter_zero import ui
    importlib.reload(ui)
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
