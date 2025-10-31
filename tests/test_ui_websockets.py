import importlib
import os
import socket
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


def _reload_ui_module():
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
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
