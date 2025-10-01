import importlib
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
    try:
        with pytest.raises(RuntimeError):
            ui.start_websockets()
    finally:
        sock.close()
        for loop in (ui.rl_ws_loop, ui.event_ws_loop, ui.log_ws_loop):
            if loop is not None:
                loop.call_soon_threadsafe(loop.stop)
