import importlib
import socket
import sys
import time

import pytest


@pytest.mark.timeout(30)
def test_websocket_threads_bind():
    pytest.importorskip("websockets")

    # ``tests.stubs`` replaces ``websockets`` with a lightweight stub. Reload the
    # real implementation so the websocket servers can be created.
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    importlib.import_module("websockets")

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
