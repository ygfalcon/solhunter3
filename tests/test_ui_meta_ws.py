import asyncio
import importlib
import json
import sys
import threading
import time
import types
import urllib.request

import pytest
from werkzeug.serving import make_server


@pytest.mark.timeout(30)
def test_ui_meta_websocket_smoke(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    ws_mod = pytest.importorskip("websockets")

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )
    base58_mod = types.ModuleType("base58")
    base58_mod.b58decode = lambda *a, **k: b""
    base58_mod.b58encode = lambda *a, **k: b""
    sys.modules.setdefault("base58", base58_mod)
    sa = sys.modules.get("sqlalchemy")
    if sa is not None:
        class _Index:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        setattr(sa, "Index", getattr(sa, "Index", _Index))

    sys.modules.pop("solhunter_zero.ui", None)
    import solhunter_zero.ui as ui
    importlib.reload(ui)

    assert ui.websockets is not None

    monkeypatch.setenv("UI_RL_WS_PORT", "0")
    monkeypatch.setenv("UI_EVENT_WS_PORT", "0")
    monkeypatch.setenv("UI_LOG_WS_PORT", "0")

    threads: dict[str, threading.Thread | None] = {}
    try:
        threads = ui.start_websockets()
        app = ui.create_app()
        server = make_server("127.0.0.1", 0, app)
        port = server.server_port
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/ui/meta", timeout=5
            ) as response:
                meta = json.loads(response.read().decode("utf-8"))
            assert meta["url"].startswith("http://127.0.0.1:")
            assert meta["events_ws"].startswith("ws://")

            ui.push_event({"event": "heartbeat", "ts": time.time()})
            time.sleep(0.2)

            async def _collect() -> tuple[str, str]:
                async with ws_mod.connect(meta["events_ws"]) as conn:
                    hello = await asyncio.wait_for(conn.recv(), timeout=5)
                    payload = await asyncio.wait_for(conn.recv(), timeout=5)
                    return hello, payload

            hello_msg, payload_msg = asyncio.run(_collect())
            hello_obj = json.loads(hello_msg)
            assert hello_obj.get("event") == "hello"
            payload_obj = json.loads(payload_msg)
            assert payload_obj.get("event") == "heartbeat"
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    finally:
        ui.stop_websockets()
        for t in threads.values():
            if t is not None:
                t.join(timeout=1)
