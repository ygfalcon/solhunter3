import importlib
import ssl
import sys
import types
from pathlib import Path

import pytest


def _reload_ui(monkeypatch, websockets_module: types.ModuleType | None = None):
    for name in list(sys.modules):
        if name.startswith("solhunter_zero.ui"):
            sys.modules.pop(name, None)

    if websockets_module is None:
        websockets_module = types.ModuleType("websockets")
        websockets_module.__spec__ = importlib.machinery.ModuleSpec("websockets", None)

        async def _noop_serve(*_args, **_kwargs):
            class _Server:
                sockets = [types.SimpleNamespace(getsockname=lambda: ("127.0.0.1", 0))]

                def close(self):
                    pass

                async def wait_closed(self):
                    pass

            return _Server()

        websockets_module.serve = _noop_serve  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "websockets", websockets_module)
    monkeypatch.setitem(sys.modules, "sqlparse", types.SimpleNamespace())
    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.wallet",
        types.ModuleType("solhunter_zero.wallet"),
    )

    module = importlib.import_module("solhunter_zero.ui")
    return module


def test_start_websockets_uses_ssl_context(monkeypatch):
    cert_path = Path("tests/fixtures/ui_ws_cert.pem")
    key_path = Path("tests/fixtures/ui_ws_key.pem")
    assert cert_path.exists()
    assert key_path.exists()

    monkeypatch.setenv("UI_WS_SCHEME", "wss")
    monkeypatch.setenv("UI_WS_SSL_CERT_PATH", str(cert_path))
    monkeypatch.setenv("UI_WS_SSL_KEY_PATH", str(key_path))

    captured_ssl: list[ssl.SSLContext | None] = []

    class _FakeServer:
        def __init__(self, host: str, port: int):
            assigned = port or 8767
            self._address = (host, assigned)
            self.sockets = [types.SimpleNamespace(getsockname=lambda: self._address)]

        def close(self):
            pass

        async def wait_closed(self):
            pass

    async def _fake_serve(handler, host, port, **kwargs):  # type: ignore[override]
        captured_ssl.append(kwargs.get("ssl"))
        return _FakeServer(host, port or 8767)

    ws_module = types.ModuleType("websockets")
    ws_module.__spec__ = importlib.machinery.ModuleSpec("websockets", None)
    ws_module.serve = _fake_serve  # type: ignore[attr-defined]

    ui = _reload_ui(monkeypatch, ws_module)

    threads = {}
    try:
        threads = ui.start_websockets()
        assert len(captured_ssl) == 3
        assert all(isinstance(ctx, ssl.SSLContext) for ctx in captured_ssl)
        assert ui._infer_ws_scheme() == "wss"
    finally:
        ui.stop_websockets()
        for thread in threads.values():
            thread.join(timeout=1)


def test_infer_ws_scheme_requires_context(monkeypatch, caplog):
    monkeypatch.setenv("UI_WS_SCHEME", "wss")
    monkeypatch.delenv("UI_WS_SSL_CERT_PATH", raising=False)
    monkeypatch.delenv("UI_WS_SSL_KEY_PATH", raising=False)
    monkeypatch.delenv("UI_WS_SSL_CA_PATH", raising=False)

    ui = _reload_ui(monkeypatch)

    with caplog.at_level("WARNING"):
        scheme = ui._infer_ws_scheme()

    assert scheme == "ws"
    assert any("TLS requested" in record.getMessage() for record in caplog.records)
