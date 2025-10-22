import asyncio
import importlib
import sys
import types

import pytest



def _load_autopilot(monkeypatch):
    wallet_stub = types.ModuleType("wallet")
    wallet_stub.KEYPAIR_DIR = ""
    wallet_stub.setup_default_keypair = lambda: types.SimpleNamespace(name="kp")
    data_sync_stub = types.ModuleType("data_sync")
    data_sync_stub.start_scheduler = lambda *a, **k: None
    data_sync_stub.stop_scheduler = lambda: None
    main_stub = types.ModuleType("main")
    main_stub.run_auto = lambda: None
    config_stub = types.ModuleType("config")
    config_stub.CONFIG_DIR = ""
    config_stub.get_active_config_name = lambda: None
    config_stub.load_config = lambda p: {}
    config_stub.apply_env_overrides = lambda c: c
    config_stub.initialize_event_bus = lambda: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.wallet", wallet_stub)
    monkeypatch.setitem(sys.modules, "solhunter_zero.data_sync", data_sync_stub)
    monkeypatch.setitem(sys.modules, "solhunter_zero.main", main_stub)
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", config_stub)
    import solhunter_zero.autopilot as ap
    return importlib.reload(ap)


def test_maybe_start_event_bus_local(monkeypatch):
    ap = _load_autopilot(monkeypatch)
    called = {}
    monkeypatch.setattr(ap, "_start_event_bus", lambda url: called.setdefault("url", url))
    monkeypatch.setenv("EVENT_BUS_URL", "ws://localhost:9999")
    ap._maybe_start_event_bus({})
    assert called["url"] == "ws://localhost:9999"


def test_maybe_start_event_bus_remote(monkeypatch):
    ap = _load_autopilot(monkeypatch)
    called = {}
    monkeypatch.setattr(ap, "_start_event_bus", lambda url: called.setdefault("url", url))
    monkeypatch.setenv("EVENT_BUS_URL", "ws://remote:9999")
    ap._maybe_start_event_bus({})
    assert "url" not in called


def test_maybe_start_event_bus_default(monkeypatch):
    ap = _load_autopilot(monkeypatch)
    called = {}
    monkeypatch.setattr(ap, "_start_event_bus", lambda url: called.setdefault("url", url))
    monkeypatch.delenv("EVENT_BUS_URL", raising=False)
    ap._maybe_start_event_bus({})
    assert called["url"] == ap.DEFAULT_WS_URL


def test_start_event_bus_invokes_readiness(monkeypatch):
    ap = _load_autopilot(monkeypatch)
    called = {}

    async def fake_start(host, port):
        called["host"] = host
        called["port"] = port

    class DummyConn:
        async def __aenter__(self):
            called["connect"] = True
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(url):
        called["url"] = url
        return DummyConn()

    async def fake_stop() -> None:
        called["stopped"] = True

    monkeypatch.setattr(ap.event_bus, "start_ws_server", fake_start)
    monkeypatch.setattr(ap.event_bus, "stop_ws_server", fake_stop)
    monkeypatch.setattr(ap.websockets, "connect", fake_connect)
    ap._start_event_bus("ws://localhost:8779")
    assert called["host"] == "localhost"
    assert called["port"] == 8779
    assert called["url"] == "ws://localhost:8779"
    assert called.get("connect") is True
    if ap._EVENT_LOOP:
        fut = asyncio.run_coroutine_threadsafe(
            ap.event_bus.stop_ws_server(), ap._EVENT_LOOP
        )
        fut.result(timeout=5)
        ap._EVENT_LOOP.call_soon_threadsafe(ap._EVENT_LOOP.stop)
