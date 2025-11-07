import os
import io
import json
import asyncio
import types
import contextlib
import importlib.machinery
import sys
import socket
import time
import urllib.error
import urllib.request
import pytest

# Skip this module when running lightweight CI self-tests
if os.getenv("SELFTEST_SKIP_ARTIFACTS") == "1" or os.getenv("CI") == "true":
    pytest.skip("Skipping heavy UI tests in self-test mode", allow_module_level=True)
from solders.keypair import Keypair
import logging
import threading

pytest.importorskip("google.protobuf")

torch_mod = types.ModuleType("torch")
torch_mod.__spec__ = importlib.machinery.ModuleSpec("torch", loader=None)
torch_mod.no_grad = contextlib.nullcontext
torch_mod.tensor = lambda *a, **k: None
torch_mod.nn = types.SimpleNamespace(
    Module=object,
    LSTM=object,
    Linear=object,
    TransformerEncoder=object,
    TransformerEncoderLayer=object,
)
torch_mod.Tensor = object
torch_mod.optim = types.ModuleType("optim")
sys.modules.setdefault("torch", torch_mod)
sys.modules.setdefault("torch.nn", torch_mod.nn)
dummy_trans = types.ModuleType("transformers")
dummy_trans.pipeline = lambda *a, **k: lambda x: []
sys.modules.setdefault("transformers", dummy_trans)
sys.modules["transformers"].pipeline = dummy_trans.pipeline
dummy_sklearn = types.ModuleType("sklearn")
dummy_sklearn.linear_model = types.SimpleNamespace(LinearRegression=object)
dummy_sklearn.ensemble = types.SimpleNamespace(
    GradientBoostingRegressor=object,
    RandomForestRegressor=object,
)
sys.modules.setdefault("sklearn", dummy_sklearn)
sys.modules.setdefault("sklearn.linear_model", dummy_sklearn.linear_model)
sys.modules.setdefault("sklearn.ensemble", dummy_sklearn.ensemble)
dummy_watchfiles = types.ModuleType("watchfiles")
dummy_watchfiles.awatch = lambda *a, **k: None
sys.modules.setdefault("watchfiles", dummy_watchfiles)

dummy_crypto = types.ModuleType("cryptography")
fernet_mod = types.ModuleType("fernet")
fernet_mod.Fernet = object
fernet_mod.InvalidToken = Exception
dummy_crypto.fernet = fernet_mod
sys.modules.setdefault("cryptography", dummy_crypto)
sys.modules.setdefault("cryptography.fernet", fernet_mod)

rich_mod = types.ModuleType("rich")
rich_mod.__spec__ = importlib.machinery.ModuleSpec("rich", loader=None)
console_mod = types.ModuleType("rich.console")
console_mod.__spec__ = importlib.machinery.ModuleSpec("rich.console", loader=None)


class _Console:
    def print(self, *a, **k):
        pass


console_mod.Console = _Console
panel_mod = types.ModuleType("rich.panel")
panel_mod.__spec__ = importlib.machinery.ModuleSpec("rich.panel", loader=None)


class _Panel:
    def __init__(self, *a, **k):
        pass


panel_mod.Panel = _Panel
table_mod = types.ModuleType("rich.table")
table_mod.__spec__ = importlib.machinery.ModuleSpec("rich.table", loader=None)


class _Table:
    def __init__(self, *a, **k):
        pass

    def add_column(self, *a, **k):
        pass

    def add_row(self, *a, **k):
        pass


table_mod.Table = _Table
sys.modules.setdefault("rich", rich_mod)
sys.modules.setdefault("rich.console", console_mod)
sys.modules.setdefault("rich.panel", panel_mod)
sys.modules.setdefault("rich.table", table_mod)

sqlparse_mod = types.ModuleType("sqlparse")
sqlparse_mod.__spec__ = importlib.machinery.ModuleSpec("sqlparse", loader=None)
def _format(sql, strip_comments=True, **kwargs):
    if strip_comments:
        lines = []
        for line in sql.splitlines():
            if "--" in line:
                line = line.split("--", 1)[0]
            if line.strip():
                lines.append(line)
        sql = "\n".join(lines)
    return sql


def _parse(sql):
    parts = [p for p in sql.split(";") if p.strip()]
    stmts = []
    for part in parts:
        first = part.strip().split()[0] if part.strip() else ""

        def _token_first(skip_ws=True, _v=first):
            return types.SimpleNamespace(value=_v)

        stmts.append(
            types.SimpleNamespace(is_whitespace=False, token_first=_token_first)
        )
    return stmts


sqlparse_mod.format = _format
sqlparse_mod.parse = _parse
sys.modules.setdefault("sqlparse", sqlparse_mod)

dummy_pydantic = types.ModuleType("pydantic")
dummy_pydantic.__spec__ = importlib.machinery.ModuleSpec("pydantic", loader=None)


class _BaseModel:
    def __init__(self, **data):
        for k, v in data.items():
            setattr(self, k, v)

    def dict(self, *a, **k):
        return self.__dict__

    def model_dump(self, *a, **k):  # pragma: no cover - pydantic v2 compat
        return self.__dict__


dummy_pydantic.BaseModel = _BaseModel
dummy_pydantic.AnyUrl = str
dummy_pydantic.ValidationError = Exception
dummy_pydantic.root_validator = lambda *a, **k: (lambda f: f)
dummy_pydantic.validator = lambda *a, **k: (lambda f: f)
dummy_pydantic.field_validator = lambda *a, **k: (lambda f: f)
dummy_pydantic.model_validator = lambda *a, **k: (lambda f: f)
sys.modules.setdefault("pydantic", dummy_pydantic)

import solhunter_zero.config as config
config.initialize_event_bus = lambda: None
import solhunter_zero.ui as ui
from collections import deque
from solhunter_zero.portfolio import Position


# ``solhunter_zero.ui`` no longer creates a Flask application at import time.
# Instantiate one explicitly for tests.
ui.app = ui.create_app()


def test_index_html_uses_recent_count_over_length():
    state = ui.UIState(
        discovery_provider=lambda: {
            "recent": [f"Token {i}" for i in range(120)],
            "recent_count": 500,
        }
    )
    app = ui.create_app(state)
    client = app.test_client()
    resp = client.get("/")
    html = resp.get_data(as_text=True)
    assert "500 tracked" in html
    assert "Newest 120 tokens shown below." in html


def test_build_dashboard_metrics_returns_expected_values():
    state = ui.UIState(
        status_provider=lambda: {
            "heartbeat": "alive",
            "trading_loop": True,
            "iterations_completed": 5,
            "trade_count": "7",
        },
        summary_provider=lambda: {"elapsed_s": 1.25},
        activity_provider=lambda: [{"id": 1}],
        trades_provider=lambda: [{"id": "t1"}, {"id": "t2"}],
        logs_provider=lambda: [{"message": "log"}],
        weights_provider=lambda: {"alpha": 0.6, "beta": 0.4},
        actions_provider=lambda: [{"action": "run"}],
    )
    metrics = ui._build_dashboard_metrics(
        state,
        status=state.snapshot_status(),
        summary=state.snapshot_summary(),
        activity=state.snapshot_activity(),
        trades=state.snapshot_trades(),
        logs=state.snapshot_logs(),
        weights=state.snapshot_weights(),
        actions=state.snapshot_actions(),
    )
    assert metrics["counts"] == {
        "activity": 1,
        "trades": 2,
        "logs": 1,
        "weights": 2,
        "actions": 1,
    }
    tile_map = metrics["stat_tile_map"]
    assert tile_map["heartbeat"]["value"] == "alive"
    assert tile_map["iterations"]["value"] == "5"
    assert tile_map["trades"]["value"] == "7"
    assert tile_map["trades"]["caption"] == "1.40 per iteration"
    assert metrics["raw"]["trade_count"] == 7
    assert metrics["raw"]["iterations_completed"] == 5


def test_status_endpoint_includes_dashboard_metrics():
    state = ui.UIState(
        status_provider=lambda: {"heartbeat": "ok", "trading_loop": True},
        summary_provider=lambda: {
            "timestamp": "2024-01-01T00:00:00Z",
            "actions_count": 3,
            "discovered_count": 4,
            "elapsed_s": 2.0,
        },
        discovery_provider=lambda: {"recent": ["AAA", "BBB"]},
        activity_provider=lambda: [{"id": 1}, {"id": 2}],
        trades_provider=lambda: [{"id": "t"}],
        logs_provider=lambda: [{"message": "log"}],
        weights_provider=lambda: {"agent": 0.9},
        actions_provider=lambda: [{"action": "trade"}],
    )
    app = ui.create_app(state)
    client = app.test_client()
    resp = client.get("/status")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["activity_count"] == 2
    assert payload["trade_count"] == 1
    metrics = payload.get("dashboard_metrics")
    assert metrics is not None
    assert metrics["counts"]["activity"] == 2
    assert metrics["counts"]["trades"] == 1
    assert metrics["counts"]["actions"] == 1


def test_tokens_endpoint_exposes_discovery_telemetry():
    state = ui.UIState(
        discovery_provider=lambda: {
            "recent": ["AAA"],
            "last_fetch_ts": 123.0,
            "cooldown_seconds": 45.0,
            "last_method": "helius",
        }
    )
    app = ui.create_app(state)
    client = app.test_client()
    resp = client.get("/tokens")
    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["last_fetch_ts"] == 123.0
    assert payload["cooldown_seconds"] == 45.0
    assert payload["last_method"] == "helius"


def test_index_html_renders_discovery_meta():
    state = ui.UIState(
        discovery_provider=lambda: {
            "recent": [],
            "last_method": "helius",
            "cooldown_seconds": 0.0,
            "last_fetch_ts": None,
        }
    )
    app = ui.create_app(state)
    html = app.test_client().get("/").get_data(as_text=True)
    assert "Method: helius" in html
    assert "Cooldown: ready" in html
    assert "Last fetch:" in html


def test_shutdown_endpoint_requires_token():
    app = ui.create_app(shutdown_token="secret-token")
    client = app.test_client()

    resp = client.get(
        "/__shutdown__",
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 403

    resp = client.get(
        "/__shutdown__",
        headers={"X-UI-Shutdown-Token": "wrong"},
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 403


def test_shutdown_endpoint_rejects_non_loopback():
    app = ui.create_app(shutdown_token="secret-token")
    client = app.test_client()

    resp = client.get(
        "/__shutdown__",
        headers={"X-UI-Shutdown-Token": "secret-token"},
        environ_overrides={"REMOTE_ADDR": "203.0.113.5"},
    )
    assert resp.status_code == 403


def test_discovery_update_requires_loopback_remote():
    state = ui.UIState()
    app = ui.create_app(state)
    client = app.test_client()

    resp = client.post(
        "/discovery",
        json={"method": "mempool"},
        environ_overrides={"REMOTE_ADDR": "203.0.113.5"},
    )
    assert resp.status_code == 403

    resp = client.post(
        "/discovery",
        json={"method": "mempool"},
        environ_overrides={"REMOTE_ADDR": "127.0.0.1"},
    )
    assert resp.status_code == 200



def test_ensure_active_keypair_selects_single(monkeypatch):
    monkeypatch.setattr(ui.wallet, "get_active_keypair_name", lambda: None)
    monkeypatch.setattr(ui.wallet, "list_keypairs", lambda: ["only"])
    selected = {}

    def _select(name):
        selected["name"] = name

    monkeypatch.setattr(ui.wallet, "select_keypair", _select)
    monkeypatch.setattr(ui.wallet, "KEYPAIR_DIR", "kpdir")
    monkeypatch.delenv("KEYPAIR_PATH", raising=False)

    ui.ensure_active_keypair()

    assert selected["name"] == "only"
    assert os.getenv("KEYPAIR_PATH") == os.path.join("kpdir", "only.json")


def test_ensure_active_config_selects_single(monkeypatch):
    monkeypatch.setattr(ui, "get_active_config_name", lambda: None)
    monkeypatch.setattr(ui, "list_configs", lambda: ["cfg"])
    selected = {}

    def _select(name):
        selected["name"] = name

    monkeypatch.setattr(ui, "select_config", _select)
    cfg = {"a": 1}
    monkeypatch.setattr(ui, "load_selected_config", lambda: cfg)
    called = {}

    def _set_env(c):
        called["cfg"] = c

    monkeypatch.setattr(ui, "set_env_from_config", _set_env)

    ui.ensure_active_config()

    assert selected["name"] == "cfg"
    assert called["cfg"] is cfg


def test_ensure_active_keypair_respects_existing_env(monkeypatch):
    monkeypatch.setenv("KEYPAIR_PATH", "custom-path")
    called = {}

    def _record():
        called["hit"] = True
        return None

    monkeypatch.setattr(ui.wallet, "get_active_keypair_name", _record)

    result = ui.ensure_active_keypair()

    assert result == "custom-path"
    assert "hit" not in called


def test_ensure_active_keypair_no_auto_select_for_multiple(monkeypatch):
    monkeypatch.delenv("KEYPAIR_PATH", raising=False)
    monkeypatch.setattr(ui.wallet, "get_active_keypair_name", lambda: None)
    monkeypatch.setattr(ui.wallet, "list_keypairs", lambda: ["a", "b"])

    result = ui.ensure_active_keypair()

    assert result is None
    assert os.getenv("KEYPAIR_PATH") is None


def test_ensure_active_config_returns_loaded(monkeypatch):
    cfg = {"value": 1}
    recorded = {}

    monkeypatch.setattr(ui, "get_active_config_name", lambda: "active")
    monkeypatch.setattr(ui, "load_selected_config", lambda: cfg)

    def _capture(data):
        recorded["cfg"] = data

    monkeypatch.setattr(ui, "set_env_from_config", _capture)

    result = ui.ensure_active_config()

    assert result is cfg
    assert recorded["cfg"] is cfg


def test_list_keypairs_delegates_to_wallet(monkeypatch):
    calls = []

    def _stub():
        calls.append("called")
        return ["one"]

    monkeypatch.setattr(ui.wallet, "list_keypairs", _stub)

    assert ui.list_keypairs() == ["one"]
    assert calls == ["called"]


def test_select_config_delegates_to_config_module(monkeypatch):
    recorded = {}

    def _select(name):
        recorded["name"] = name

    monkeypatch.setattr(ui.config_module, "select_config", _select)

    ui.select_config("demo")

    assert recorded["name"] == "demo"


def test_initialize_event_bus_re_exports(monkeypatch):
    called = {}

    def _init():
        called["hit"] = True

    monkeypatch.setattr(ui.config_module, "initialize_event_bus", _init)

    ui.initialize_event_bus()

    assert called["hit"] is True


def test_start_handles_are_exposed():
    assert hasattr(ui, "start_all_thread")
    assert hasattr(ui, "start_all_proc")
    assert hasattr(ui, "start_all_ready")


def test_uiserver_start_raises_when_background_thread_fails(monkeypatch):
    state = ui.UIState()

    class _FailingServer:
        def __init__(self) -> None:
            self.server_port = 9999
            self.closed = False
            self.shutdown_called = False

        def serve_forever(self) -> None:
            raise OSError("simulated bind failure")

        def server_close(self) -> None:
            self.closed = True

        def shutdown(self) -> None:
            self.shutdown_called = True

    created_servers: list[_FailingServer] = []

    def _make_server(host, port, app, threaded):
        server = _FailingServer()
        created_servers.append(server)
        return server

    monkeypatch.setattr("werkzeug.serving.make_server", _make_server)

    server = ui.UIServer(state, host="127.0.0.1", port=0)

    with pytest.raises(ui.UIStartupError) as excinfo:
        server.start()

    assert isinstance(excinfo.value.__cause__, OSError)
    assert "simulated bind failure" in str(excinfo.value.__cause__)
    assert created_servers and created_servers[0].closed is True
    assert server._thread is None
