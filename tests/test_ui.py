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
    assert metrics["stat_tile_map"]["heartbeat"]["value"] == "ok"
    assert metrics["raw"]["trade_count"] == 1
    assert payload["recent_tokens"] == ["AAA", "BBB"]



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


def test_start_and_stop(monkeypatch):
    ui.start_all_thread = None
    ui.start_all_proc = None
    ui.start_all_ready = threading.Event()
    events = []

    class DummyProc:
        def __init__(self, *a, **k):
            events.append("start")
            self.ev = threading.Event()

        def poll(self):
            return None if not self.ev.is_set() else 0

        def wait(self, timeout=None):
            self.ev.wait(timeout)

        def terminate(self):
            events.append("term")
            self.ev.set()

        def kill(self):
            events.append("kill")
            self.ev.set()

    monkeypatch.setattr(ui.subprocess, "Popen", DummyProc)

    client = ui.app.test_client()

    resp = client.post("/start_all")
    assert resp.get_json()["status"] == "started"
    assert ui.start_all_thread and ui.start_all_thread.is_alive()
    assert ui.start_all_ready.is_set()

    resp = client.post("/start_all")
    assert resp.get_json()["status"] == "already running"

    resp = client.post("/stop_all")
    assert resp.get_json()["status"] == "stopped"

    ui.start_all_thread.join(timeout=1)
    assert not ui.start_all_thread.is_alive()
    assert "term" in events


def test_balances_includes_usd(monkeypatch):
    pf = ui.Portfolio(path=None)
    pf.balances = {"tok": Position("tok", 2, 1.0)}

    monkeypatch.setattr(ui, "Portfolio", lambda *a, **k: pf)
    monkeypatch.setattr(ui, "fetch_token_prices", lambda tokens: {"tok": 3.0})

    client = ui.app.test_client()
    resp = client.get("/balances")
    data = resp.get_json()

    assert data["tok"]["price"] == 3.0
    assert data["tok"]["usd"] == 6.0
    assert data["tok"]["amount"] == 2


def test_trading_loop_awaits_run_iteration(monkeypatch):
    calls = []

    async def fake_run_iteration(*args, **kwargs):
        calls.append(True)
        ui.stop_event.set()

    monkeypatch.setattr(ui.main_module, "_run_iteration", fake_run_iteration)
    monkeypatch.setattr(ui, "MEMORY", object())
    monkeypatch.setattr(ui, "Portfolio", lambda *a, **k: object())
    monkeypatch.setattr(ui, "load_selected_config", lambda: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    async def _load_sel():
        return None

    async def _load_kp(path):
        return None

    monkeypatch.setattr(ui.wallet, "load_selected_keypair_async", _load_sel)
    monkeypatch.setattr(ui.wallet, "load_keypair_async", _load_kp)
    monkeypatch.setattr(ui, "ensure_active_keypair", lambda: None)
    monkeypatch.setattr(ui, "ensure_active_config", lambda: None)
    monkeypatch.delenv("KEYPAIR_PATH", raising=False)
    monkeypatch.setattr(ui, "loop_delay", 0)

    ui.stop_event.clear()

    thread = threading.Thread(
        target=lambda: asyncio.run(ui.trading_loop()), daemon=True
    )
    thread.start()
    thread.join(timeout=1)

    assert calls


def test_trading_loop_falls_back_to_env_keypair(monkeypatch):
    used = {}

    async def fake_run_iteration(*args, keypair=None, **kwargs):
        used["keypair"] = keypair
        ui.stop_event.set()

    monkeypatch.setattr(ui.main_module, "_run_iteration", fake_run_iteration)
    monkeypatch.setattr(ui, "MEMORY", object())
    monkeypatch.setattr(ui, "Portfolio", lambda *a, **k: object())
    monkeypatch.setattr(ui, "load_selected_config", lambda: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    async def _load_sel2():
        return None

    monkeypatch.setattr(ui.wallet, "load_selected_keypair_async", _load_sel2)
    monkeypatch.setattr(ui, "ensure_active_keypair", lambda: None)
    monkeypatch.setattr(ui, "ensure_active_config", lambda: None)

    sentinel = object()

    async def fake_load_keypair(path):
        used.setdefault("paths", []).append(path)
        return sentinel

    monkeypatch.setattr(ui.wallet, "load_keypair_async", fake_load_keypair)
    monkeypatch.setenv("KEYPAIR_PATH", "envpath")
    monkeypatch.setattr(ui, "loop_delay", 0)

    ui.stop_event.clear()
    thread = threading.Thread(
        target=lambda: asyncio.run(ui.trading_loop()), daemon=True
    )
    thread.start()
    thread.join(timeout=1)

    assert used["keypair"] is sentinel
    assert "envpath" in used.get("paths", [])


def test_trading_loop_checks_redis_only(monkeypatch):
    counts = {"check": 0, "init": 0}

    def fake_check():
        counts["check"] += 1

    def fake_init():
        counts["init"] += 1

    async def fake_run_iteration(*args, **kwargs):
        ui.stop_event.set()

    monkeypatch.setattr(ui.main_module, "_run_iteration", fake_run_iteration)
    monkeypatch.setattr(ui, "MEMORY", object())
    monkeypatch.setattr(ui, "Portfolio", lambda *a, **k: object())
    monkeypatch.setattr(ui, "load_selected_config", lambda: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    async def _load_sel():
        return None

    async def _load_kp(path):
        return None

    monkeypatch.setattr(ui.wallet, "load_selected_keypair_async", _load_sel)
    monkeypatch.setattr(ui.wallet, "load_keypair_async", _load_kp)
    monkeypatch.setattr(ui, "ensure_active_keypair", lambda: None)
    monkeypatch.setattr(ui, "ensure_active_config", lambda: None)
    monkeypatch.setattr(ui, "_check_redis_connection", fake_check)
    monkeypatch.setattr(ui, "initialize_event_bus", fake_init)
    monkeypatch.setattr(ui, "_event_bus_initialized", True)
    monkeypatch.delenv("KEYPAIR_PATH", raising=False)
    monkeypatch.setattr(ui, "loop_delay", 0)

    ui.stop_event.clear()
    thread = threading.Thread(
        target=lambda: asyncio.run(ui.trading_loop()), daemon=True
    )
    thread.start()
    thread.join(timeout=1)

    assert counts == {"check": 1, "init": 0}


def test_get_and_set_risk_params(monkeypatch):
    monkeypatch.delenv("RISK_TOLERANCE", raising=False)
    monkeypatch.delenv("MAX_ALLOCATION", raising=False)
    monkeypatch.delenv("RISK_MULTIPLIER", raising=False)

    client = ui.app.test_client()

    resp = client.get("/risk")
    data = resp.get_json()
    assert "risk_tolerance" in data

    resp = client.post(
        "/risk",
        json={
            "risk_tolerance": 0.2,
            "max_allocation": 0.3,
            "risk_multiplier": 1.5,
        },
    )
    assert resp.get_json()["status"] == "ok"
    assert os.getenv("RISK_TOLERANCE") == "0.2"
    assert os.getenv("MAX_ALLOCATION") == "0.3"
    assert os.getenv("RISK_MULTIPLIER") == "1.5"


def test_risk_params_invalid_env(monkeypatch, caplog):
    monkeypatch.setenv("RISK_TOLERANCE", "oops")
    monkeypatch.setenv("MAX_ALLOCATION", "bad")
    monkeypatch.setenv("RISK_MULTIPLIER", "nope")
    client = ui.app.test_client()
    with caplog.at_level(logging.WARNING, logger="solhunter_zero.ui"):
        resp = client.get("/risk")
    data = resp.get_json()
    assert data["risk_tolerance"] == 0.1
    assert data["max_allocation"] == 0.2
    assert data["risk_multiplier"] == 1.0
    assert "RISK_TOLERANCE" in caplog.text
    assert "MAX_ALLOCATION" in caplog.text
    assert "RISK_MULTIPLIER" in caplog.text


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"risk_tolerance": 0.2}, {"risk_tolerance": 0.2}),
        ({"max_allocation": 0.3}, {"max_allocation": 0.3}),
        ({"risk_multiplier": 2.0}, {"risk_multiplier": 2.0}),
    ],
)
def test_risk_endpoint_emits_event(monkeypatch, payload, expected):
    monkeypatch.delenv("RISK_TOLERANCE", raising=False)
    monkeypatch.delenv("MAX_ALLOCATION", raising=False)
    monkeypatch.delenv("RISK_MULTIPLIER", raising=False)
    events: list[dict] = []

    from solhunter_zero.event_bus import subscribe

    async def on_risk(p):
        events.append(p)

    unsub = subscribe("risk_updated", on_risk)

    client = ui.app.test_client()
    resp = client.post("/risk", json=payload)
    assert resp.get_json()["status"] == "ok"
    asyncio.run(asyncio.sleep(0))
    unsub()

    assert events == [expected]


def test_risk_params_rejects_non_numeric(monkeypatch):
    monkeypatch.delenv("RISK_TOLERANCE", raising=False)
    monkeypatch.delenv("MAX_ALLOCATION", raising=False)
    monkeypatch.delenv("RISK_MULTIPLIER", raising=False)

    client = ui.app.test_client()

    for key in ("risk_tolerance", "max_allocation", "risk_multiplier"):
        resp = client.post("/risk", json={key: "not-a-number"})
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "invalid numeric value"
    assert os.getenv("RISK_TOLERANCE") is None
    assert os.getenv("MAX_ALLOCATION") is None
    assert os.getenv("RISK_MULTIPLIER") is None


def test_risk_params_boundary_values(monkeypatch):
    client = ui.app.test_client()

    for val in (0, 1):
        monkeypatch.delenv("RISK_TOLERANCE", raising=False)
        resp = client.post("/risk", json={"risk_tolerance": val})
        assert resp.get_json()["status"] == "ok"
        assert os.getenv("RISK_TOLERANCE") == str(float(val))

    for val in (0, 1):
        monkeypatch.delenv("MAX_ALLOCATION", raising=False)
        resp = client.post("/risk", json={"max_allocation": val})
        assert resp.get_json()["status"] == "ok"
        assert os.getenv("MAX_ALLOCATION") == str(float(val))

    monkeypatch.delenv("RISK_MULTIPLIER", raising=False)
    resp = client.post("/risk", json={"risk_multiplier": 1})
    assert resp.get_json()["status"] == "ok"
    assert os.getenv("RISK_MULTIPLIER") == "1.0"


def test_risk_params_out_of_range(monkeypatch):
    client = ui.app.test_client()

    for val in (-0.1, 1.1):
        monkeypatch.delenv("RISK_TOLERANCE", raising=False)
        resp = client.post("/risk", json={"risk_tolerance": val})
        assert resp.status_code == 400
        assert (
            resp.get_json()["error"]
            == "risk_tolerance must be between 0 and 1"
        )
        assert os.getenv("RISK_TOLERANCE") is None

    for val in (-0.1, 1.1):
        monkeypatch.delenv("MAX_ALLOCATION", raising=False)
        resp = client.post("/risk", json={"max_allocation": val})
        assert resp.status_code == 400
        assert (
            resp.get_json()["error"]
            == "max_allocation must be between 0 and 1"
        )
        assert os.getenv("MAX_ALLOCATION") is None

    for val in (0, -1):
        monkeypatch.delenv("RISK_MULTIPLIER", raising=False)
        resp = client.post("/risk", json={"risk_multiplier": val})
        assert resp.status_code == 400
        assert (
            resp.get_json()["error"]
            == "risk_multiplier must be positive"
        )
        assert os.getenv("RISK_MULTIPLIER") is None


def test_get_and_set_discovery_method(monkeypatch):
    monkeypatch.delenv("DISCOVERY_METHOD", raising=False)
    client = ui.app.test_client()

    resp = client.get("/discovery")
    assert resp.get_json()["method"] == "helius"

    resp = client.post("/discovery", json={"method": "mempool"})
    assert resp.get_json()["status"] == "ok"
    assert os.getenv("DISCOVERY_METHOD") == "mempool"


def test_discovery_method_invalid_value(monkeypatch):
    monkeypatch.delenv("DISCOVERY_METHOD", raising=False)
    client = ui.app.test_client()
    resp = client.post("/discovery", json={"method": "invalid"})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "Invalid discovery method" in data["error"]
    assert os.getenv("DISCOVERY_METHOD") is None


def test_start_requires_env(monkeypatch):
    async def noop():
        pass

    monkeypatch.setattr(ui, "trading_loop", noop)
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
    monkeypatch.setattr(ui, "load_selected_config", lambda: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    monkeypatch.setattr(ui, "ensure_active_keypair", lambda: None)
    monkeypatch.setattr(ui, "ensure_active_config", lambda: None)
    monkeypatch.delenv("BIRDEYE_API_KEY", raising=False)
    monkeypatch.delenv("SOLANA_RPC_URL", raising=False)
    monkeypatch.delenv("DEX_BASE_URL", raising=False)
    ui.app = ui.create_app()
    client = ui.app.test_client()
    resp = client.post("/start")
    assert resp.status_code == 400
    msg = resp.get_json()["message"]
    assert "DEX_BASE_URL" in msg
    assert "BIRDEYE_API_KEY or SOLANA_RPC_URL" in msg


def test_upload_endpoints_prevent_traversal(monkeypatch, tmp_path):
    monkeypatch.setattr(ui.wallet, "KEYPAIR_DIR", str(tmp_path / "keys"))
    monkeypatch.setattr(
        ui.wallet, "ACTIVE_KEYPAIR_FILE", str(tmp_path / "keys" / "active")
    )
    monkeypatch.setattr(config, "CONFIG_DIR", str(tmp_path / "cfgs"))
    monkeypatch.setattr(
        config, "ACTIVE_CONFIG_FILE", str(tmp_path / "cfgs" / "active")
    )
    os.makedirs(ui.wallet.KEYPAIR_DIR, exist_ok=True)
    os.makedirs(config.CONFIG_DIR, exist_ok=True)

    client = ui.app.test_client()

    kp = Keypair()
    data = json.dumps(list(kp.to_bytes()))
    try:
        resp = client.post(
            "/keypairs/upload",
            data={
                "name": "../evil",
                "file": (io.BytesIO(data.encode()), "kp.json"),
            },
            content_type="multipart/form-data",
        )
    except TypeError:
        pytest.skip("test client lacks file upload support")
    assert resp.status_code == 400
    assert not list((tmp_path / "keys").glob("*.json"))

    resp = client.post(
        "/configs/upload",
        data={"name": "../cfg", "file": (io.BytesIO(b"x"), "c.toml")},
        content_type="multipart/form-data",
    )
    assert resp.status_code == 400
    assert not list((tmp_path / "cfgs").iterdir())


def test_start_auto_selects_single_keypair(monkeypatch, tmp_path):
    monkeypatch.setattr(ui.wallet, "KEYPAIR_DIR", str(tmp_path))
    monkeypatch.setattr(
        ui.wallet, "ACTIVE_KEYPAIR_FILE", str(tmp_path / "active")
    )
    os.makedirs(ui.wallet.KEYPAIR_DIR, exist_ok=True)

    kp = Keypair()
    (tmp_path / "only.json").write_text(json.dumps(list(kp.to_bytes())))

    async def noop():
        pass

    monkeypatch.setattr(ui, "trading_loop", noop)
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
    monkeypatch.setattr(ui, "load_selected_config", lambda: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    monkeypatch.setattr(ui, "ensure_active_config", lambda: None)
    monkeypatch.setenv("BIRDEYE_API_KEY", "x")
    monkeypatch.setenv("DEX_BASE_URL", "x")
    ui.app = ui.create_app()
    client = ui.app.test_client()
    resp = client.post("/start")
    assert resp.get_json()["status"] == "started"
    ui.trading_thread.join(timeout=1)
    assert (tmp_path / "active").read_text() == "only"


def test_get_and_set_weights(monkeypatch):
    monkeypatch.delenv("AGENT_WEIGHTS", raising=False)
    client = ui.app.test_client()

    resp = client.get("/weights")
    assert resp.get_json() == {}

    resp = client.post("/weights", json={"sim": 1.2})
    assert resp.get_json()["status"] == "ok"
    assert json.loads(os.getenv("AGENT_WEIGHTS"))["sim"] == 1.2


def test_rl_weights_event_updates_env(monkeypatch):
    monkeypatch.delenv("AGENT_WEIGHTS", raising=False)
    monkeypatch.delenv("RISK_MULTIPLIER", raising=False)

    ui._update_rl_weights(
        {"weights": {"x": 1.5}, "risk": {"risk_multiplier": 2.0}}
    )

    assert json.loads(os.getenv("AGENT_WEIGHTS"))["x"] == 1.5
    assert os.getenv("RISK_MULTIPLIER") == "2.0"


def test_logs_endpoint(monkeypatch):
    monkeypatch.setattr(ui, "log_buffer", deque(maxlen=5))
    ui.buffer_handler.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().setLevel(logging.INFO)

    logging.getLogger().info("alpha")
    logging.getLogger().error("beta")

    client = ui.app.test_client()
    resp = client.get("/logs")
    logs = resp.get_json()["logs"]

    assert any("alpha" in log for log in logs)
    assert logs[-1] == "beta"


def test_history_endpoint_truncates_results():
    history_entries = [{"iteration": i} for i in range(250)]
    state = ui.UIState(history_provider=lambda: history_entries)
    app = ui.create_app(state=state)
    client = app.test_client()

    resp = client.get("/history")
    data = resp.get_json()

    assert isinstance(data, list)
    assert len(data) == 200
    assert data[0]["iteration"] == 50
    assert data[-1]["iteration"] == 249


def test_index_json_includes_history_endpoint():
    app = ui.create_app()
    client = app.test_client()

    resp = client.get("/?format=json")
    payload = resp.get_json()

    assert "/history" in payload["endpoints"]


def test_autostart_env(monkeypatch):
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    monkeypatch.setenv("BIRDEYE_API_KEY", "x")
    monkeypatch.setenv("DEX_BASE_URL", "x")
    monkeypatch.setenv("AUTO_START", "1")

    called = []

    def fake_autostart():
        called.append(True)
        return {"status": "started"}

    monkeypatch.setattr(ui, "autostart", fake_autostart)

    app = ui.create_app()
    assert called

    client = app.test_client()
    resp = client.get("/logs")
    logs = resp.get_json()["logs"]
    assert any("Autostart triggered" in log for log in logs)


def test_token_history_endpoint(monkeypatch):
    pf = ui.Portfolio(path=None)
    pf.balances = {"tok": Position("tok", 1, 2.0)}
    monkeypatch.setattr(ui, "current_portfolio", pf, raising=False)
    monkeypatch.setattr(ui, "fetch_token_prices", lambda tokens: {"tok": 3.0})
    monkeypatch.setattr(ui, "pnl_history", [], raising=False)
    monkeypatch.setattr(ui, "token_pnl_history", {}, raising=False)
    monkeypatch.setattr(ui, "allocation_history", {}, raising=False)

    ui.record_history({"tok": 3.0})

    client = ui.app.test_client()
    resp = client.get("/token_history")
    data = resp.get_json()
    assert "tok" in data
    assert data["tok"]["pnl_history"][-1] == pytest.approx(1.0)
    assert data["tok"]["allocation_history"][-1] == pytest.approx(1.0)
    resp = client.get("/token_history")
    data2 = resp.get_json()
    assert len(data2["tok"]["pnl_history"]) == len(data["tok"]["pnl_history"])


def _setup_memory(monkeypatch):
    mem = ui.Memory("sqlite:///:memory:")
    monkeypatch.setattr(ui, "MEMORY", mem)

    orig_session = mem.Session

    def _sync_session():
        async_session = orig_session()

        class Wrapper:
            async def __aenter__(self_wr):
                self_wr._s = await async_session.__aenter__()
                return self_wr

            async def __aexit__(self_wr, exc_type, exc, tb):
                return await async_session.__aexit__(exc_type, exc, tb)

            def __enter__(self_wr):
                return asyncio.run(self_wr.__aenter__())

            def __exit__(self_wr, exc_type, exc, tb):
                return asyncio.run(self_wr.__aexit__(exc_type, exc, tb))

            def execute(self_wr, *a, **k):
                coro = self_wr._s.execute(*a, **k)
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    return asyncio.run(coro)
                else:
                    return coro

            def commit(self_wr):
                coro = self_wr._s.commit()
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    return asyncio.run(coro)
                else:
                    return coro

            def add(self_wr, obj):
                return self_wr._s.add(obj)

        return Wrapper()

    mem.Session = _sync_session

    orig_trade = mem.log_trade

    def _sync_log_trade(*a, **k):
        from solhunter_zero.util import run_coro

        return run_coro(orig_trade(*a, **k))

    mem.log_trade = _sync_log_trade

    orig_var = mem._log_var_async

    def _sync_log_var(value):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(orig_var(value))
        finally:
            loop.close()

    mem.log_var = _sync_log_var
    return mem


def test_memory_insert(monkeypatch):
    _setup_memory(monkeypatch)
    client = ui.app.test_client()
    resp = client.post(
        "/memory/insert",
        json={
            "sql": (
                "INSERT INTO trades(token,direction,amount,price) "
                "VALUES(:t,:d,:a,:p)"
            ),
            "params": {"t": "TOK", "d": "buy", "a": 1.0, "p": 2.0},
        },
    )
    assert resp.get_json()["status"] == "ok"
    resp = client.post(
        "/memory/query",
        json={"sql": "SELECT token FROM trades", "params": {}},
    )
    data = resp.get_json()
    assert len(data) == 1 and data[0]["token"] == "TOK"


def test_memory_update(monkeypatch):
    _setup_memory(monkeypatch)
    client = ui.app.test_client()
    client.post(
        "/memory/insert",
        json={
            "sql": (
                "INSERT INTO trades(token,direction,amount,price) "
                "VALUES(:t,:d,:a,:p)"
            ),
            "params": {"t": "TOK", "d": "buy", "a": 1.0, "p": 2.0},
        },
    )
    resp = client.post(
        "/memory/update",
        json={
            "sql": "UPDATE trades SET price=:p WHERE token=:t",
            "params": {"p": 3.0, "t": "TOK"},
        },
    )
    assert resp.get_json()["rows"] == 1
    resp = client.post(
        "/memory/query",
        json={
            "sql": "SELECT price FROM trades WHERE token=:t",
            "params": {"t": "TOK"},
        },
    )
    assert resp.get_json()[0]["price"] == 3.0


def test_memory_query(monkeypatch):
    _setup_memory(monkeypatch)
    client = ui.app.test_client()
    client.post(
        "/memory/insert",
        json={
            "sql": (
                "INSERT INTO trades(token,direction,amount,price) "
                "VALUES(:t,:d,:a,:p)"
            ),
            "params": {"t": "TOK", "d": "buy", "a": 1.0, "p": 2.0},
        },
    )
    resp = client.post(
        "/memory/query",
        json={
            "sql": "SELECT token, price FROM trades WHERE token=:t",
            "params": {"t": "TOK"},
        },
    )
    data = resp.get_json()
    assert data == [{"token": "TOK", "price": 2.0}]


def test_memory_rejects_disallowed_sql(monkeypatch):
    _setup_memory(monkeypatch)
    client = ui.app.test_client()
    resp = client.post("/memory/insert", json={"sql": "SELECT * FROM trades"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "disallowed sql"


def test_memory_sql_validation(monkeypatch):
    _setup_memory(monkeypatch)
    client = ui.app.test_client()
    # comments should be ignored
    resp = client.post("/memory/query", json={"sql": "-- comment\nSELECT 1"})
    assert resp.status_code == 200
    # multiple statements are rejected
    resp = client.post("/memory/query", json={"sql": "SELECT 1; SELECT 2"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "disallowed sql"
    # disallowed command is rejected
    resp = client.post("/memory/query", json={"sql": "DROP TABLE trades"})
    assert resp.status_code == 400
    assert resp.get_json()["error"] == "disallowed sql"


def test_memory_requires_auth(monkeypatch):
    _setup_memory(monkeypatch)
    monkeypatch.setenv("UI_API_TOKEN", "token")
    client = ui.app.test_client()
    # missing token
    ui.request.headers = {}
    resp = client.post("/memory/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 401
    # wrong token
    ui.request.headers = {"Authorization": "Bearer wrong"}
    resp = client.post("/memory/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 401
    # correct token
    ui.request.headers = {"Authorization": "Bearer token"}
    resp = client.post("/memory/query", json={"sql": "SELECT 1"})
    assert resp.status_code == 200


def test_vars_endpoint(monkeypatch):
    mem = _setup_memory(monkeypatch)
    mem.log_var(0.1)
    mem.log_var(0.2)
    client = ui.app.test_client()
    resp = client.get("/vars")
    data = resp.get_json()
    assert [v["value"] for v in data] == [0.1, 0.2]


def test_rl_status_endpoint(monkeypatch):
    daemon = type(
        "D", (), {"last_train_time": 1.0, "checkpoint_path": "chk.pt"}
    )()
    monkeypatch.setattr(ui, "rl_daemon", daemon, raising=False)
    client = ui.app.test_client()
    resp = client.get("/rl/status")
    data = resp.get_json()
    assert data["last_train_time"] == 1.0
    assert data["checkpoint_path"] == "chk.pt"


def test_status_endpoint(monkeypatch):
    class DummyThread:
        def is_alive(self):
            return True

    monkeypatch.setattr(ui, "trading_thread", DummyThread(), raising=False)

    class DummyDaemon:
        def is_alive(self):
            return True

    monkeypatch.setattr(ui, "rl_daemon", DummyDaemon(), raising=False)
    monkeypatch.setattr(ui, "depth_service_connected", True, raising=False)

    called = {}

    class Dummy:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    def fake_ws(url, *a, **k):
        called["url"] = url

        class Conn:
            async def __aenter__(self_inner):
                return Dummy()

            async def __aexit__(self_inner, exc_type, exc, tb):
                pass

            def __await__(self_inner):
                async def _coro():
                    return self_inner

                return _coro().__await__()

        return Conn()

    monkeypatch.setattr(ui.websockets, "connect", fake_ws)
    monkeypatch.setattr(ui, "get_event_bus_url", lambda *_: "ws://bus")

    client = ui.app.test_client()
    resp = client.get("/status")
    data = resp.get_json()
    assert data == {
        "trading_loop": True,
        "rl_daemon": True,
        "depth_service": True,
        "event_bus": True,
        "heartbeat": False,
        "system_metrics": {"cpu": 0.0, "memory": 0.0},
    }
    assert called["url"] == "ws://bus"


def test_status_endpoint_requires_heartbeat_or_alive(monkeypatch):
    class DummyThread:
        def is_alive(self):
            return True

    monkeypatch.setattr(ui, "trading_thread", DummyThread(), raising=False)
    monkeypatch.setattr(ui, "rl_daemon", object(), raising=False)
    monkeypatch.setattr(ui, "rl_daemon_heartbeat", 0.0, raising=False)
    monkeypatch.setattr(ui, "depth_service_connected", True, raising=False)

    called = {}

    class Dummy:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    def fake_ws(url, *a, **k):
        called["url"] = url

        class Conn:
            async def __aenter__(self_inner):
                return Dummy()

            async def __aexit__(self_inner, exc_type, exc, tb):
                pass

            def __await__(self_inner):
                async def _coro():
                    return self_inner

                return _coro().__await__()

        return Conn()

    monkeypatch.setattr(ui.websockets, "connect", fake_ws)
    monkeypatch.setattr(ui, "get_event_bus_url", lambda *_: "ws://bus")

    client = ui.app.test_client()
    resp = client.get("/status")
    data = resp.get_json()
    assert data["rl_daemon"] is False
    assert called["url"] == "ws://bus"


def test_status_endpoint_with_message(monkeypatch):
    class DummyThread:
        def is_alive(self):
            return True

    monkeypatch.setattr(ui, "trading_thread", DummyThread(), raising=False)

    class DummyDaemon:
        def is_alive(self):
            return True

    monkeypatch.setattr(ui, "rl_daemon", DummyDaemon(), raising=False)
    monkeypatch.setattr(ui, "depth_service_connected", True, raising=False)
    monkeypatch.setattr(ui, "startup_message", "hi", raising=False)

    called = {}

    class Dummy:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    def fake_ws(url, *a, **k):
        called["url"] = url

        class Conn:
            async def __aenter__(self_inner):
                return Dummy()

            async def __aexit__(self_inner, exc_type, exc, tb):
                pass

            def __await__(self_inner):
                async def _coro():
                    return self_inner

                return _coro().__await__()

        return Conn()

    monkeypatch.setattr(ui.websockets, "connect", fake_ws)
    monkeypatch.setattr(ui, "get_event_bus_url", lambda *_: "ws://bus")
    monkeypatch.setattr(ui.request, "args", {"include_message": "1"}, raising=False)

    client = ui.app.test_client()
    resp = client.get("/status")
    data = resp.get_json()
    assert data == {
        "trading_loop": True,
        "rl_daemon": True,
        "depth_service": True,
        "event_bus": True,
        "heartbeat": False,
        "system_metrics": {"cpu": 0.0, "memory": 0.0},
        "message": "hi",
    }
    assert called["url"] == "ws://bus"


def test_autostart(monkeypatch):
    ui.trading_thread = None
    events = []

    done = threading.Event()

    def fake_run_auto():
        events.append("run")
        done.wait()

    monkeypatch.setattr(ui.main_module, "run_auto", fake_run_auto)
    monkeypatch.setattr(ui, "ensure_active_keypair", lambda: None)
    monkeypatch.setattr(ui, "ensure_active_config", lambda: None)
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
    monkeypatch.setattr(ui, "apply_env_overrides", lambda c: c)
    monkeypatch.setattr(ui, "set_env_from_config", lambda c: None)
    monkeypatch.setattr(ui, "initialize_event_bus", lambda: None)
    monkeypatch.setattr(ui, "_check_redis_connection", lambda: None)
    monkeypatch.setattr(ui, "_event_bus_initialized", False)
    ui.app = ui.create_app()
    client = ui.app.test_client()
    resp = client.post("/autostart")
    assert resp.get_json()["status"] == "started"
    assert "run" in events
    resp = client.post("/autostart")
    assert resp.get_json()["status"] == "already running"
    done.set()
    ui.trading_thread.join(timeout=1)


@pytest.mark.parametrize("bad_name", ["../evil", "dir/evil", "dir\\evil"])
def test_upload_keypair_rejects_bad_name(monkeypatch, bad_name):
    called: list[str] = []
    monkeypatch.setattr(
        ui.wallet,
        "save_keypair",
        lambda *a, **k: called.append("called"),
    )

    class Dummy:
        def __init__(self, data: bytes, filename: str):
            self._data = data
            self.filename = filename

        def read(self) -> bytes:
            return self._data

    ui.request.files = {"file": Dummy(b"[]", "kp.json")}
    ui.request.form = {"name": bad_name}
    resp = ui.upload_keypair()
    assert resp[1] == 400
    assert not called
    ui.request.files = {}
    ui.request.form = {}


@pytest.mark.parametrize("bad_name", ["../evil", "dir/evil", "dir\\evil"])
def test_upload_config_rejects_bad_name(monkeypatch, bad_name):
    called: list[str] = []
    monkeypatch.setattr(
        ui,
        "save_config",
        lambda *a, **k: called.append("called"),
    )

    class Dummy:
        def __init__(self, data: bytes, filename: str):
            self._data = data
            self.filename = filename

        def read(self) -> bytes:
            return self._data

    ui.request.files = {"file": Dummy(b"a=1", "cfg.toml")}
    ui.request.form = {"name": bad_name}
    resp = ui.upload_config()
    assert resp[1] == 400
    assert not called
    ui.request.files = {}
    ui.request.form = {}


def _allocate_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _format_host_for_url(host: str) -> str:
    return host if ":" not in host or host.startswith("[") else f"[{host}]"


def _wait_for_status(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    formatted_host = _format_host_for_url(host)
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://{formatted_host}:{port}/status", timeout=0.5
            ) as resp:
                if getattr(resp, "status", 200) == 200:
                    return
        except Exception as exc:  # pragma: no cover - transient startup errors
            last_error = exc
            time.sleep(0.05)
        else:
            time.sleep(0.05)
    raise AssertionError(f"UI server did not start: {last_error}")


def test_ui_server_start_propagates_port_in_use() -> None:
    busy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        busy_sock.bind(("127.0.0.1", 0))
        busy_sock.listen(1)
        port = busy_sock.getsockname()[1]
        server = ui.UIServer(ui.UIState(), host="127.0.0.1", port=port)

        with pytest.raises(ui.UIStartupError):
            server.start()

        assert server._thread is None
        assert server._server is None
    finally:
        busy_sock.close()


def test_ui_server_start_surfaces_make_server_failure(monkeypatch) -> None:
    class BoomError(RuntimeError):
        pass

    import werkzeug.serving

    def _raise(*args, **kwargs):
        raise BoomError("boom")

    monkeypatch.setattr(werkzeug.serving, "make_server", _raise)

    server = ui.UIServer(ui.UIState(), host="127.0.0.1", port=0)

    with pytest.raises(ui.UIStartupError) as excinfo:
        server.start()

    assert "failed to bind" in str(excinfo.value)
    assert server._thread is None
    assert server._server is None


@pytest.mark.parametrize(
    "host, probe_host",
    [("127.0.0.1", "127.0.0.1"), ("0.0.0.0", "127.0.0.1")],
)
def test_ui_server_stop_handles_loopback(host: str, probe_host: str) -> None:
    port = _allocate_port()
    server = ui.UIServer(ui.UIState(), host=host, port=port)
    thread = None
    try:
        server.start()
        _wait_for_status(probe_host, server.port)
        thread = server._thread
        assert thread is not None and thread.is_alive()
    finally:
        server.stop()
    assert server._thread is None
    if thread is not None:
        assert not thread.is_alive()


def test_ui_server_shutdown_requires_token() -> None:
    port = _allocate_port()
    server = ui.UIServer(ui.UIState(), host="127.0.0.1", port=port)
    try:
        server.start()
        _wait_for_status("127.0.0.1", server.port)
        formatted_host = _format_host_for_url(server._resolve_shutdown_host())
        shutdown_url = f"http://{formatted_host}:{server.port}/__shutdown__"

        with pytest.raises(urllib.error.HTTPError) as excinfo:
            urllib.request.urlopen(shutdown_url, timeout=1)
        assert excinfo.value.code == 403
        excinfo.value.close()

        thread = server._thread
        assert thread is not None and thread.is_alive()

        request = urllib.request.Request(
            shutdown_url,
            headers={ui.SHUTDOWN_TOKEN_HEADER: server._shutdown_token},
        )
        with urllib.request.urlopen(request, timeout=1):
            pass

        deadline = time.time() + 2
        while thread.is_alive() and time.time() < deadline:
            time.sleep(0.05)
    finally:
        server.stop()
    assert server._thread is None


def test_ui_server_stop_fallback(monkeypatch) -> None:
    port = _allocate_port()
    server = ui.UIServer(ui.UIState(), host="127.0.0.1", port=port)
    shutdown_called = False
    try:
        server.start()
        _wait_for_status("127.0.0.1", server.port)
        base_server = server._server
        assert base_server is not None

        original_shutdown = base_server.shutdown

        def _record_shutdown(*args, **kwargs):
            nonlocal shutdown_called
            shutdown_called = True
            return original_shutdown(*args, **kwargs)

        monkeypatch.setattr(base_server, "shutdown", _record_shutdown)

        def _raise(*args, **kwargs):
            raise urllib.error.URLError("boom")

        monkeypatch.setattr(urllib.request, "urlopen", _raise)
    finally:
        server.stop()
    assert shutdown_called
    assert server._thread is None
