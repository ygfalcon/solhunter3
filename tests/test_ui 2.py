import os
import io
import json
import asyncio
import types
import contextlib
import importlib.machinery
import sys
import pytest
from solders.keypair import Keypair
import solhunter_zero.config as config
config.initialize_event_bus = lambda: None
import solhunter_zero.ui as ui
from collections import deque
from solhunter_zero.portfolio import Position
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
    monkeypatch.setattr(ui, "Memory", lambda *a, **k: object())
    monkeypatch.setattr(ui, "Portfolio", lambda *a, **k: object())
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
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
    monkeypatch.setattr(ui, "Memory", lambda *a, **k: object())
    monkeypatch.setattr(ui, "Portfolio", lambda *a, **k: object())
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
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


def test_risk_endpoint_emits_event(monkeypatch):
    monkeypatch.delenv("RISK_MULTIPLIER", raising=False)
    events = []

    from solhunter_zero.event_bus import subscribe

    async def on_risk(payload):
        events.append(payload)

    unsub = subscribe("risk_updated", on_risk)

    client = ui.app.test_client()
    resp = client.post("/risk", json={"risk_multiplier": 2.0})
    assert resp.get_json()["status"] == "ok"
    asyncio.run(asyncio.sleep(0))
    unsub()

    assert events and events[0]["multiplier"] == 2.0


def test_get_and_set_discovery_method(monkeypatch):
    monkeypatch.delenv("DISCOVERY_METHOD", raising=False)
    client = ui.app.test_client()

    resp = client.get("/discovery")
    assert resp.get_json()["method"] == "websocket"

    resp = client.post("/discovery", json={"method": "mempool"})
    assert resp.get_json()["status"] == "ok"
    assert os.getenv("DISCOVERY_METHOD") == "mempool"


def test_start_requires_env(monkeypatch):
    async def noop():
        pass

    monkeypatch.setattr(ui, "trading_loop", noop)
    monkeypatch.setattr(ui, "load_config", lambda p=None: {})
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
    resp = client.post(
        "/keypairs/upload",
        data={
            "name": "../evil",
            "file": (io.BytesIO(data.encode()), "kp.json"),
        },
        content_type="multipart/form-data",
    )
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

    from solhunter_zero.event_bus import publish
    from solhunter_zero.schemas import RLWeights

    publish(
        "rl_weights",
        RLWeights(weights={"x": 1.5}, risk={"risk_multiplier": 2.0}),
    )
    asyncio.run(asyncio.sleep(0))

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
    monkeypatch.setattr(ui, "token_pnl_history", {}, raising=False)
    monkeypatch.setattr(ui, "allocation_history", {}, raising=False)

    client = ui.app.test_client()
    resp = client.get("/token_history")
    data = resp.get_json()
    assert "tok" in data
    assert data["tok"]["pnl_history"][-1] == pytest.approx(1.0)
    assert data["tok"]["allocation_history"][-1] == pytest.approx(1.0)


def _setup_memory(monkeypatch):
    mem = ui.Memory("sqlite:///:memory:")
    monkeypatch.setattr(ui, "Memory", lambda *a, **k: mem)

    orig_session = mem.Session

    def _sync_session():
        async_session = orig_session()

        class Wrapper:
            def __enter__(self_wr):
                from solhunter_zero.util import run_coro

                return run_coro(async_session.__aenter__())

            def __exit__(self_wr, exc_type, exc, tb):
                from solhunter_zero.util import run_coro

                return run_coro(async_session.__aexit__(exc_type, exc, tb))

            def execute(self_wr, *a, **k):
                from solhunter_zero.util import run_coro

                return run_coro(async_session.execute(*a, **k))

            def commit(self_wr):
                from solhunter_zero.util import run_coro

                return run_coro(async_session.commit())

        return Wrapper()

    mem.Session = _sync_session

    orig = mem.log_trade

    def _sync_log_trade(*a, **k):
        from solhunter_zero.util import run_coro

        return run_coro(orig(*a, **k))

    mem.log_trade = _sync_log_trade
    return mem


def test_memory_insert(monkeypatch):
    mem = _setup_memory(monkeypatch)
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
    trades = asyncio.run(mem.list_trades())
    assert len(trades) == 1 and trades[0].token == "TOK"


def test_memory_update(monkeypatch):
    mem = _setup_memory(monkeypatch)
    mem.log_trade(token="TOK", direction="buy", amount=1.0, price=2.0)
    client = ui.app.test_client()
    resp = client.post(
        "/memory/update",
        json={
            "sql": "UPDATE trades SET price=:p WHERE token=:t",
            "params": {"p": 3.0, "t": "TOK"},
        },
    )
    assert resp.get_json()["rows"] == 1
    assert asyncio.run(mem.list_trades())[0].price == 3.0


def test_memory_query(monkeypatch):
    mem = _setup_memory(monkeypatch)
    mem.log_trade(token="TOK", direction="buy", amount=1.0, price=2.0)
    client = ui.app.test_client()
    resp = client.post(
        "/memory/query",
        json={
            "sql": "SELECT token, price FROM trades WHERE token=:t",
            "params": {"t": "TOK"},
        },
    )
    data = resp.get_json()
    assert data == [{"token": "TOK", "price": 2.0}]


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
    monkeypatch.setattr(ui, "rl_daemon", object(), raising=False)
    monkeypatch.setattr(ui, "depth_service_connected", True, raising=False)

    called = {}

    class Dummy:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    def fake_ws(url):
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
    ui.app = ui.create_app()
    client = ui.app.test_client()
    resp = client.post("/autostart")
    assert resp.get_json()["status"] == "started"
    assert "run" in events
    resp = client.post("/autostart")
    assert resp.get_json()["status"] == "already running"
    done.set()
    ui.trading_thread.join(timeout=1)
