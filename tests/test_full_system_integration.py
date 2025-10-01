import sys
import types
import contextlib
from pathlib import Path
import importlib.machinery
import importlib.util
import asyncio
import pytest
from solhunter_zero import main as main_module
from solhunter_zero.simulation import SimulationResult
from solhunter_zero.rl_daemon import RLDaemon
from solhunter_zero.agents.dqn import DQNAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.offline_data import OfflineData

pytest.importorskip("torch.nn.utils.rnn")


if importlib.util.find_spec("solders") is None:
    s_mod = types.ModuleType("solders")
    s_mod.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    sys.modules.setdefault("solders", s_mod)

    class _KP:
        def __init__(self, b=None):
            self._b = b or b""

        def to_bytes(self):
            return self._b

        @classmethod
        def from_bytes(cls, b):
            return cls(b)

    sys.modules["solders.keypair"] = types.SimpleNamespace(Keypair=_KP)
    dummy = types.SimpleNamespace()
    sys.modules["solders.pubkey"] = types.SimpleNamespace(Pubkey=object)
    sys.modules["solders.hash"] = types.SimpleNamespace(Hash=object)
    sys.modules["solders.message"] = types.SimpleNamespace(MessageV0=object)
    sys.modules["solders.transaction"] = types.SimpleNamespace(
        VersionedTransaction=object
    )
    sys.modules["solders.instruction"] = types.SimpleNamespace(
        Instruction=object, AccountMeta=object
    )
    sys.modules["solders.signature"] = types.SimpleNamespace(Signature=object)

if importlib.util.find_spec("psutil") is None:
    psutil_mod = types.ModuleType("psutil")
    psutil_mod.cpu_percent = lambda *a, **k: 0.0
    sys.modules["psutil"] = psutil_mod


pb_stub = types.ModuleType("event_pb2")
for _name in [
    "ActionExecuted",
    "WeightsUpdated",
    "RLWeights",
    "RLCheckpoint",
    "PortfolioUpdated",
    "DepthUpdate",
    "DepthServiceStatus",
    "Heartbeat",
    "TradeLogged",
    "RLMetrics",
    "SystemMetrics",
    "PriceUpdate",
    "ConfigUpdated",
    "PendingSwap",
    "RemoteSystemMetrics",
    "RiskMetrics",
    "RiskUpdated",
    "SystemMetricsCombined",
    "TokenDiscovered",
    "Event",
]:
    setattr(pb_stub, _name, type(_name, (), {}))
sys.modules["solhunter_zero.event_pb2"] = pb_stub

if importlib.util.find_spec("aiofiles") is None:
    aiofiles_mod = types.ModuleType("aiofiles")
    aiofiles_mod.__spec__ = importlib.machinery.ModuleSpec("aiofiles", None)

    class _FakeFile:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def write(self, data):
            pass

    aiofiles_mod.open = lambda *a, **k: _FakeFile()
    sys.modules["aiofiles"] = aiofiles_mod

if importlib.util.find_spec("bip_utils") is None:
    bip_mod = types.ModuleType("bip_utils")
    bip_mod.Bip39SeedGenerator = object
    bip_mod.Bip44 = object
    bip_mod.Bip44Coins = object
    bip_mod.Bip44Changes = object
    sys.modules["bip_utils"] = bip_mod

if importlib.util.find_spec("cachetools") is None:
    cache_mod = types.ModuleType("cachetools")

    class _DummyCache(dict):
        def __init__(self, *a, **k):
            pass

    cache_mod.LRUCache = _DummyCache
    cache_mod.TTLCache = _DummyCache
    sys.modules["cachetools"] = cache_mod

if importlib.util.find_spec("solana") is None:
    sol_mod = types.ModuleType("solana")
    sol_mod.__spec__ = importlib.machinery.ModuleSpec("solana", None)
    rpc_mod = types.ModuleType("rpc")

    class _AsyncClient:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_fees(self):
            return {}

        async def get_program_accounts(self, *a, **k):
            return {}

    rpc_mod.api = types.SimpleNamespace(
        Client=object, AsyncClient=_AsyncClient
    )
    rpc_mod.websocket_api = types.SimpleNamespace(
        connect=lambda *a, **k: None, RpcTransactionLogsFilterMentions=object
    )
    sys.modules["solana"] = sol_mod
    sys.modules["solana.rpc"] = rpc_mod
    sys.modules["solana.rpc.api"] = rpc_mod.api
    sys.modules["solana.rpc.async_api"] = rpc_mod.api
    sys.modules["solana.rpc.websocket_api"] = rpc_mod.websocket_api
else:
    import solana.rpc.websocket_api as ws_mod

    ws_mod.connect = lambda *a, **k: None
    if not hasattr(ws_mod, "RpcTransactionLogsFilterMentions"):
        ws_mod.RpcTransactionLogsFilterMentions = object

if importlib.util.find_spec("numpy") is None:
    np_mod = types.ModuleType("numpy")
    np_mod.array = lambda *a, **k: []
    np_mod.asarray = lambda *a, **k: []
    np_mod.zeros = lambda *a, **k: []
    np_mod.ones = lambda *a, **k: []
    sys.modules["numpy"] = np_mod

if importlib.util.find_spec("sqlalchemy") is None:
    sa_mod = types.ModuleType("sqlalchemy")
    sa_mod.create_engine = lambda *a, **k: None
    sa_mod.Column = lambda *a, **k: None
    sa_mod.String = object
    sa_mod.Integer = object
    sa_mod.Float = object
    sa_mod.Boolean = object
    sa_mod.Text = object
    sa_mod.DateTime = object
    sa_mod.select = lambda *a, **k: None
    sa_mod.ForeignKey = lambda *a, **k: None
    sa_mod.MetaData = lambda: None
    sa_mod.Table = lambda *a, **k: None
    sa_mod.Index = lambda *a, **k: None
    sa_mod.engine = types.SimpleNamespace(create_engine=lambda *a, **k: None)
    orm = types.ModuleType("orm")
    orm.sessionmaker = lambda *a, **k: lambda **kw: types.SimpleNamespace(
        commit=lambda: None, add=lambda *a, **k: None, query=lambda *a, **k: []
    )
    orm.declarative_base = lambda *a, **k: type("Base", (), {})
    sa_mod.orm = orm
    ext = types.ModuleType("ext")
    ext.asyncio = types.SimpleNamespace(
        create_async_engine=lambda *a, **k: None
    )
    ext.asyncio.async_sessionmaker = lambda *a, **k: None
    ext.asyncio.AsyncSession = type("AsyncSession", (), {})
    sa_mod.ext = ext
    sys.modules["sqlalchemy"] = sa_mod
    sys.modules["sqlalchemy.orm"] = orm
    sys.modules["sqlalchemy.ext"] = ext
    sys.modules["sqlalchemy.ext.asyncio"] = ext.asyncio

if importlib.util.find_spec("watchfiles") is None:
    wf_mod = types.ModuleType("watchfiles")
    wf_mod.awatch = lambda *a, **k: None
    sys.modules["watchfiles"] = wf_mod


# Stub heavy optional dependencies
_faiss_mod = types.ModuleType("faiss")
_faiss_mod.__spec__ = importlib.machinery.ModuleSpec("faiss", None)
sys.modules.setdefault("faiss", _faiss_mod)
_st_mod = types.ModuleType("sentence_transformers")
_st_mod.__spec__ = importlib.machinery.ModuleSpec(
    "sentence_transformers", None
)
sys.modules.setdefault("sentence_transformers", _st_mod)
sys.modules["sentence_transformers"].SentenceTransformer = (
    lambda *a, **k: types.SimpleNamespace(
        get_sentence_embedding_dimension=lambda: 1, encode=lambda x: []
    )
)

sklearn = types.ModuleType("sklearn")
sklearn.__spec__ = importlib.machinery.ModuleSpec("sklearn", None)
sys.modules.setdefault("sklearn", sklearn)
sys.modules["sklearn.linear_model"] = types.SimpleNamespace(
    LinearRegression=object
)
sys.modules["sklearn.ensemble"] = types.SimpleNamespace(
    GradientBoostingRegressor=object, RandomForestRegressor=object
)
sys.modules["sklearn.cluster"] = types.SimpleNamespace(
    KMeans=object, DBSCAN=object
)
_xgb_mod = types.ModuleType("xgboost")
_xgb_mod.__spec__ = importlib.machinery.ModuleSpec("xgboost", None)
_xgb_mod.XGBRegressor = object
sys.modules["xgboost"] = _xgb_mod

if importlib.util.find_spec("torch") is None:
    torch_mod = types.ModuleType("torch")
    torch_mod.__spec__ = importlib.machinery.ModuleSpec("torch", None)
    torch_mod.no_grad = contextlib.nullcontext
    torch_mod.tensor = lambda *a, **k: None
    torch_mod.nn = types.SimpleNamespace(
        Module=object,
        LSTM=object,
        Linear=object,
        TransformerEncoder=object,
        TransformerEncoderLayer=object,
        Sequential=lambda *a, **k: None,
        ReLU=object,
    )
    torch_mod.optim = types.ModuleType("optim")
    torch_mod.optim.__spec__ = importlib.machinery.ModuleSpec("torch.optim", None)
    torch_mod.cuda = types.SimpleNamespace(is_available=lambda: False)
    torch_mod.save = lambda obj, path: Path(path).write_text("x")
    sys.modules["torch"] = torch_mod
    sys.modules["torch.nn"] = torch_mod.nn
    sys.modules["torch.optim"] = torch_mod.optim
    torch = torch_mod
else:  # pragma: no cover - real dependency
    import torch  # type: ignore

sys.modules["solhunter_zero.models"] = types.SimpleNamespace(
    get_model=lambda *a, **k: None,
    load_compiled_model=lambda *a, **k: None,
    export_torchscript=lambda *a, **k: None,
)
_trans_mod = types.ModuleType("transformers")
_trans_mod.__spec__ = importlib.machinery.ModuleSpec("transformers", None)
_trans_mod.pipeline = lambda *a, **k: lambda *x, **y: None
sys.modules.setdefault("transformers", _trans_mod)
sys.modules["transformers"].pipeline = _trans_mod.pipeline


@pytest.mark.parametrize("mode", ["auto"])
def test_trading_workflow(monkeypatch, tmp_path, mode):
    repo_root = Path(__file__).resolve().parents[1]
    cfg_path = repo_root / "config" / "default.toml"
    key_path = repo_root / "keypairs" / "default.json"

    monkeypatch.setenv("SOLHUNTER_CONFIG", str(cfg_path))
    monkeypatch.setenv("KEYPAIR_PATH", str(key_path))
    monkeypatch.setenv("AGENTS", "")
    monkeypatch.setenv("USE_DEPTH_STREAM", "0")

    monkeypatch.setattr(main_module, "bootstrap", lambda *a, **k: None)

    async def fake_discover(self, **_):
        return ["TOK"]

    import solhunter_zero.agents.discovery as discovery_mod
    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)

    class DummySM:
        def __init__(self, *a, **k):
            pass

        async def evaluate(self, token, portfolio):
            return [{"token": token, "side": "buy", "amount": 1, "price": 0}]

    monkeypatch.setattr(main_module, "StrategyManager", DummySM)

    start_calls = {"count": 0}
    stop_calls = {"count": 0}

    async def _start_ws_server(*_a, **_k):
        start_calls["count"] += 1

    async def _stop_ws_server(*_a, **_k):
        stop_calls["count"] += 1

    monkeypatch.setattr(main_module.event_bus, "start_ws_server", _start_ws_server)
    monkeypatch.setattr(main_module.event_bus, "stop_ws_server", _stop_ws_server)
    monkeypatch.setattr(main_module.event_bus, "publish", lambda *a, **k: None)
    monkeypatch.setattr(
        main_module.event_bus, "get_event_bus_url", lambda cfg=None: None, raising=False
    )

    order_call = {"responses": []}

    async def _fake_place_order(token, side, amount, price, **_k):
        response = {
            "order_id": "1",
            "token": token,
            "side": side,
            "amount": amount,
            "price": price,
        }
        order_call["responses"].append(response)
        return response

    import solhunter_zero.loop as loop_mod
    monkeypatch.setattr(main_module, "place_order_async", _fake_place_order)

    async def _fake_trading_loop(cfg, runtime_cfg, memory, portfolio, state, **kwargs):
        await main_module.event_bus.start_ws_server()
        await main_module.place_order_async("TOK", "buy", 1, 0)
        await memory.log_trade(token="TOK", direction="buy", amount=1, price=0)
        await portfolio.update_async("TOK", 1, 0)
        Path(portfolio.path).write_text(
            '{"TOK": {"amount": 1, "entry_price": 0, "high_price": 0}}'
        )
        await main_module.event_bus.stop_ws_server()

    monkeypatch.setattr(loop_mod, "trading_loop", _fake_trading_loop)
    monkeypatch.setattr(main_module, "trading_loop", _fake_trading_loop)

    mem_inst = {}

    class DummyMem:
        def __init__(self, url: str):
            self.trades = []
            mem_inst["obj"] = self

        async def wait_ready(self):
            return None

        async def log_trade(self, **kw):
            self.trades.append(types.SimpleNamespace(**kw))

        def list_trades(self):
            return self.trades

        def start_writer(self):
            return None

    monkeypatch.setattr(main_module, "Memory", DummyMem)
    monkeypatch.setattr(main_module, "ensure_connectivity", lambda **_: None)

    mem_path = tmp_path / "mem.db"
    pf_path = tmp_path / "pf.json"

    main_module.run_auto(
        memory_path=f"sqlite:///{mem_path}",
        portfolio_path=str(pf_path),
        loop_delay=0,
        iterations=1,
        dry_run=False,
    )

    assert start_calls["count"] > 0
    assert stop_calls["count"] > 0
    trade_resp = next(
        (r for r in order_call["responses"] if r["token"] == "TOK"),
        None,
    )
    assert trade_resp is not None and trade_resp["order_id"] == "1"
    assert trade_resp["side"] == "buy"

    mem = mem_inst.get("obj")
    trades = mem.list_trades() if mem else []
    assert len(trades) == 1
    assert trades[0].token == "TOK"
    assert trades[0].direction == "buy"
    assert trades[0].amount == 1
    assert trades[0].price == 0

    assert pf_path.exists()
    pf = main_module.Portfolio(path=str(pf_path))
    assert pf.balances["TOK"].amount > 0
    pf_path.unlink()
    assert not pf_path.exists()

