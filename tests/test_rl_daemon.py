import asyncio
import sys
import types
import importlib.util
import pytest
import subprocess

pytest.importorskip("google.protobuf")

# Stub heavy optional dependencies similar to event_bus tests
dummy_trans = types.ModuleType("transformers")
dummy_trans.pipeline = lambda *a, **k: lambda x: []
import importlib.machinery
dummy_trans.__spec__ = importlib.machinery.ModuleSpec("transformers", None)
if importlib.util.find_spec("transformers") is None:
    sys.modules.setdefault("transformers", dummy_trans)
sys.modules["transformers"].pipeline = dummy_trans.pipeline
if importlib.util.find_spec("sentence_transformers") is None:
    st_mod = types.ModuleType("sentence_transformers")
    st_mod.__spec__ = importlib.machinery.ModuleSpec("sentence_transformers", None)
    sys.modules.setdefault("sentence_transformers", st_mod)
if importlib.util.find_spec("faiss") is None:
    faiss_mod = types.ModuleType("faiss")
    faiss_mod.__spec__ = importlib.machinery.ModuleSpec("faiss", None)
    sys.modules.setdefault("faiss", faiss_mod)
if importlib.util.find_spec("torch") is None:
    torch_mod = types.ModuleType("torch")
    torch_mod.__spec__ = importlib.machinery.ModuleSpec("torch", None)
    torch_mod.__path__ = []
    torch_mod.Tensor = object
    torch_mod.cuda = types.SimpleNamespace(is_available=lambda: False)
    torch_mod.backends = types.SimpleNamespace(mps=types.SimpleNamespace(is_available=lambda: False))
    torch_mod.device = lambda *a, **k: types.SimpleNamespace(type="cpu")
    torch_mod.load = lambda *a, **k: {}
    torch_mod.float32 = object()
    class _NoGrad:
        def __enter__(self):
            pass
        def __exit__(self, *exc):
            pass
    torch_mod.no_grad = lambda: _NoGrad()
    torch_mod.tensor = lambda *a, **k: object()
    sys.modules.setdefault("torch", torch_mod)
    torch_nn = types.ModuleType("torch.nn")
    torch_nn.__spec__ = importlib.machinery.ModuleSpec("torch.nn", None)
    torch_nn.__path__ = []
    class _Module:
        def to(self, *a, **k):
            return self

        def load_state_dict(self, *a, **k):
            return None

        def parameters(self):
            return []

    torch_nn.Module = _Module

    def _module_factory(*a, **k):
        return _Module()

    torch_nn.Sequential = _module_factory
    torch_nn.Linear = _module_factory
    torch_nn.ReLU = _module_factory
    torch_nn.MSELoss = _module_factory
    sys.modules.setdefault("torch.nn", torch_nn)
    torch_opt = types.ModuleType("torch.optim")
    torch_opt.__spec__ = importlib.machinery.ModuleSpec("torch.optim", None)
    torch_opt.__path__ = []
    sys.modules.setdefault("torch.optim", torch_opt)
    torch_utils = types.ModuleType("torch.utils")
    torch_utils.__spec__ = importlib.machinery.ModuleSpec("torch.utils", None)
    torch_utils.__path__ = []
    sys.modules.setdefault("torch.utils", torch_utils)
    tud = types.ModuleType("torch.utils.data")
    tud.__spec__ = importlib.machinery.ModuleSpec("torch.utils.data", None)
    sys.modules.setdefault("torch.utils.data", tud)
    tud.Dataset = object
    tud.DataLoader = object
if importlib.util.find_spec("numpy") is None:
    np_mod = types.ModuleType("numpy")
    np_mod.__spec__ = importlib.machinery.ModuleSpec("numpy", None)
    sys.modules.setdefault("numpy", np_mod)
if importlib.util.find_spec("sklearn") is None:
    sk_mod = types.ModuleType("sklearn")
    sk_mod.__spec__ = importlib.machinery.ModuleSpec("sklearn", None)
    sys.modules.setdefault("sklearn", sk_mod)
    sys.modules["sklearn.linear_model"] = types.SimpleNamespace(LinearRegression=object)
    sys.modules["sklearn.ensemble"] = types.SimpleNamespace(
        GradientBoostingRegressor=object,
        RandomForestRegressor=object,
    )
    sys.modules["sklearn.cluster"] = types.SimpleNamespace(KMeans=object, DBSCAN=object)
    sys.modules["sklearn.gaussian_process"] = types.SimpleNamespace(GaussianProcessRegressor=object)
    sys.modules["sklearn.gaussian_process.kernels"] = types.SimpleNamespace(
        Matern=object,
        RBF=object,
        ConstantKernel=object,
        C=object,
    )
try:
    import google
except ModuleNotFoundError:
    google = types.ModuleType("google")
    google.__spec__ = importlib.machinery.ModuleSpec("google", None)
    google.__path__ = []
    sys.modules.setdefault("google", google)

if importlib.util.find_spec("google.protobuf") is None:
    protobuf = types.ModuleType("protobuf")
    protobuf.__spec__ = importlib.machinery.ModuleSpec("google.protobuf", None)
    descriptor = types.ModuleType("descriptor")
    descriptor.__spec__ = importlib.machinery.ModuleSpec("descriptor", None)
    descriptor_pool = types.ModuleType("descriptor_pool")
    descriptor_pool.__spec__ = importlib.machinery.ModuleSpec("descriptor_pool", None)
    symbol_database = types.ModuleType("symbol_database")
    symbol_database.__spec__ = importlib.machinery.ModuleSpec("symbol_database", None)
    symbol_database.Default = lambda: object()
    internal = types.ModuleType("internal")
    internal.__spec__ = importlib.machinery.ModuleSpec("internal", None)
    internal.builder = types.ModuleType("builder")
    internal.builder.__spec__ = importlib.machinery.ModuleSpec("builder", None)
    protobuf.descriptor = descriptor
    protobuf.descriptor_pool = descriptor_pool
    protobuf.symbol_database = symbol_database
    protobuf.internal = internal
    google.protobuf = protobuf
    sys.modules.setdefault("google.protobuf", protobuf)
    sys.modules.setdefault("google.protobuf.descriptor", descriptor)
    sys.modules.setdefault("google.protobuf.descriptor_pool", descriptor_pool)
    sys.modules.setdefault("google.protobuf.symbol_database", symbol_database)
    sys.modules.setdefault("google.protobuf.internal", internal)
    sys.modules.setdefault("google.protobuf.internal.builder", internal.builder)

if importlib.util.find_spec("pytorch_lightning") is None:
    pl = types.ModuleType("pytorch_lightning")
    pl.__spec__ = importlib.machinery.ModuleSpec("pytorch_lightning", None)
    callbacks = types.SimpleNamespace(Callback=object)
    pl.callbacks = callbacks
    pl.LightningModule = type("LightningModule", (), {})
    pl.LightningDataModule = type("LightningDataModule", (), {})
    pl.Trainer = type("Trainer", (), {"fit": lambda *a, **k: None})
    sys.modules.setdefault("pytorch_lightning", pl)
if importlib.util.find_spec("aiohttp") is None:
    aiohttp_mod = types.ModuleType("aiohttp")
    aiohttp_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp", None)
    sys.modules.setdefault("aiohttp", aiohttp_mod)
if importlib.util.find_spec("aiofiles") is None:
    aiof_mod = types.ModuleType("aiofiles")
    aiof_mod.__spec__ = importlib.machinery.ModuleSpec("aiofiles", None)
    sys.modules.setdefault("aiofiles", aiof_mod)
if importlib.util.find_spec("sqlalchemy") is None:
    sa = types.ModuleType("sqlalchemy")
    sa.__spec__ = importlib.machinery.ModuleSpec("sqlalchemy", None)
    sa.create_engine = lambda *a, **k: None
    sa.MetaData = type("Meta", (), {"create_all": lambda *a, **k: None})
    sa.Column = lambda *a, **k: None
    sa.String = sa.Integer = sa.Float = sa.Numeric = sa.Text = object
    sa.DateTime = object
    sa.LargeBinary = object
    sa.ForeignKey = lambda *a, **k: None
    sys.modules.setdefault("sqlalchemy", sa)
    orm = types.ModuleType("orm")
    orm.__spec__ = importlib.machinery.ModuleSpec("sqlalchemy.orm", None)

    def declarative_base(*a, **k):
        return type("Base", (), {"metadata": sa.MetaData()})

    orm.declarative_base = declarative_base

    class DummySession:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            pass

        def add(self, *a, **k):
            pass

        def commit(self):
            pass

        def query(self, *a, **k):
            return []

    orm.sessionmaker = lambda *a, **k: lambda **kw: DummySession()
    sys.modules.setdefault("sqlalchemy.orm", orm)
if importlib.util.find_spec("solders") is None:
    s_mod = types.ModuleType("solders")
    s_mod.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    sys.modules.setdefault("solders", s_mod)
    sys.modules["solders.keypair"] = types.SimpleNamespace(Keypair=type("Keypair", (), {}))
    sys.modules["solders.pubkey"] = types.SimpleNamespace(Pubkey=object)
    sys.modules["solders.hash"] = types.SimpleNamespace(Hash=object)
    sys.modules["solders.message"] = types.SimpleNamespace(MessageV0=object)
    sys.modules["solders.transaction"] = types.SimpleNamespace(VersionedTransaction=object)
if importlib.util.find_spec("solana") is None:
    sol_mod = types.ModuleType("solana")
    sol_mod.__spec__ = importlib.machinery.ModuleSpec("solana", None)
    sys.modules.setdefault("solana", sol_mod)
    sys.modules["solana.rpc"] = types.ModuleType("rpc")
    sys.modules["solana.rpc.api"] = types.SimpleNamespace(Client=object)
    sys.modules["solana.rpc.async_api"] = types.SimpleNamespace(AsyncClient=object)
    sys.modules["solana.rpc.websocket_api"] = types.SimpleNamespace(connect=lambda *a, **k: None)
    sys.modules["solana.rpc.websocket_api"].RpcTransactionLogsFilterMentions = object

from solhunter_zero.rl_daemon import RLDaemon
from solhunter_zero.agents.dqn import DQNAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.offline_data import OfflineData
from solhunter_zero.portfolio import Portfolio
from solhunter_zero.event_bus import (
    start_ws_server,
    stop_ws_server,
    connect_ws,
    disconnect_ws,
)
import solhunter_zero.event_bus as event_bus
import websockets
import json
import os
import logging


def _no_load(self):
    self._last_mtime = os.path.getmtime(self.model_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("algo", ["dqn", "a3c", "ddpg"])
async def test_rl_daemon_updates_and_agent_reloads(tmp_path, monkeypatch, caplog, algo):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    async def _stub_trade(self, **kw):
        return None

    monkeypatch.setattr(Memory, "log_trade", _stub_trade)
    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)
    await mem.log_trade(token='tok', direction='sell', amount=1, price=2)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, total_depth=1.5, imbalance=0.0)
    await data.log_snapshot('tok', 1.1, 1.0, total_depth=1.6, imbalance=0.0)

    model_path = tmp_path / 'model.pt'
    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=model_path,
        algo=algo,
        hierarchical_rl=False,
    )
    monkeypatch.setattr(DQNAgent, "reload_weights", _no_load)

    def _init(self, memory_agent=None, epsilon=0.0, model_path=None, **kwargs):
        self.model_path = model_path
        self._last_mtime = 0.0

    monkeypatch.setattr(DQNAgent, "__init__", _init)
    agent = DQNAgent(memory_agent=MemoryAgent(mem), epsilon=0.0, model_path=model_path)

    import solhunter_zero.rl_training as rl_training
    from pathlib import Path

    def fake_fit(*a, **k):
        Path(k.get("model_path")).write_text("x")

    monkeypatch.setattr(rl_training, "fit", fake_fit)
    import torch
    monkeypatch.setattr(torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(daemon.model, "load_state_dict", lambda *_: None)
    from types import SimpleNamespace
    dummy_trade = SimpleNamespace(direction='buy', amount=1, price=1, token='tok')
    dummy_snap = SimpleNamespace(token='tok', price=1.0, depth=1.0, timestamp=0)

    async def _fetch(self):
        return [dummy_trade], [dummy_snap]

    monkeypatch.setattr(RLDaemon, "_fetch_new", _fetch)
    daemon.register_agent(agent)
    first = agent._last_mtime
    with caplog.at_level(logging.INFO):
        await daemon.train()
    assert model_path.exists()
    assert agent._last_mtime != first
    assert any("checkpoint" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_queued_trades_trigger_update(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    queue: asyncio.Queue = asyncio.Queue()
    mem_agent = MemoryAgent(mem, offline_data=data, queue=queue)
    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'model.pt', algo='dqn', queue=queue)

    action = {'token': 'tok', 'side': 'buy', 'amount': 1.0, 'price': 1.0}
    await mem_agent.log(action, skip_db=True)

    await daemon.train()
    assert (tmp_path / 'model.pt').exists()


def test_daemon_receives_risk_update(tmp_path):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    data = OfflineData(data_db)

    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'model.pt')

    from solhunter_zero.event_bus import publish

    publish("risk_updated", {"multiplier": 3.0})
    asyncio.run(asyncio.sleep(0))

    assert daemon.current_risk == 3.0


@pytest.mark.asyncio
async def test_rl_checkpoint_event_emitted(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    events = []
    from solhunter_zero.event_bus import subscribe
    import solhunter_zero.rl_training as rl_training
    from pathlib import Path

    unsub = subscribe("rl_checkpoint", lambda p: events.append(p))

    def fake_fit(*a, **k):
        Path(k.get("model_path")).write_text("x")

    monkeypatch.setattr(rl_training, "fit", fake_fit)
    import torch
    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'model.pt')
    monkeypatch.setattr(torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(daemon.model, "load_state_dict", lambda *_: None)
    from types import SimpleNamespace
    dummy_trade = SimpleNamespace(direction='buy', amount=1, price=1, token='tok')
    dummy_snap = SimpleNamespace(token='tok', price=1.0, depth=1.0, timestamp=0)
    monkeypatch.setattr(RLDaemon, "_fetch_new", lambda self: ([dummy_trade], [dummy_snap]))
    await daemon.train()
    unsub()

    from solhunter_zero.schemas import RLCheckpoint

    assert events and isinstance(events[0], RLCheckpoint)
    assert events[0].path.endswith("model.pt")


@pytest.mark.asyncio
async def test_rl_weights_event_emitted(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    events = []
    from solhunter_zero.event_bus import subscribe
    import solhunter_zero.rl_training as rl_training
    from pathlib import Path

    unsub = subscribe("rl_weights", lambda p: events.append(p))

    def fake_fit(*a, **k):
        Path(k.get("model_path")).write_text("x")

    monkeypatch.setattr(rl_training, "fit", fake_fit)
    import torch
    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'model.pt')
    monkeypatch.setattr(torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(daemon.model, "load_state_dict", lambda *_: None)
    await daemon.train()
    unsub()

    from solhunter_zero.schemas import RLWeights

    assert events and isinstance(events[0], RLWeights)
    assert events[0].weights


@pytest.mark.asyncio
async def test_rl_metrics_event_emitted(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    events = []
    from solhunter_zero.event_bus import subscribe
    import solhunter_zero.rl_training as rl_training
    from pathlib import Path

    unsub = subscribe("rl_metrics", lambda p: events.append(p))

    def fake_fit(*a, **k):
        Path(k.get("model_path")).write_text("x")
        from solhunter_zero.event_bus import publish
        publish("rl_metrics", {"loss": 1.0, "reward": 0.5})

    monkeypatch.setattr(rl_training, "fit", fake_fit)
    import torch
    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'model.pt')
    monkeypatch.setattr(torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(daemon.model, "load_state_dict", lambda *_: None)
    await daemon.train()
    unsub()

    assert events and isinstance(events[0], dict)


@pytest.mark.asyncio
async def test_rl_metrics_via_external_ws(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    async def _noop_log_trade(self, **kw):
        return None

    monkeypatch.setattr(Memory, "log_trade", _noop_log_trade)
    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)

    async def _snap(*a, **k):
        pass
    monkeypatch.setattr(OfflineData, "log_snapshot", _snap)
    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    import solhunter_zero.rl_training as rl_training
    from pathlib import Path
    port = 8771
    await start_ws_server("localhost", port)
    await connect_ws(f"ws://localhost:{port}")

    def fake_fit(*a, **k):
        Path(k.get("model_path")).write_text("x")

    monkeypatch.setattr(rl_training, "fit", fake_fit)
    import torch
    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'model.pt')
    monkeypatch.setattr(torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(daemon.model, "load_state_dict", lambda *_: None)
    from types import SimpleNamespace
    dummy_trade = SimpleNamespace(direction='buy', amount=1, price=1, token='tok')
    dummy_snap = SimpleNamespace(token='tok', price=1.0, depth=1.0, timestamp=0)
    monkeypatch.setattr(RLDaemon, "_fetch_new", lambda self: ([dummy_trade], [dummy_snap]))

    from solhunter_zero import event_pb2
    async with websockets.connect(f"ws://localhost:{port}") as ws:
        await daemon.train()
        for _ in range(3):
            raw = await asyncio.wait_for(ws.recv(), timeout=1)
            if isinstance(raw, bytes):
                ev = event_pb2.Event()
                ev.ParseFromString(raw)
                if ev.topic == "rl_metrics":
                    assert ev.rl_metrics.reward != 0
                    break
        else:
            raise AssertionError("rl_metrics not received")

    await disconnect_ws()
    await stop_ws_server()


def test_distributed_rl_connects_broker(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    # avoid initializing heavy database layers

    called = {}

    async def fake_connect(urls):
        called['urls'] = urls

    monkeypatch.setattr(event_bus, 'connect_broker', fake_connect)
    import solhunter_zero.rl_daemon as rl_d
    monkeypatch.setattr(rl_d, 'connect_broker', fake_connect)
    monkeypatch.setenv('BROKER_URL', 'redis://localhost')

    RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'m.pt', distributed_rl=True)

    assert called.get('urls') == ['redis://localhost']


def test_predict_action_returns_vector(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'

    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=tmp_path/'m.pt')

    class DummyTensor:
        def __init__(self, arr):
            self.arr = arr
        def cpu(self):
            return self
        def squeeze(self):
            return self
        def tolist(self):
            return self.arr

    class DummyModel:
        def to(self, *a, **k):
            return self
        def eval(self):
            pass
        def __call__(self, x):
            return DummyTensor([0.1, -0.1])

    daemon.jit_model = DummyModel()
    pf = Portfolio(path=None)
    pf.record_prices({"tok": 1.0})
    pf.record_prices({"tok": 1.1})

    out = asyncio.run(daemon.predict_action(pf, "tok", 1.1))
    assert isinstance(out, list) and len(out) == 2



def test_daemon_loads_hierarchical_policy(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    # avoid dataset generation during init
    monkeypatch.setenv("RL_BUILD_MMAP_DATASET", "0")


    policy = tmp_path / 'hp.json'
    policy.write_text('[0.0, 2.0]')

    class DummyAgent:
        def __init__(self, name):
            self.name = name

    agents = [DummyAgent("a1"), DummyAgent("a2")]
    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=tmp_path/'m.pt',
        agents=agents,
        hierarchical_rl=True,
        hierarchical_model_path=policy,
    )
    assert daemon.hier_policy is not None
    assert daemon.hier_weights.get("a1") == 0.0
    assert daemon.hier_weights.get("a2") == 2.0


def test_hierarchical_policy_persists_weights(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    monkeypatch.setenv("RL_BUILD_MMAP_DATASET", "0")

    policy = tmp_path / 'hp.json'
    policy.write_text('[0.0, 2.0]')

    class DummyAgent:
        def __init__(self, name):
            self.name = name

    agents = [DummyAgent("a1"), DummyAgent("a2")]

    import solhunter_zero.rl_training as rl_training
    from pathlib import Path
    monkeypatch.setattr(
        rl_training,
        "fit",
        lambda *a, **k: (Path(k.get("model_path")).write_text("x")),
    )

    import solhunter_zero.event_bus as event_bus
    import solhunter_zero.rl_daemon as rl_d
    monkeypatch.setattr(event_bus, "publish", lambda *a, **k: None)
    monkeypatch.setattr(rl_d, "publish", lambda *a, **k: None)
    class DummyData:
        def __init__(self, *a, **k):
            pass
    monkeypatch.setattr(rl_d, "OfflineData", DummyData)

    import solhunter_zero.hierarchical_rl as hrl
    monkeypatch.setattr(hrl, "torch", None)
    monkeypatch.setattr(hrl, "nn", None)

    from types import SimpleNamespace
    trades = [
        SimpleNamespace(reason="a1", direction="buy", amount=1, price=1),
        SimpleNamespace(reason="a1", direction="sell", amount=1, price=2),
        SimpleNamespace(reason="a2", direction="buy", amount=1, price=1),
        SimpleNamespace(reason="a2", direction="sell", amount=1, price=1.5),
    ]
    snaps = [SimpleNamespace(token="tok", price=1.0, depth=1.0, timestamp=0)]
    monkeypatch.setattr(RLDaemon, "_fetch_new", lambda self: (trades, snaps))

    import torch
    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=tmp_path/'m.pt',
        agents=agents,
        hierarchical_rl=True,
        hierarchical_model_path=policy,
    )
    monkeypatch.setattr(torch, "load", lambda *a, **k: {})
    monkeypatch.setattr(daemon.model, "load_state_dict", lambda *_: None)

    asyncio.run(daemon.train())
    saved = dict(daemon.hier_weights)
    daemon.close()

    daemon2 = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=tmp_path/'m.pt',
        agents=agents,
        hierarchical_rl=True,
        hierarchical_model_path=policy,
    )
    assert daemon2.hier_weights == saved


def test_subprocess_terminated_on_close(tmp_path, monkeypatch):
    monkeypatch.setenv("RL_BUILD_MMAP_DATASET", "0")
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=tmp_path / 'm.pt',
    )
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    daemon._proc = proc
    try:
        daemon.close()
        assert proc.poll() is not None
        assert daemon._proc is None
    finally:
        if proc.poll() is None:
            proc.kill()
