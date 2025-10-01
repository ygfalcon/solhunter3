import sys
import types
import importlib.util
import importlib.machinery
from pathlib import Path
import asyncio
import pytest

pytest.importorskip("google.protobuf")

dummy_trans = types.ModuleType("transformers")
dummy_trans.pipeline = lambda *a, **k: lambda x: []
if importlib.util.find_spec("transformers") is None:
    dummy_trans.__spec__ = importlib.machinery.ModuleSpec("transformers", None)
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
    def _save(obj, path, *a, **k):
        Path(path).touch()
    torch_mod.save = _save
    torch_mod.tensor = lambda *a, **k: object()
    torch_mod.zeros = lambda *a, **k: object()
    torch_mod.long = int
    sys.modules.setdefault("torch", torch_mod)
    torch_nn = types.ModuleType("torch.nn")
    torch_nn.__spec__ = importlib.machinery.ModuleSpec("torch.nn", None)
    torch_nn.__path__ = []
    torch_nn.Module = type(
        "Module",
        (),
        {
            "to": lambda self, *a, **k: None,
            "load_state_dict": lambda self, *a, **k: None,
        },
    )
    torch_nn.Sequential = lambda *a, **k: object()
    torch_nn.Linear = lambda *a, **k: object()
    torch_nn.ReLU = lambda *a, **k: object()
    torch_nn.MSELoss = lambda *a, **k: object()
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
if importlib.util.find_spec("pytorch_lightning") is None:
    pl = types.ModuleType("pytorch_lightning")
    pl.__spec__ = importlib.machinery.ModuleSpec("pytorch_lightning", None)
    callbacks = types.SimpleNamespace(Callback=object)
    pl.callbacks = callbacks
    import torch.nn as _nn
    pl.LightningModule = type(
        "LightningModule",
        (_nn.Module,),
        {
            "save_hyperparameters": lambda self, *a, **k: None,
            "state_dict": lambda self: {},
        },
    )
    pl.LightningDataModule = type("LightningDataModule", (), {})
    pl.Trainer = type(
        "Trainer",
        (),
        {
            "__init__": lambda self, *a, **k: None,
            "fit": lambda *a, **k: None,
        },
    )
    sys.modules.setdefault("pytorch_lightning", pl)

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

event_pb2 = types.ModuleType("event_pb2")
for name in [
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
    "PriceUpdate",
    "SystemMetrics",
    "SystemMetricsCombined",
    "RiskMetrics",
    "RiskUpdated",
    "RemoteSystemMetrics",
    "TokenDiscovered",
    "MemorySyncRequest",
    "MemorySyncResponse",
    "PendingSwap",
    "ConfigUpdated",
    "Event",
]:
    setattr(event_pb2, name, object())
sys.modules.setdefault("solhunter_zero.event_pb2", event_pb2)

from solhunter_zero.rl_training import RLTraining
import solhunter_zero.rl_training as rl_training
from solhunter_zero.offline_data import OfflineData
from scripts import build_mmap_dataset


@pytest.mark.asyncio
async def test_rl_training_runs(tmp_path):
    db = f"sqlite:///{tmp_path/'data.db'}"
    data = OfflineData(db)
    await data.log_snapshot("tok", 1.0, 1.0, total_depth=1.5, imbalance=0.0, slippage=0.0, volume=0.0)
    await data.log_snapshot("tok", 1.1, 1.0, total_depth=1.6, imbalance=0.0, slippage=0.0, volume=0.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)
    await data.log_trade("tok", "sell", 1.0, 1.1)

    model_path = tmp_path / "ppo_model.pt"
    trainer = RLTraining(db_url=db, model_path=model_path)
    await trainer.train()
    assert model_path.exists()


def test_dynamic_worker_count(monkeypatch, tmp_path):
    import solhunter_zero.rl_training as rl_training

    class DummyDataset(rl_training.Dataset):
        def __len__(self):
            return 400

        def __getitem__(self, idx):
            return (
                rl_training.torch.zeros(10),
                rl_training.torch.tensor(0, dtype=rl_training.torch.long),
                rl_training.torch.tensor(0.0),
            )

    counts = []

    class DummyLoader:
        def __init__(self, *a, **kw):
            counts.append(kw.get("num_workers"))

    monkeypatch.setattr(rl_training, "DataLoader", DummyLoader)
    monkeypatch.setattr(rl_training, "_TradeDataset", lambda *a, **k: DummyDataset())
    monkeypatch.setattr(rl_training.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rl_training.torch, "save", lambda *a, **k: None)

    rl_training.fit(
        [],
        [],
        model_path=tmp_path / "m.pt",
        dynamic_workers=True,
        cpu_callback=lambda: 80.0,
    )
    high = counts[-1]
    rl_training.fit(
        [],
        [],
        model_path=tmp_path / "m.pt",
        dynamic_workers=True,
        cpu_callback=lambda: 10.0,
    )
    low = counts[-1]
    assert low > high


def test_workers_adjusted_each_epoch(monkeypatch):
    import solhunter_zero.rl_training as rl_training

    class DummyDataset(rl_training.Dataset):
        def __len__(self):
            return 400

        def __getitem__(self, idx):
            return (
                rl_training.torch.zeros(10),
                rl_training.torch.tensor(0, dtype=rl_training.torch.long),
                rl_training.torch.tensor(0.0),
            )

    class DummyLoader:
        def __init__(self, *a, **kw):
            self.num_workers = kw.get("num_workers")
            self.pin_memory = kw.get("pin_memory")
            self.persistent_workers = kw.get("persistent_workers")

    monkeypatch.setattr(rl_training, "DataLoader", lambda *a, **kw: DummyLoader(*a, **kw))
    monkeypatch.setattr(rl_training, "_TradeDataset", lambda *a, **k: DummyDataset())
    monkeypatch.setattr(rl_training.os, "cpu_count", lambda: 4)

    cpu_val = {"v": 80.0}
    dm = rl_training.TradeDataModule("db", dynamic_workers=True, cpu_callback=lambda: cpu_val["v"])
    dm.dataset = DummyDataset()
    loader = dm.train_dataloader()
    cb = rl_training._DynamicWorkersCallback()
    trainer = types.SimpleNamespace(datamodule=dm, train_dataloader=loader)

    cb.on_train_epoch_start(trainer, rl_training.LightningPPO())
    high = loader.num_workers
    cpu_val["v"] = 10.0
    cb.on_train_epoch_start(trainer, rl_training.LightningPPO())
    low = loader.num_workers
    assert low > high


def test_worker_count_reduced_when_memory_high(monkeypatch, tmp_path):
    import solhunter_zero.rl_training as rl_training

    class DummyDataset(rl_training.Dataset):
        def __len__(self):
            return 400

        def __getitem__(self, idx):
            return (
                rl_training.torch.zeros(10),
                rl_training.torch.tensor(0, dtype=rl_training.torch.long),
                rl_training.torch.tensor(0.0),
            )

    counts = []

    class DummyLoader:
        def __init__(self, *a, **kw):
            counts.append(kw.get("num_workers"))

    monkeypatch.setattr(rl_training, "DataLoader", DummyLoader)
    monkeypatch.setattr(rl_training, "_TradeDataset", lambda *a, **k: DummyDataset())
    monkeypatch.setattr(rl_training.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rl_training.torch, "save", lambda *a, **k: None)

    monkeypatch.setattr(
        rl_training.psutil,
        "virtual_memory",
        lambda: types.SimpleNamespace(percent=90.0),
    )
    rl_training.fit([], [], model_path=tmp_path / "m.pt")
    high = counts[-1]

    monkeypatch.setattr(
        rl_training.psutil,
        "virtual_memory",
        lambda: types.SimpleNamespace(percent=50.0),
    )
    rl_training.fit([], [], model_path=tmp_path / "m.pt")
    low = counts[-1]

    assert low > high


def test_worker_counts_update_on_metrics(monkeypatch, tmp_path):
    import solhunter_zero.rl_training as rl_training
    from solhunter_zero import event_bus

    class DummyDataset(rl_training.Dataset):
        def __len__(self):
            return 400

        def __getitem__(self, idx):
            return (
                rl_training.torch.zeros(10),
                rl_training.torch.tensor(0, dtype=rl_training.torch.long),
                rl_training.torch.tensor(0.0),
            )

    class DummyLoader:
        def __init__(self, *a, **kw):
            self.num_workers = kw.get("num_workers")
            self.pin_memory = kw.get("pin_memory")
            self.persistent_workers = kw.get("persistent_workers")

    monkeypatch.setattr(rl_training, "DataLoader", lambda *a, **kw: DummyLoader(*a, **kw))
    monkeypatch.setattr(rl_training, "_TradeDataset", lambda *a, **k: DummyDataset())
    monkeypatch.setattr(rl_training.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rl_training.torch, "save", lambda *a, **k: None)

    trainer = rl_training.RLTraining(
        db_url="db",
        model_path=tmp_path / "m.pt",
        dynamic_workers=True,
        worker_update_interval=0.0,
    )
    trainer.data.dataset = DummyDataset()
    loader = trainer.data.train_dataloader()
    trainer.recompute_workers(loader)

    event_bus.publish("system_metrics_combined", {"cpu": 90.0})
    high = loader.num_workers
    event_bus.publish("system_metrics_combined", {"cpu": 10.0})
    low = loader.num_workers

    trainer.close()

    assert low > high


def test_calc_num_workers_cluster_env(monkeypatch):
    import types
    import solhunter_zero.rl_training as rl_training

    monkeypatch.setattr(rl_training.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(
        rl_training.psutil,
        "virtual_memory",
        lambda: types.SimpleNamespace(percent=0.0),
    )
    monkeypatch.setattr(
        rl_training.psutil, "cpu_percent", lambda interval=None: 20.0
    )

    calls = {"count": 0}

    def fake_get_cpu_usage(_cb=None):
        calls["count"] += 1
        return 80.0

    monkeypatch.setattr(rl_training, "_get_cpu_usage", fake_get_cpu_usage)

    monkeypatch.setenv("RL_CLUSTER_WORKERS", "1")
    cluster = rl_training._calc_num_workers(400, dynamic=True)

    monkeypatch.setenv("RL_CLUSTER_WORKERS", "0")
    local = rl_training._calc_num_workers(400, dynamic=True)

    assert calls["count"] == 1
    assert cluster < local


@pytest.mark.asyncio
async def test_mmap_preferred_when_available(monkeypatch, tmp_path):
    import numpy as np
    from pathlib import Path
    from solhunter_zero.offline_data import OfflineData

    db_url = f"sqlite:///{tmp_path/'data.db'}"
    data = OfflineData(db_url)
    await data.log_snapshot("tok", 1.0, 1.0, total_depth=1.0, imbalance=0.0, slippage=0.0, volume=0.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)

    mmap_dir = tmp_path / "datasets"
    mmap_dir.mkdir()
    mmap_path = mmap_dir / "offline_data.npz"
    await data.export_npz(mmap_path)

    monkeypatch.chdir(tmp_path)

    called = {}
    orig_load = np.load

    def fake_load(path, *a, **kw):
        called["path"] = str(path)
        return orig_load(path, *a, **kw)

    monkeypatch.setattr(np, "load", fake_load)
    monkeypatch.setattr(OfflineData, "list_trades", lambda *a, **k: (_ for _ in ()).throw(AssertionError("db used")))
    monkeypatch.setattr(OfflineData, "list_snapshots", lambda *a, **k: (_ for _ in ()).throw(AssertionError("db used")))

    trainer = RLTraining(db_url=db_url, model_path=tmp_path / "m.pt")
    await trainer.data.setup()

    assert Path(called["path"]).resolve() == mmap_path.resolve()
    assert Path(trainer.data.mmap_path).resolve() == mmap_path.resolve()


@pytest.mark.asyncio
async def test_mmap_generated_when_missing(monkeypatch, tmp_path):
    db_url = f"sqlite:///{tmp_path/'data.db'}"
    data = OfflineData(db_url)
    await data.log_snapshot("tok", 1.0, 1.0, imbalance=0.0, total_depth=1.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)

    out_dir = tmp_path / "datasets"
    out_dir.mkdir()
    mmap_path = out_dir / "offline_data.npz"

    called = {}

    def fake_main(args=None):
        called["args"] = args
        mmap_path.write_bytes(b"x")
        return 0

    monkeypatch.setattr(build_mmap_dataset, "main", fake_main)
    monkeypatch.chdir(tmp_path)

    trainer = RLTraining(db_url=db_url, model_path=tmp_path / "m.pt")

    assert mmap_path.exists()
    assert Path(trainer.data.mmap_path).resolve() == mmap_path.resolve()
    assert "--db" in called.get("args", [])


@pytest.mark.asyncio
async def test_mmap_generation_disabled(monkeypatch, tmp_path):
    db_url = f"sqlite:///{tmp_path/'data.db'}"
    data = OfflineData(db_url)
    await data.log_snapshot("tok", 1.0, 1.0, imbalance=0.0, total_depth=1.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)

    out_dir = tmp_path / "datasets"
    out_dir.mkdir()
    mmap_path = out_dir / "offline_data.npz"

    called = False

    def fake_main(args=None):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(build_mmap_dataset, "main", fake_main)
    monkeypatch.setenv("RL_BUILD_MMAP_DATASET", "0")
    monkeypatch.chdir(tmp_path)

    trainer = RLTraining(db_url=db_url, model_path=tmp_path / "m.pt")
    await trainer.data.setup()

    assert not called
    assert trainer.data.mmap_path is None
    assert not mmap_path.exists()


@pytest.mark.asyncio
async def test_trade_data_prefetch(monkeypatch, tmp_path):
    import numpy as np
    import sys
    monkeypatch.setitem(sys.modules, "numpy", np)
    import importlib
    import solhunter_zero.rl_training as rl_training
    rl_training = importlib.reload(rl_training)
    db_url = f"sqlite:///{tmp_path/'data.db'}"
    data = OfflineData(db_url)
    await data.log_snapshot("tok", 1.0, 1.0, imbalance=0.0, total_depth=1.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)

    monkeypatch.setenv("RL_BUILD_MMAP_DATASET", "0")
    dm = rl_training.TradeDataModule(db_url, mmap_path=None, prefetch_buffer=2)
    task = dm.start_prefetch()
    assert task is not None
    await dm.setup()
    assert dm.dataset is not None


@pytest.mark.asyncio
async def test_mmap_created_on_daemon_train(monkeypatch, tmp_path):
    from solhunter_zero.offline_data import OfflineData
    from scripts import build_mmap_dataset
    import psutil
    monkeypatch.setattr(
        psutil,
        "Process",
        lambda: types.SimpleNamespace(cpu_percent=lambda interval=None: 0.0),
        raising=False,
    )
    from solhunter_zero.rl_daemon import RLDaemon

    db_path = tmp_path / "data.db"
    db_url = f"sqlite:///{db_path}"
    data = OfflineData(db_url)
    await data.log_snapshot("tok", 1.0, 1.0, total_depth=1.0, imbalance=0.0)
    await data.log_trade("tok", "buy", 1.0, 1.0)

    mmap_path = tmp_path / "datasets" / "offline_data.npz"

    def fake_main(args=None):
        mmap_path.parent.mkdir(exist_ok=True)
        mmap_path.write_bytes(b"x")
        return 0

    monkeypatch.setattr(build_mmap_dataset, "main", fake_main)
    monkeypatch.chdir(tmp_path)

    daemon = RLDaemon(
        memory_path=f"sqlite:///{tmp_path/'mem.db'}",
        data_path=str(db_path),
        model_path=tmp_path / "model.pt",
    )

    asyncio.run(daemon.train())

    assert mmap_path.exists()
    daemon.close()
    mmap_path.unlink()


def test_prioritized_sampler(monkeypatch):
    import types
    import datetime as dt
    import solhunter_zero.rl_training as rl_training

    class DummySampler:
        def __init__(self, weights, num_samples, replacement=True):
            self.weights = list(float(w) for w in weights)
            self.num_samples = num_samples

        def __iter__(self):
            import random
            total = sum(self.weights)
            probs = [w / total for w in self.weights]
            for _ in range(self.num_samples):
                yield random.choices(range(len(self.weights)), probs)[0]

    monkeypatch.setattr(rl_training, "WeightedRandomSampler", DummySampler)

    trades = [
        types.SimpleNamespace(token="t", side="buy", price=1.0, amount=1.0, timestamp=dt.datetime.utcnow()),
        types.SimpleNamespace(token="t", side="sell", price=1.0, amount=2.0, timestamp=dt.datetime.utcnow()),
    ]

    ds = rl_training._TradeDataset(trades, [])
    prio = rl_training.PrioritizedReplayDataset(ds)
    sampler = rl_training.WeightedRandomSampler(prio.weights, 500, replacement=True)
    counts = {0: 0, 1: 0}
    for idx in sampler:
        counts[idx] += 1

    assert counts[1] > counts[0]


def test_trade_dataset_cluster_feature(monkeypatch):
    import types
    import datetime as dt
    import numpy as np
    import solhunter_zero.rl_training as rl_training

    calls = {"count": 0}

    class DummyMemory:
        cluster_centroids = np.zeros((3, 2), dtype=np.float32)

        def cluster_trades(self):
            calls["count"] += 1

        def top_cluster(self, _ctx):
            return 2

        def top_cluster_many(self, ctxs):
            return [2 for _ in ctxs]

    trades = [
        types.SimpleNamespace(
            token="t",
            side="buy",
            price=1.0,
            amount=1.0,
            timestamp=dt.datetime.utcnow(),
            context="foo",
        )
    ]

    ds = rl_training._TradeDataset(trades, [], memory=DummyMemory())
    assert calls["count"] == 1
    assert ds.states.shape[1] == 10
    assert ds.states[0][-1] == pytest.approx(2 / 3)


def test_models_accept_new_feature():
    import torch
    import solhunter_zero.rl_training as rl_training
    import solhunter_zero.rl_daemon as rl_d
    import solhunter_zero.rl_algorithms as rl_a

    x10 = torch.zeros(1, 10)
    assert rl_training.LightningPPO()(x10).shape == (1, 2)
    assert rl_training.LightningDQN()(x10).shape == (1, 2)
    assert rl_training.LightningA3C()(x10).shape == (1, 2)
    ddpg = rl_training.LightningDDPG()
    assert ddpg.actor(x10).shape == (1, 1)
    assert ddpg.critic(torch.cat([x10, torch.zeros(1, 1)], dim=1)).shape == (1, 1)

    x9 = torch.zeros(1, 9)
    assert rl_d._DQN()(x9).shape == (1, 2)
    assert rl_d._PPO()(x9).shape == (1, 2)
    assert rl_a._A3C()(x9).shape == (1, 2)
    assert rl_a._DDPG()(x9).shape == (1, 1)


