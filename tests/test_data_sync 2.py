import asyncio
import aiohttp
import time
import types
import sys

dummy_trans = types.ModuleType("transformers")
dummy_trans.pipeline = lambda *a, **k: lambda x: []
sys.modules.setdefault("transformers", dummy_trans)
sys.modules["transformers"].pipeline = dummy_trans.pipeline
dummy_solana = types.ModuleType("solana")
dummy_rpc = types.ModuleType("solana.rpc")
dummy_api = types.ModuleType("solana.rpc.api")
dummy_api.Client = object
dummy_async_api = types.ModuleType("solana.rpc.async_api")
dummy_async_api.AsyncClient = object
sys.modules.setdefault("solana", dummy_solana)
sys.modules.setdefault("solana.rpc", dummy_rpc)
sys.modules.setdefault("solana.rpc.api", dummy_api)
sys.modules.setdefault("solana.rpc.async_api", dummy_async_api)
sys.modules.setdefault("sklearn", types.ModuleType("sklearn"))
dummy_sk_linear = types.ModuleType("sklearn.linear_model")
dummy_sk_linear.LinearRegression = object
sys.modules.setdefault("sklearn.linear_model", dummy_sk_linear)
dummy_sk_ensemble = types.ModuleType("sklearn.ensemble")
dummy_sk_ensemble.GradientBoostingRegressor = object
dummy_sk_ensemble.RandomForestRegressor = object
sys.modules.setdefault("sklearn.ensemble", dummy_sk_ensemble)
sys.modules.setdefault("numpy", types.ModuleType("numpy"))
sys.modules.setdefault("xgboost", types.ModuleType("xgboost"))
sys.modules.setdefault("solders", types.ModuleType("solders"))
sys.modules.setdefault("solders.keypair", types.ModuleType("solders.keypair"))
sys.modules["solders.keypair"].Keypair = object
sys.modules.setdefault("solders.transaction", types.ModuleType("solders.transaction"))
sys.modules["solders.transaction"].VersionedTransaction = object
sys.modules.setdefault("torch", types.ModuleType("torch"))
sys.modules["torch"].Tensor = object
sys.modules.setdefault("torch.nn", types.ModuleType("torch.nn"))
sys.modules["torch.nn"].Module = object
sys.modules.setdefault("google", types.ModuleType("google"))
sys.modules.setdefault("google.protobuf", types.ModuleType("google.protobuf"))
dummy_pb = types.ModuleType("solhunter_zero.event_pb2")
for name in [
    "Event",
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
]:
    setattr(dummy_pb, name, type(name, (), {}))
sys.modules.setdefault("solhunter_zero.event_pb2", dummy_pb)

from solhunter_zero import data_sync
from solhunter_zero.offline_data import OfflineData

class FakeResp:
    def __init__(self, url):
        self.url = url
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        pass
    def raise_for_status(self):
        pass
    async def json(self):
        return {"snapshots": []}

class FakeSession:
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        pass
    def get(self, url, timeout=10):
        return FakeResp(url)


def test_sync_snapshots_no_recursion(tmp_path, monkeypatch):
    db = tmp_path / "data.db"

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    monkeypatch.setattr(data_sync, "fetch_sentiment_async", lambda *a, **k: 0.0)

    calls = {}
    orig_gather = asyncio.gather

    async def gather_wrapper(*args, **kwargs):
        calls.setdefault("count", 0)
        calls["count"] += 1
        return await orig_gather(*args, **kwargs)

    monkeypatch.setattr(asyncio, "gather", gather_wrapper)

    asyncio.run(data_sync.sync_snapshots(["A", "B"], db_path=str(db), base_url="http://api"))

    assert calls.get("count") == 1


def test_scheduler_rotation(tmp_path, monkeypatch):
    db = tmp_path / "data.db"

    async def fake_sync(days: int = 3, db_path: str = "offline_data.db") -> None:
        data = OfflineData(f"sqlite:///{db_path}")
        await data.log_snapshot("tok", 1.0, 1.0, imbalance=0.0)
        await data.close()

    monkeypatch.setattr(data_sync, "sync_recent", fake_sync)
    monkeypatch.setenv("OFFLINE_DATA_LIMIT_GB", "0.000001")

    data_sync.start_scheduler(interval=0.01, db_path=str(db))
    time.sleep(0.05)
    data_sync.stop_scheduler()

    data = OfflineData(f"sqlite:///{db}")
    snaps = asyncio.run(data.list_snapshots())
    assert len(snaps) <= 1

