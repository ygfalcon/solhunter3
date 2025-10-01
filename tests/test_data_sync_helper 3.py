import json
import os
import asyncio
import aiohttp
import time
import pytest

transformers = pytest.importorskip("transformers")
if not hasattr(transformers, "pipeline"):
    transformers.pipeline = lambda *a, **k: lambda x: []
pytest.importorskip("sklearn")

from solhunter_zero import depth_client

import solhunter_zero.data_sync as data_sync
from solhunter_zero.offline_data import OfflineData


@pytest.mark.asyncio
async def test_sync_snapshots_and_prune(tmp_path, monkeypatch):
    db = tmp_path / "data.db"

    called = {}

    class FakeResp:
        def __init__(self, url):
            called.setdefault("urls", []).append(url)
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def raise_for_status(self):
            pass
        async def json(self):
            return {"snapshots": [
                {"price": 1.0, "depth": 2.0, "total_depth": 3.0, "imbalance": 0.1},
                {"price": 1.1, "depth": 2.1, "total_depth": 3.1, "imbalance": 0.2},
            ]}

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=10):
            return FakeResp(url)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    monkeypatch.setattr(data_sync, "fetch_sentiment_async", lambda *a, **k: 0.0)

    await data_sync.sync_snapshots(["TOK"], db_path=str(db), limit_gb=0.0000001, base_url="http://api")

    data = OfflineData(f"sqlite:///{db}")
    snaps = await data.list_snapshots("TOK")
    assert not snaps  # pruned due to low limit
    assert called["urls"]


@pytest.mark.asyncio
async def test_sync_snapshots_stored(tmp_path, monkeypatch):
    db = tmp_path / "data.db"

    class FakeResp:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def raise_for_status(self):
            pass
        async def json(self):
            return {"snapshots": [
                {"price": 1.0, "depth": 2.0, "total_depth": 3.0, "imbalance": 0.1},
                {"price": 1.1, "depth": 2.1, "total_depth": 3.1, "imbalance": 0.2},
            ]}

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=10):
            return FakeResp()

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    monkeypatch.setattr(data_sync, "fetch_sentiment_async", lambda *a, **k: 0.0)

    await data_sync.sync_snapshots(["TOK"], db_path=str(db), limit_gb=1.0, base_url="http://api")

    data = OfflineData(f"sqlite:///{db}")
    snaps = await data.list_snapshots("TOK")
    assert len(snaps) == 2


@pytest.mark.asyncio
async def test_depth_snapshot_listener(tmp_path, monkeypatch):
    msg = {
        "tok": {
            "price": 1.0,
            "depth": 2.0,
            "total_depth": 2.0,
            "slippage": 0.2,
            "volume": 10.0,
            "imbalance": 0.1,
            "tx_rate": 0.3,
            "whale_share": 0.4,
            "spread": 0.01,
            "sentiment": 0.5,
        }
    }

    class FakeMsg:
        def __init__(self, data):
            self.type = aiohttp.WSMsgType.TEXT
            self.data = json.dumps(data)

    class FakeWS:
        def __init__(self, messages):
            self.messages = list(messages)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.messages:
                return FakeMsg(self.messages.pop(0))
            raise StopAsyncIteration

    class FakeSession:
        def __init__(self, messages):
            self.messages = messages

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def ws_connect(self, url):
            self.url = url
            return FakeWS(self.messages)

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession([msg]))

    db = tmp_path / "data.db"
    data = OfflineData(f"sqlite:///{db}")
    from solhunter_zero.data_pipeline import start_depth_snapshot_listener
    from solhunter_zero.event_bus import publish

    unsub = start_depth_snapshot_listener(data)
    publish("depth_update", msg, _broadcast=False)
    await asyncio.sleep(0)  # allow handler to run
    unsub()

    snaps = await data.list_snapshots("tok")
    assert snaps
    row = snaps[0]
    assert row.price == 1.0
    assert row.depth == 2.0
    assert row.total_depth == 2.0
    assert row.slippage == 0.2
    assert row.volume == 10.0
    assert row.imbalance == 0.1
    assert row.tx_rate == 0.3
    assert row.whale_share == 0.4
    assert row.spread == 0.01
    assert row.sentiment == 0.5


def test_sync_concurrency(tmp_path, monkeypatch):
    db = tmp_path / "data.db"

    class FakeResp:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def raise_for_status(self):
            pass
        async def json(self):
            await asyncio.sleep(0.05)
            return {"snapshots": []}

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=10):
            return FakeResp()

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    monkeypatch.setattr(data_sync, "fetch_sentiment_async", lambda *a, **k: 0.0)

    async def run(conc):
        start = time.perf_counter()
        await data_sync.sync_snapshots([
            "A",
            "B",
        ], db_path=str(db), base_url="http://api", concurrency=conc)
        return time.perf_counter() - start

    t1 = asyncio.run(run(1))
    t2 = asyncio.run(run(2))
    assert t2 < t1

