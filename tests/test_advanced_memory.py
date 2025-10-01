import pytest
pytest.importorskip("numpy")
pytest.importorskip("faiss")
import numpy as np, faiss
if getattr(np, "_STUB", False) or getattr(faiss, "_STUB", False):
    pytest.skip("requires real numpy/faiss", allow_module_level=True)

from solhunter_zero.advanced_memory import AdvancedMemory
import sys


def test_insert_search_persist(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    db = tmp_path / "mem.db"
    idx = tmp_path / "index.faiss"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    sim_id = mem.log_simulation("TOK", expected_roi=1.5, success_prob=0.8, agent="test")
    mem.log_trade(
        token="TOK",
        direction="buy",
        amount=1.0,
        price=2.0,
        reason="test",
        context="great momentum ahead",
        emotion="bullish",
        simulation_id=sim_id,
    )

    trades = mem.list_trades()
    assert len(trades) == 1
    assert trades[0].context == "great momentum ahead"

    results = mem.search("momentum")
    assert results and results[0].id == trades[0].id

    # Recreate memory to test persistence
    mem2 = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    trades2 = mem2.list_trades()
    assert len(trades2) == 1
    results2 = mem2.search("momentum")
    assert results2 and results2[0].id == trades[0].id


def test_advanced_list_filters(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx.faiss"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    a = mem.log_trade(token="X", direction="buy", amount=1, price=1)
    b = mem.log_trade(token="Y", direction="sell", amount=2, price=1)
    c = mem.log_trade(token="X", direction="sell", amount=3, price=1)

    assert len(mem.list_trades(token="X")) == 2
    assert len(mem.list_trades(limit=2)) == 2
    ids = [t.id for t in mem.list_trades(since_id=b)]
    assert ids == [c]


def test_replicated_trade_dedup(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    db = tmp_path / "rep.db"
    idx = tmp_path / "rep.index"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx), replicate=True)

    from uuid import uuid4
    from solhunter_zero.event_bus import publish
    from solhunter_zero.schemas import TradeLogged

    tid = str(uuid4())
    event = TradeLogged(token="TOK", direction="buy", amount=1.0, price=1.0, uuid=tid)
    publish("trade_logged", event)
    publish("trade_logged", event)

    trades = mem.list_trades()
    assert len(trades) == 1
    assert trades[0].uuid == tid


def test_sync_interval_env(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    monkeypatch.setenv("MEMORY_SYNC_INTERVAL", "2.5")

    seen = {}

    def fake_start(self, interval: float = 0.0):
        seen["interval"] = interval

    monkeypatch.setattr(AdvancedMemory, "_start_sync_task", fake_start)

    AdvancedMemory(
        url=f"sqlite:///{tmp_path/'env.db'}",
        index_path=str(tmp_path/'env.index'),
        replicate=True,
    )

    assert seen.get("interval") == 2.5


def test_sync_interval_param(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    monkeypatch.setenv("MEMORY_SYNC_INTERVAL", "2.5")

    seen = {}

    def fake_start(self, interval: float = 0.0):
        seen["interval"] = interval

    monkeypatch.setattr(AdvancedMemory, "_start_sync_task", fake_start)

    AdvancedMemory(
        url=f"sqlite:///{tmp_path/'param.db'}",
        index_path=str(tmp_path/'param.index'),
        replicate=True,
        sync_interval=1.0,
    )

    assert seen.get("interval") == 1.0


def test_memory_sync_exchange(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    db1 = tmp_path / "a.db"
    idx1 = tmp_path / "a.index"
    db2 = tmp_path / "b.db"
    idx2 = tmp_path / "b.index"

    mem1 = AdvancedMemory(url=f"sqlite:///{db1}", index_path=str(idx1), replicate=True)
    mem2 = AdvancedMemory(url=f"sqlite:///{db2}", index_path=str(idx2), replicate=True)

    mem1.log_trade(token="A", direction="buy", amount=1.0, price=1.0)

    from solhunter_zero.event_bus import publish
    from solhunter_zero.schemas import MemorySyncRequest

    publish("memory_sync_request", MemorySyncRequest(last_id=0))

    assert len(mem2.list_trades()) == 1

    mem2.log_trade(token="B", direction="sell", amount=2.0, price=2.0)
    publish("memory_sync_request", MemorySyncRequest(last_id=0))

    assert len(mem1.list_trades()) == 2
    assert len(mem2.list_trades()) == 2


def test_cluster_trades_groups(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    import numpy as np
    import faiss
    import importlib
    sys.modules["numpy"] = np
    sys.modules["faiss"] = faiss
    import solhunter_zero.advanced_memory as am
    am = importlib.reload(am)
    am.faiss = faiss

    class DummyModel:
        def __init__(self):
            self.dim = 2

        def get_sentence_embedding_dimension(self):
            return self.dim

        def encode(self, texts):
            vecs = []
            for t in texts:
                if "bear" in t:
                    vecs.append([-1.0, 0.0])
                else:
                    vecs.append([1.0, 0.0])
            return np.asarray(vecs, dtype="float32")

    db = tmp_path / "c.db"
    idx = tmp_path / "c.index"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem.model = DummyModel()
    mem.index = faiss.IndexIDMap2(faiss.IndexFlatL2(mem.model.dim))
    mem.cpu_index = None

    a = mem.log_trade(token="T", direction="buy", amount=1, price=1, context="bull run", emotion="greedy")
    b = mem.log_trade(token="T", direction="sell", amount=1, price=2, context="bullish vibes", emotion="greedy")
    c = mem.log_trade(token="T", direction="buy", amount=1, price=2, context="bear drop", emotion="fear")
    d = mem.log_trade(token="T", direction="sell", amount=1, price=1, context="bearish slump", emotion="fear")

    clusters = mem.cluster_trades(num_clusters=2)
    assert clusters[a] == clusters[b]
    assert clusters[c] == clusters[d]
    assert clusters[a] != clusters[c]

    cid = mem.top_cluster("bear crash soon")
    assert cid == clusters[c]

    cids = mem.top_cluster_many(["bear crash soon", "bull run again"])
    assert cids == [clusters[c], clusters[a]]

    stats = mem.export_cluster_stats()
    by_cluster = {s["cluster"]: s for s in stats}
    assert by_cluster[clusters[a]]["common_emotion"] == "greedy"
    assert by_cluster[clusters[c]]["common_emotion"] == "fear"
    assert by_cluster[clusters[a]]["average_roi"] > 0
    assert by_cluster[clusters[c]]["average_roi"] < 0


def test_close_and_reopen(tmp_path, monkeypatch):
    monkeypatch.setenv("GPU_MEMORY_INDEX", "0")
    import sys, importlib
    for m in ["numpy", "faiss", "requests", "requests.exceptions", "sentence_transformers"]:
        sys.modules.pop(m, None)
    import numpy as np
    import faiss
    import solhunter_zero.advanced_memory as am
    am = importlib.reload(am)

    class DummyModel:
        def get_sentence_embedding_dimension(self):
            return 3

        def encode(self, texts):
            return np.zeros((len(texts), 3), dtype="float32")

    am.faiss = faiss
    monkeypatch.setattr(am, "SentenceTransformer", lambda *a, **k: DummyModel())

    db = tmp_path / "persist.db"
    idx = tmp_path / "persist.index"
    mem = am.AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem.log_trade(token="X", direction="buy", amount=1.0, price=1.0, context="test")
    mem.close()

    mem2 = am.AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    trades = mem2.list_trades()
    assert len(trades) == 1
    assert mem2.index is not None and mem2.index.ntotal == 1

