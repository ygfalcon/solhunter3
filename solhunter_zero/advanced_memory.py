from __future__ import annotations

import contextlib
import os
import datetime
import threading
import time
import uuid as uuid_module
from typing import List, Any
from collections import deque
from collections.abc import Iterable

import numpy as np
try:  # optional heavy deps
    import faiss  # type: ignore
    from sentence_transformers import SentenceTransformer
except Exception:  # pragma: no cover - optional dependency
    faiss = None
    SentenceTransformer = None

try:  # pragma: no cover - optional dependency
    import torch
except Exception:  # pragma: no cover - torch is optional
    torch = None  # type: ignore

from .device import detect_gpu, get_gpu_backend
from .util import parse_bool_env

_HAS_FAISS_GPU = bool(faiss and hasattr(faiss, "StandardGpuResources"))


def _detect_gpu() -> bool:
    """Return ``True`` if a CUDA or MPS device is available."""
    if _HAS_FAISS_GPU:
        try:
            faiss.StandardGpuResources()
            return True
        except Exception:
            pass
    try:
        return detect_gpu()
    except Exception:
        return False


def _gpu_index_enabled() -> bool:
    """Return ``True`` if the FAISS index should use GPU acceleration."""
    if parse_bool_env("FORCE_CPU_INDEX", False):
        return False
    if os.getenv("GPU_MEMORY_INDEX") is not None:
        return parse_bool_env("GPU_MEMORY_INDEX", False)
    if not _HAS_FAISS_GPU:
        return False
    try:
        backend = get_gpu_backend()
        return (
            backend == "torch"
            and torch is not None
            and torch.cuda.is_available()
        )
    except Exception:
        return False
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    DateTime,
    Text,
    ForeignKey,
    Index,
    event,
)
from sqlalchemy.orm import declarative_base, sessionmaker

from .base_memory import BaseMemory
from .event_bus import publish, subscription
from .schemas import TradeLogged, MemorySyncRequest, MemorySyncResponse


Base = declarative_base()


def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()


class SimulationSummary(Base):
    __tablename__ = "simulation_summaries"

    id = Column(Integer, primary_key=True)
    token = Column(String, nullable=False)
    agent = Column(String)
    expected_roi = Column(Float, nullable=False)
    success_prob = Column(Float, nullable=False)
    realized_roi = Column(Float)
    realized_price = Column(Float)
    timestamp = Column(DateTime, default=utcnow)


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    uuid = Column(String, unique=True, nullable=False, default=lambda: str(uuid_module.uuid4()))
    token = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=utcnow)
    reason = Column(Text)
    context = Column(Text)
    emotion = Column(String)
    simulation_id = Column(Integer, ForeignKey("simulation_summaries.id"))
    __table_args__ = (
        Index("ix_trades_id", "id"),
        Index("ix_trades_token_id", "token", "id"),
    )


def summarize_context(
    trades: List[Trade], *, model: SentenceTransformer | None = None, dim: int | None = None
) -> np.ndarray:
    """Aggregate embeddings of ``trades`` into a single vector."""
    if model is not None:
        out_dim = model.get_sentence_embedding_dimension()
    else:
        out_dim = dim or 1
    if not trades or model is None:
        return np.zeros(out_dim, dtype="float32")
    texts = [t.context or f"{t.direction} {t.token}" for t in trades]
    vecs = model.encode(texts)
    vecs = np.asarray(vecs, dtype="float32")
    return vecs.mean(axis=0)


class AdvancedMemory(BaseMemory):
    """Store trades with semantic search on context text."""

    def __init__(
        self,
        url: str = "sqlite:///memory.db",
        index_path: str = "trade.index",
        replicate: bool = False,
        *,
        sync_interval: float | None = None,
    ) -> None:
        # SQLite tuned for long-running/background use
        is_sqlite = url.startswith("sqlite")
        connect_args = {}
        if is_sqlite:
            connect_args = {"check_same_thread": False}
        self.engine = create_engine(
            url,
            echo=False,
            future=True,
            pool_pre_ping=True,
            connect_args=connect_args,
        )
        if is_sqlite:
            def _set_sqlite_pragma(dbapi_conn, _connection_record):
                cursor = dbapi_conn.cursor()
                try:
                    cursor.execute("PRAGMA journal_mode=WAL")
                    cursor.execute("PRAGMA synchronous=NORMAL")
                finally:
                    cursor.close()

            event.listen(self.engine, "connect", _set_sqlite_pragma)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False)

        self.index_path = index_path
        self.cpu_index = None
        self.index = None
        self.model = None
        self._use_gpu = _gpu_index_enabled()
        self._faiss_lock = threading.RLock()
        self._index_dirty = False
        self._adds_since_flush = 0
        self._last_index_flush = time.time()
        self._recent_uuid_limit = 5000
        self._recent_uuids: deque[str] = deque()
        self._recent_uuid_set: set[str] = set()
        self._flush_ms = int(os.getenv("MEMORY_INDEX_FLUSH_MS", "750") or 750)
        self._flush_batch = int(os.getenv("MEMORY_INDEX_MAX_BATCH_ADDS", "256") or 256)
        self._cluster_cap = int(os.getenv("MEMORY_CLUSTER_MAX_VECTORS", "50000") or 50000)
        self._embed_model_name = os.getenv("MEMORY_EMBED_MODEL", "all-MiniLM-L6-v2")
        self._max_context_chars = int(os.getenv("MEMORY_MAX_CONTEXT_CHARS", "2048") or 2048)

        self.cluster_index = None
        self.cluster_centroids: np.ndarray | None = None
        self._trade_clusters: dict[int, int] = {}

        self._replication_sub = None
        self._sync_req_sub = None
        self._sync_res_sub = None
        self._sync_stop = None
        self._sync_thread = None
        interval_env = os.getenv("MEMORY_SYNC_INTERVAL")
        try:
            env_interval = float(interval_env) if interval_env else None
        except Exception:
            env_interval = None
        sync_interval = sync_interval if sync_interval is not None else env_interval
        if sync_interval is None:
            sync_interval = 5.0
        if replicate:
            self._replication_sub = subscription("trade_logged", self._apply_remote)
            self._replication_sub.__enter__()
            self._sync_req_sub = subscription("memory_sync_request", self._handle_sync_request)
            self._sync_req_sub.__enter__()
            self._sync_res_sub = subscription("memory_sync_response", self._handle_sync_response)
            self._sync_res_sub.__enter__()
            self._start_sync_task(sync_interval)

    # ------------------------------------------------------------------
    def _ensure_model(self) -> None:
        if self.model is not None:
            return
        if SentenceTransformer is None or faiss is None:
            return
        model = SentenceTransformer(self._embed_model_name)
        dim = model.get_sentence_embedding_dimension()
        # Ensure index directory exists before any writes
        try:
            d = os.path.dirname(self.index_path)
            if d:
                os.makedirs(d, exist_ok=True)
        except Exception:
            pass
        if os.path.exists(self.index_path):
            try:
                cpu_index = faiss.read_index(self.index_path)
                if cpu_index.d != dim:
                    cpu_index = faiss.IndexIDMap2(faiss.IndexFlatL2(dim))
            except Exception:
                cpu_index = faiss.IndexIDMap2(faiss.IndexFlatL2(dim))
        else:
            cpu_index = faiss.IndexIDMap2(faiss.IndexFlatL2(dim))
        if self._use_gpu and _HAS_FAISS_GPU:
            try:
                self.cpu_index = cpu_index
                self.index = faiss.index_cpu_to_all_gpus(cpu_index)
            except Exception:
                # Fall back to CPU if GPU path fails for any reason
                self.cpu_index = None
                self.index = cpu_index
        else:
            self.cpu_index = None
            self.index = cpu_index
        self.model = model
        self._index_dirty = False
        self._adds_since_flush = 0
        self._last_index_flush = time.time()

    # ------------------------------------------------------------------
    def _flush_index_locked(self) -> None:
        if self.index is None or faiss is None:
            return
        if not self._index_dirty:
            return
        # Atomic write to avoid truncated/corrupt index under crash
        tmp = f"{self.index_path}.tmp"
        faiss.write_index(self.cpu_index or self.index, tmp)
        os.replace(tmp, self.index_path)
        self._index_dirty = False
        self._adds_since_flush = 0
        self._last_index_flush = time.time()

    # ------------------------------------------------------------------
    def _add_embedding(self, text: str, trade_id: int) -> None:
        if faiss is None:
            return
        with self._faiss_lock:
            self._ensure_model()
            if self.index is None or self.model is None:
                return
            # Truncate pathological contexts to keep encode latency bounded
            t = text if not text or self._max_context_chars <= 0 else text[: self._max_context_chars]
            vec = self.model.encode([t])[0].astype("float32")
            ids = np.array([trade_id], dtype="int64")
            self.index.add_with_ids(np.array([vec]), ids)
            if self.cpu_index is not None:
                self.cpu_index.add_with_ids(np.array([vec]), ids)
            self._index_dirty = True
            self._adds_since_flush += 1
            now = time.time()
            if (
                (now - self._last_index_flush) * 1000.0 >= self._flush_ms
                or self._adds_since_flush >= self._flush_batch
            ):
                self._flush_index_locked()

    # ------------------------------------------------------------------
    def _apply_remote(self, msg: Any) -> None:
        data = msg if isinstance(msg, dict) else msg.__dict__
        trade_uuid = data.get("uuid")
        if trade_uuid is not None:
            if trade_uuid in self._recent_uuid_set:
                return
            with self.Session() as session:
                exists = session.query(Trade).filter_by(uuid=trade_uuid).first()
                if exists:
                    self._recent_uuids.append(trade_uuid)
                    self._recent_uuid_set.add(trade_uuid)
                    while len(self._recent_uuids) > self._recent_uuid_limit:
                        old = self._recent_uuids.popleft()
                        self._recent_uuid_set.discard(old)
                    return
        data.pop("trade_id", None)
        trade_id = self.log_trade(_broadcast=False, **data)
        if trade_uuid and trade_id:
            self._recent_uuids.append(trade_uuid)
            self._recent_uuid_set.add(trade_uuid)
            while len(self._recent_uuids) > self._recent_uuid_limit:
                old = self._recent_uuids.popleft()
                self._recent_uuid_set.discard(old)

    # ------------------------------------------------------------------
    def log_simulation(
        self,
        token: str,
        *,
        expected_roi: float,
        success_prob: float,
        agent: str | None = None,
        realized_roi: float | None = None,
        realized_price: float | None = None,
    ) -> int:
        """Insert a simulation summary and return its id."""
        with self.Session() as session:
            sim = SimulationSummary(
                token=token,
                agent=agent,
                expected_roi=expected_roi,
                success_prob=success_prob,
                realized_roi=realized_roi,
                realized_price=realized_price,
            )
            session.add(sim)
            session.commit()
            return sim.id

    # ------------------------------------------------------------------
    def log_trade(
        self,
        *,
        token: str,
        direction: str,
        amount: float,
        price: float,
        uuid: str | None = None,
        reason: str | None = None,
        context: str = "",
        thought: str | None = None,
        emotion: str = "",
        simulation_id: int | None = None,
        _broadcast: bool = True,
    ) -> int:
        with self.Session() as session:
            trade_uuid = uuid or str(uuid_module.uuid4())
            ctx = context or (thought or "")
            trade = Trade(
                token=token,
                direction=direction,
                amount=amount,
                price=price,
                uuid=trade_uuid,
                reason=reason,
                context=ctx,
                emotion=emotion,
                simulation_id=simulation_id,
            )
            session.add(trade)
            session.commit()
            text = ctx or f"{direction} {token}"
            self._add_embedding(text, trade.id)
        if _broadcast:
            try:
                publish(
                    "trade_logged",
                    TradeLogged(
                        token=token,
                        direction=direction,
                        amount=amount,
                        price=price,
                        reason=reason,
                        context=ctx,
                        emotion=emotion,
                        simulation_id=simulation_id,
                        uuid=trade_uuid,
                        trade_id=trade.id,
                    ),
                )
            except Exception:
                pass
        return trade.id

    # ------------------------------------------------------------------
    def list_trades(
        self,
        *,
        token: str | None = None,
        limit: int | None = None,
        since_id: int | None = None,
    ) -> List[Trade]:
        """Return trades optionally filtered by token or id."""
        with self.Session() as session:
            q = session.query(Trade)
            if token is not None:
                q = q.filter_by(token=token)
            if since_id is not None:
                q = q.filter(Trade.id > since_id)
            q = q.order_by(Trade.id)
            if limit is not None:
                q = q.limit(limit)
            return list(q)

    # ------------------------------------------------------------------
    def simulation_success_rate(self, token: str, *, agent: str | None = None) -> float:
        """Return the average success probability for recorded simulations."""
        with self.Session() as session:
            query = session.query(SimulationSummary).filter_by(token=token)
            if agent is not None:
                query = query.filter_by(agent=agent)
            sims = query.all()
            if not sims:
                return 0.0
            return float(sum(s.success_prob for s in sims) / len(sims))

    # ------------------------------------------------------------------
    def search(self, query: str, k: int = 5) -> List[Trade]:
        ids: list[int] | None = None
        with self._faiss_lock:
            self._ensure_model()
            if self.index is not None and self.model is not None:
                if self.index.ntotal == 0:
                    return []
                vec = self.model.encode([query])[0].astype("float32")
                _distances, indices = self.index.search(np.array([vec]), k)
                ids = [int(idx) for idx in indices[0] if idx != -1]
                if not ids:
                    return []
        if ids is not None:
            with self.Session() as session:
                rows = list(session.query(Trade).filter(Trade.id.in_(ids)))
                # Preserve FAISS similarity order
                order = {tid: i for i, tid in enumerate(ids)}
                rows.sort(key=lambda r: order.get(r.id, 1e9))
                return rows
        # simple fallback search
        with self.Session() as session:
            return (
                session.query(Trade)
                .filter(Trade.context.contains(query))
                .limit(k)
                .all()
            )

    # ------------------------------------------------------------------
    def latest_summary(self, *, limit: int = 10, token: str | None = None) -> np.ndarray:
        """Return an aggregated embedding vector for recent trades."""
        with self.Session() as session:
            q = session.query(Trade)
            if token is not None:
                q = q.filter_by(token=token)
            q = q.order_by(Trade.id.desc()).limit(limit)
            trades = list(q)
        with self._faiss_lock:
            self._ensure_model()
            dim = self.model.get_sentence_embedding_dimension() if self.model else 1
            return summarize_context(trades, model=self.model, dim=dim)

    # ------------------------------------------------------------------
    def export_trades(self, since_id: int = 0) -> List[TradeLogged]:
        return [
            TradeLogged(
                token=t.token,
                direction=t.direction,
                amount=t.amount,
                price=t.price,
                reason=t.reason,
                context=t.context,
                emotion=t.emotion,
                simulation_id=t.simulation_id,
                uuid=t.uuid,
                trade_id=t.id,
            )
            for t in self.list_trades(since_id=since_id)
        ]

    # ------------------------------------------------------------------
    def import_trades(self, trades: List[TradeLogged]) -> None:
        for t in trades:
            self._apply_remote(t)

    # ------------------------------------------------------------------
    def export_index(self) -> bytes | None:
        with self._faiss_lock:
            self._ensure_model()
            if self.index is None:
                return None
            self._flush_index_locked()
            return open(self.index_path, "rb").read()

    # ------------------------------------------------------------------
    def import_index(self, data: bytes) -> None:
        if faiss is None:
            return
        with self._faiss_lock:
            self._ensure_model()
            if self.index is None:
                return
            # Atomic import + dimension guard
            tmp = self.index_path + ".sync"
            try:
                with open(tmp, "wb") as fh:
                    fh.write(data)
                idx = faiss.read_index(tmp)
            finally:
                with contextlib.suppress(Exception):
                    os.remove(tmp)
            try:
                dim_ok = (
                    self.model.get_sentence_embedding_dimension()
                    if self.model is not None
                    else idx.d
                )
            except Exception:
                dim_ok = idx.d
            if getattr(idx, "d", dim_ok) != dim_ok:
                # Ignore incompatible index
                return
            if self.index.ntotal < idx.ntotal:
                try:
                    if self._use_gpu and _HAS_FAISS_GPU:
                        self.cpu_index = idx
                        self.index = faiss.index_cpu_to_all_gpus(idx)
                    else:
                        self.cpu_index = None
                        self.index = idx
                except Exception:
                    self.cpu_index = None
                    self.index = idx
                self._index_dirty = True
                self._flush_index_locked()

    # ------------------------------------------------------------------
    def cluster_trades(self, num_clusters: int = 50) -> dict[int, int]:
        """Cluster stored trade embeddings using FAISS k-means."""
        if faiss is None:
            self.cluster_index = None
            self.cluster_centroids = None
            self._trade_clusters = {}
            return {}
        with self._faiss_lock:
            self._ensure_model()
            if (
                self.index is None
                or self.model is None
                or self.index.ntotal == 0
            ):
                self.cluster_index = None
                self.cluster_centroids = None
                self._trade_clusters = {}
                return {}

            raw_index = self.cpu_index or self.index
            base_index = getattr(raw_index, "index", raw_index)
            ntotal = getattr(raw_index, "ntotal", 0)
            if ntotal == 0:
                self.cluster_index = None
                self.cluster_centroids = None
                self._trade_clusters = {}
                return {}
            dim = self.model.get_sentence_embedding_dimension()
            start = max(0, ntotal - self._cluster_cap)
            positions = range(start, ntotal)
            try:
                ids_arr = faiss.vector_to_array(raw_index.id_map)
                ids_subset = ids_arr[start:ntotal]
            except Exception:
                # Fallback: reconstruct ids as sequential if id_map missing
                ids_subset = np.arange(start, ntotal, dtype="int64")
            vecs = np.zeros((len(ids_subset), dim), dtype="float32")
            for idx_pos, store_pos in enumerate(positions):
                vecs[idx_pos] = base_index.reconstruct(store_pos)
            if vecs.size == 0:
                self.cluster_index = None
                self.cluster_centroids = None
                self._trade_clusters = {}
                return {}
            kmeans = faiss.Kmeans(dim, num_clusters, niter=25, verbose=False, seed=123)
            kmeans.cp.min_points_per_centroid = 1
            uniq = np.unique(vecs, axis=0)
            init = uniq[:num_clusters]
            if init.shape[0] < num_clusters:
                init = np.pad(init, ((0, num_clusters - init.shape[0]), (0, 0)), "edge")
            kmeans.train(vecs, init_centroids=init.astype("float32"))
            _, assign = kmeans.index.search(vecs, 1)
            self.cluster_centroids = kmeans.centroids
            self.cluster_index = faiss.IndexFlatL2(dim)
            self.cluster_index.add(kmeans.centroids)
            id_list = [int(i) for i in ids_subset]
            self._trade_clusters = {int(i): int(c) for i, c in zip(id_list, assign.ravel())}
            return dict(self._trade_clusters)

    # ------------------------------------------------------------------
    def top_cluster(self, context: str) -> int | None:
        """Return nearest cluster id for ``context`` text."""
        with self._faiss_lock:
            self._ensure_model()
            if (
                self.cluster_index is None
                or self.model is None
                or self.cluster_index.ntotal == 0
            ):
                return None
            vec = self.model.encode([context])[0].astype("float32")
            _, idx = self.cluster_index.search(np.array([vec]), 1)
            return int(idx[0][0]) if idx.size else None

    # ------------------------------------------------------------------
    def top_cluster_many(self, contexts: Iterable[str]) -> List[int | None]:
        """Return nearest cluster ids for multiple ``contexts``."""
        texts = list(contexts)
        if not texts:
            return []
        with self._faiss_lock:
            self._ensure_model()
            if (
                self.cluster_index is None
                or self.model is None
                or self.cluster_index.ntotal == 0
            ):
                return [None for _ in texts]
            vecs = self.model.encode(texts).astype("float32")
            _, idx = self.cluster_index.search(vecs, 1)
        out: List[int | None] = []
        for row in idx:
            out.append(int(row[0]) if row.size else None)
        return out

    # ------------------------------------------------------------------
    def export_cluster_stats(self) -> List[dict[str, Any]]:
        """Return summary statistics for each cluster."""
        if not self._trade_clusters or self.cluster_centroids is None:
            return []

        from collections import Counter

        with self.Session() as session:
            trades = {t.id: t for t in session.query(Trade).all()}

        stats: list[dict[str, Any]] = []
        for cluster_id in range(len(self.cluster_centroids)):
            ids = [tid for tid, c in self._trade_clusters.items() if c == cluster_id]
            if not ids:
                stats.append({"cluster": cluster_id, "count": 0, "average_roi": 0.0, "common_emotion": None})
                continue
            buy = sell = 0.0
            emotions: list[str] = []
            for tid in ids:
                t = trades.get(tid)
                if t is None:
                    continue
                if t.direction == "buy":
                    buy += float(t.amount) * float(t.price)
                else:
                    sell += float(t.amount) * float(t.price)
                if t.emotion:
                    emotions.append(t.emotion)
            roi = (sell - buy) / buy if buy > 0 else 0.0
            common = Counter(emotions).most_common(1)
            emotion = common[0][0] if common else None
            stats.append(
                {
                    "cluster": cluster_id,
                    "count": len(ids),
                    "average_roi": float(roi),
                    "common_emotion": emotion,
                }
            )
        return stats

    # ------------------------------------------------------------------
    def request_sync(self) -> None:
        last = 0
        trades = self.list_trades(limit=1)
        if trades:
            last = trades[-1].id
        publish("memory_sync_request", MemorySyncRequest(last_id=last))

    # ------------------------------------------------------------------
    def _handle_sync_request(self, msg: Any) -> None:
        data = msg if isinstance(msg, dict) else msg.__dict__
        since = int(data.get("last_id", 0))
        payload = MemorySyncResponse(
            trades=self.export_trades(since_id=since),
            index=self.export_index() or b"",
        )
        publish("memory_sync_response", payload)

    # ------------------------------------------------------------------
    def _handle_sync_response(self, msg: Any) -> None:
        data = msg if isinstance(msg, dict) else msg.__dict__
        trades = data.get("trades") or []
        idx = data.get("index")
        if trades:
            self.import_trades(
                [TradeLogged(**t) if isinstance(t, dict) else t for t in trades]
            )
        if idx:
            self.import_index(idx)

    # ------------------------------------------------------------------
    def _sync_loop(self, interval: float) -> None:
        while not self._sync_stop.is_set():
            self._sync_stop.wait(interval)
            if self._sync_stop.is_set():
                break
            try:
                self.request_sync()
            except Exception:
                pass

    # ------------------------------------------------------------------
    def _start_sync_task(self, interval: float = 5.0) -> None:
        self._sync_stop = threading.Event()
        self._sync_thread = threading.Thread(
            target=self._sync_loop, args=(interval,), daemon=True
        )
        self._sync_thread.start()

    # ------------------------------------------------------------------
    def close(self) -> None:
        with self._faiss_lock:
            if self.index is not None:
                self._flush_index_locked()
        if self._replication_sub is not None:
            self._replication_sub.__exit__(None, None, None)
        if self._sync_req_sub is not None:
            self._sync_req_sub.__exit__(None, None, None)
        if self._sync_res_sub is not None:
            self._sync_res_sub.__exit__(None, None, None)
        if self._sync_thread is not None and self._sync_stop is not None:
            self._sync_stop.set()
            self._sync_thread.join(timeout=1)
