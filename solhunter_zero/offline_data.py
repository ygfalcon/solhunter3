from __future__ import annotations

import asyncio
import datetime
import json
import mmap
import os
import time
from contextlib import suppress

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Float, Integer, String, insert, select
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
)

Base = declarative_base()


def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"

    id = Column(Integer, primary_key=True)
    token = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    depth = Column(Float, nullable=False)
    total_depth = Column(Float, nullable=False, default=0.0)
    slippage = Column(Float, nullable=False, default=0.0)
    volume = Column(Float, nullable=False, default=0.0)
    imbalance = Column(Float, nullable=False)
    tx_rate = Column(Float, nullable=False, default=0.0)
    whale_share = Column(Float, nullable=False, default=0.0)
    spread = Column(Float, nullable=False, default=0.0)
    sentiment = Column(Float, nullable=False, default=0.0)
    timestamp = Column(DateTime, default=utcnow)


class MarketTrade(Base):
    __tablename__ = "market_trades"

    id = Column(Integer, primary_key=True)
    token = Column(String, nullable=False)
    side = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=utcnow)


_sqlalchemy_index = getattr(sa, "Index", None)
if _sqlalchemy_index is not None:
    _sqlalchemy_index(
        "ix_market_snapshots_token_ts", MarketSnapshot.token, MarketSnapshot.timestamp
    )
    _sqlalchemy_index(
        "ix_market_trades_token_ts", MarketTrade.token, MarketTrade.timestamp
    )
    _sqlalchemy_index("ix_market_snapshots_ts", MarketSnapshot.timestamp)
    _sqlalchemy_index("ix_market_trades_ts", MarketTrade.timestamp)


class OfflineData:
    """Store order book snapshots and trade metrics for offline training."""

    def __init__(self, url: str = "sqlite:///offline_data.db") -> None:
        if url.startswith("sqlite:///") and "+" not in url:
            url = url.replace("sqlite:///", "sqlite+aiosqlite:///")
        self.engine = create_async_engine(url, echo=False, future=True)
        self.Session = async_sessionmaker(bind=self.engine, expire_on_commit=False)
        self._queue: asyncio.Queue[tuple[str, dict]] | None = None
        self._writer_task: asyncio.Task | None = None
        self._batch_size = 100
        self._interval = 1.0
        self._flush_max_batch: int | None = None
        self._queue_maxsize = int(os.getenv("OFFLINE_QUEUE_MAXSIZE", "10000") or 0)
        drop_oldest = os.getenv("OFFLINE_DROP_OLDEST", "0").lower()
        self._drop_oldest = drop_oldest in {"1", "true", "yes", "on"}
        self._coalesce_ms = float(os.getenv("OFFLINE_COALESCE_MS", "0") or 0.0)
        self._coalesce: dict[str, tuple[float, dict | None]] = {}
        self._memmap: mmap.mmap | None = None
        self._memmap_fd: int | None = None
        self._memmap_pos = 0
        self._memmap_size = 0
        self._memmap_flush_rows = 5000
        self._memmap_count = 0
        self._memmap_flush_ms = float(os.getenv("OFFLINE_MEMMAP_FLUSH_MS", "0") or 0.0)
        self._last_memmap_flush = time.monotonic()
        import asyncio
        async def _init_models():
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            asyncio.set_event_loop(asyncio.new_event_loop())
            loop = asyncio.get_event_loop()
        if loop.is_running():
            self._init_task = loop.create_task(_init_models())
        else:
            loop.run_until_complete(_init_models())
            self._init_task = loop.create_future()
            self._init_task.set_result(None)

    def start_writer(
        self,
        batch_size: int = 100,
        interval: float = 1.0,
        max_batch: int | None = None,
        memmap_path: str | None = None,
        memmap_size: int = 1_000_000,
    ) -> None:
        """Start background writer flushing queued entries."""
        env_batch = os.getenv("OFFLINE_BATCH_SIZE")
        env_interval = os.getenv("OFFLINE_FLUSH_INTERVAL")
        env_max = os.getenv("OFFLINE_FLUSH_MAX_BATCH")
        env_mmap_path = os.getenv("OFFLINE_MEMMAP_PATH")
        env_mmap_size = os.getenv("OFFLINE_MEMMAP_SIZE")
        env_mmap_flush_rows = os.getenv("OFFLINE_MEMMAP_FLUSH_ROWS")
        if env_batch is not None:
            batch_size = int(env_batch)
        if env_interval is not None:
            interval = float(env_interval)
        if env_max is not None:
            max_batch = int(env_max)
        if env_mmap_path is not None:
            memmap_path = env_mmap_path
        if env_mmap_size is not None:
            memmap_size = int(env_mmap_size)
        if env_mmap_flush_rows is not None:
            self._memmap_flush_rows = int(env_mmap_flush_rows)
        if self._writer_task and not self._writer_task.done():
            return
        self._batch_size = batch_size
        self._interval = interval
        self._flush_max_batch = max_batch
        self._memmap_pos = 0
        if memmap_path:
            self._memmap_size = memmap_size
            self._memmap_fd = os.open(memmap_path, os.O_RDWR | os.O_CREAT)
            os.ftruncate(self._memmap_fd, memmap_size)
            self._memmap = mmap.mmap(self._memmap_fd, memmap_size)
        else:
            self._memmap = None
            self._memmap_fd = None
            self._memmap_size = 0
        queue_size = max(self._queue_maxsize, 0)
        self._queue = asyncio.Queue(maxsize=queue_size)
        self._last_memmap_flush = time.monotonic()
        loop = asyncio.get_event_loop()
        self._writer_task = loop.create_task(self._writer())

    async def _writer(self) -> None:
        assert self._queue is not None
        pending: list[tuple[str, dict]] = []
        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        self._queue.get(), timeout=self._interval
                    )
                    pending.append(item)
                    if len(pending) >= self._batch_size:
                        await self._process_pending(pending)
                except asyncio.TimeoutError:
                    if pending:
                        await self._process_pending(pending)
        except asyncio.CancelledError:
            pass
        finally:
            if pending:
                await self._process_pending(pending)
            # drain remaining items
            while self._queue and not self._queue.empty():
                pending.append(self._queue.get_nowait())
            if pending:
                await self._process_pending(pending)
            if self._memmap is not None and self._memmap_pos:
                await self._flush_memmap()
            if self._memmap is not None:
                self._memmap.close()
                if self._memmap_fd is not None:
                    os.close(self._memmap_fd)
                self._memmap = None
                self._memmap_fd = None
            self._queue = None

    async def _process_pending(self, pending: list[tuple[str, dict]]) -> None:
        if not pending:
            return
        await self._handle_batch(pending)
        if self._queue is not None:
            for _ in range(len(pending)):
                self._queue.task_done()
        pending.clear()

    async def _handle_batch(self, items: list[tuple[str, dict]]) -> None:
        if self._memmap is None:
            await self._flush(items)
            return
        loop = asyncio.get_event_loop()
        for t, d in items:
            line = json.dumps([t, d]).encode() + b"\n"
            if self._memmap_size and len(line) > self._memmap_size:
                if self._memmap_count:
                    await self._flush_memmap()
                await self._flush([(t, d)])
                continue
            if self._memmap_pos + len(line) > self._memmap_size:
                await self._flush_memmap()
            self._memmap[self._memmap_pos : self._memmap_pos + len(line)] = line
            self._memmap_pos += len(line)
            self._memmap_count += 1
            now = loop.time()
            if self._memmap_count >= self._memmap_flush_rows or (
                self._memmap_flush_ms
                and (now - self._last_memmap_flush) * 1000.0 >= self._memmap_flush_ms
            ):
                await self._flush_memmap()
                self._last_memmap_flush = now
            else:
                try:
                    self._memmap.flush()
                except Exception:
                    pass

    async def _flush_memmap(self) -> None:
        if self._memmap is None or self._memmap_pos == 0:
            return
        self._memmap.seek(0)
        data = self._memmap.read(self._memmap_pos).splitlines()
        items = [tuple(json.loads(line)) for line in data]
        self._memmap_pos = 0
        self._memmap_count = 0
        self._memmap.seek(0)
        await self._flush(items)
        try:
            self._memmap.flush()
            if self._memmap_fd is not None:
                os.fsync(self._memmap_fd)
        except Exception:
            pass
        self._last_memmap_flush = asyncio.get_event_loop().time()

    async def _flush(self, items: list[tuple[str, dict]]) -> None:
        max_batch = self._flush_max_batch or len(items)
        async with self.Session() as session:
            for i in range(0, len(items), max_batch):
                batch = items[i : i + max_batch]
                snaps = [d for t, d in batch if t == "snap"]
                trades = [d for t, d in batch if t != "snap"]
                if snaps:
                    await session.execute(insert(MarketSnapshot), snaps)
                if trades:
                    await session.execute(insert(MarketTrade), trades)
            await session.commit()

    async def log_snapshot(
        self,
        token: str,
        price: float,
        depth: float,
        imbalance: float,
        total_depth: float = 0.0,
        slippage: float = 0.0,
        volume: float = 0.0,
        tx_rate: float = 0.0,
        whale_share: float = 0.0,
        spread: float = 0.0,
        sentiment: float = 0.0,
    ) -> None:
        await self._init_task
        data = dict(
            token=token,
            price=price,
            depth=depth,
            total_depth=total_depth,
            slippage=slippage,
            volume=volume,
            imbalance=imbalance,
            tx_rate=tx_rate,
            whale_share=whale_share,
            spread=spread,
            sentiment=sentiment,
        )
        if self._queue is not None:
            await self._maybe_coalesce_snapshot(token, data)
        else:
            async with self.Session() as session:
                session.add(MarketSnapshot(**data))
                await session.commit()

    async def log_trade(
        self,
        token: str,
        side: str,
        price: float,
        amount: float,
    ) -> None:
        """Record an executed trade for offline learning."""
        await self._init_task
        data = dict(token=token, side=side, price=price, amount=amount)
        if self._queue is not None:
            await self._enqueue_queue(("trade", data))
        else:
            async with self.Session() as session:
                session.add(MarketTrade(**data))
                await session.commit()

    async def list_snapshots(self, token: str | None = None):
        await self._init_task
        async with self.Session() as session:
            q = select(MarketSnapshot)
            if token:
                q = q.filter(MarketSnapshot.token == token)
            q = q.order_by(MarketSnapshot.timestamp)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def list_trades(self, token: str | None = None):
        await self._init_task
        async with self.Session() as session:
            q = select(MarketTrade)
            if token:
                q = q.filter(MarketTrade.token == token)
            q = q.order_by(MarketTrade.timestamp)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def export_npz(self, out_path: str, token: str | None = None):
        """Export snapshots and trades to a compressed ``.npz`` file.

        Parameters
        ----------
        out_path:
            Destination ``.npz`` file.
        token:
            Optional token filter.
        """
        try:
            import numpy as np
        except Exception as exc:  # pragma: no cover - dependency guard
            raise RuntimeError("NumPy is required for export_npz") from exc

        snaps = await self.list_snapshots(token)
        trades = await self.list_trades(token)

        snap_dtype = [
            ("token", "U32"),
            ("price", "f4"),
            ("depth", "f4"),
            ("total_depth", "f4"),
            ("slippage", "f4"),
            ("volume", "f4"),
            ("imbalance", "f4"),
            ("tx_rate", "f4"),
            ("whale_share", "f4"),
            ("spread", "f4"),
            ("sentiment", "f4"),
            ("timestamp", "f8"),
        ]

        snap_iter = (
            (
                s.token,
                float(s.price),
                float(s.depth),
                float(getattr(s, "total_depth", 0.0)),
                float(getattr(s, "slippage", 0.0)),
                float(getattr(s, "volume", 0.0)),
                float(s.imbalance),
                float(getattr(s, "tx_rate", 0.0)),
                float(getattr(s, "whale_share", 0.0)),
                float(getattr(s, "spread", 0.0)),
                float(getattr(s, "sentiment", 0.0)),
                s.timestamp.timestamp(),
            )
            for s in snaps
        )

        snap_arr = np.fromiter(snap_iter, dtype=snap_dtype, count=len(snaps))

        trade_dtype = [
            ("token", "U32"),
            ("side", "U8"),
            ("price", "f4"),
            ("amount", "f4"),
            ("timestamp", "f8"),
        ]

        trade_iter = (
            (
                t.token,
                t.side,
                float(t.price),
                float(t.amount),
                t.timestamp.timestamp(),
            )
            for t in trades
        )

        trade_arr = np.fromiter(trade_iter, dtype=trade_dtype, count=len(trades))

        np.savez_compressed(out_path, snapshots=snap_arr, trades=trade_arr)
        with np.load(out_path, mmap_mode="r") as npz_file:
            return {name: npz_file[name] for name in npz_file.files}

    async def close(self) -> None:
        """Flush pending items and dispose engine."""
        await self._init_task
        if self._writer_task:
            self._writer_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._writer_task
            self._writer_task = None
        if self._queue and not self._queue.empty():
            pending = [self._queue.get_nowait() for _ in range(self._queue.qsize())]
            await self._handle_batch(pending)
            for _ in pending:
                self._queue.task_done()
        coalesced = [
            ("snap", latest)
            for _, latest in self._coalesce.values()
            if latest is not None
        ]
        if coalesced:
            await self._flush(coalesced)
        self._coalesce.clear()
        if self._memmap is not None and self._memmap_pos:
            await self._flush_memmap()
            self._memmap.close()
            if self._memmap_fd is not None:
                os.close(self._memmap_fd)
            self._memmap = None
            self._memmap_fd = None
        self._queue = None
        await self.engine.dispose()

    @property
    def is_running(self) -> bool:
        return self._writer_task is not None and not self._writer_task.done()

    async def _enqueue_queue(self, item: tuple[str, dict]) -> None:
        assert self._queue is not None
        try:
            self._queue.put_nowait(item)
            return
        except asyncio.QueueFull:
            if not self._drop_oldest:
                await self._queue.put(item)
                return
        if self._drop_oldest:
            with suppress(asyncio.QueueEmpty):
                self._queue.get_nowait()
                self._queue.task_done()
            with suppress(asyncio.QueueFull):
                self._queue.put_nowait(item)

    async def _maybe_coalesce_snapshot(self, token: str, data: dict) -> None:
        if self._coalesce_ms <= 0:
            await self._enqueue_queue(("snap", data))
            return
        loop = asyncio.get_event_loop()
        now = loop.time()
        deadline, latest = self._coalesce.get(token, (0.0, None))
        if now < deadline:
            self._coalesce[token] = (deadline, data)
            return
        if latest is not None:
            await self._enqueue_queue(("snap", latest))
        await self._enqueue_queue(("snap", data))
        self._coalesce[token] = (now + self._coalesce_ms / 1000.0, None)
