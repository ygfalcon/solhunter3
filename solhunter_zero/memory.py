from __future__ import annotations

import asyncio
import datetime
import json
import logging
import os
from contextlib import suppress
from typing import Any

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base

from .base_memory import BaseMemory
from .event_bus import publish
from .schemas import TradeLogged

Base = declarative_base()
logger = logging.getLogger(__name__)


def utcnow() -> datetime.datetime:
    return datetime.datetime.utcnow()


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True)
    token = Column(String, nullable=False, index=True)
    direction = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=utcnow)
    reason = Column(Text)
    created_at = Column(DateTime, default=utcnow, index=True)

    __table_args__ = (
        Index("ix_trades_token_created", "token", "created_at"),
        Index("ix_trades_id", "id"),
    )


class VaRLog(Base):
    __tablename__ = "var_logs"

    id = Column(Integer, primary_key=True)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=utcnow)


def load_snapshot(path: str) -> list[dict[str, Any]]:
    """Deserialize memory snapshot from ``path`` (list[dict] or {"trades": [...]})"""
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return []
    trades = data.get("trades", data) if isinstance(data, dict) else data
    return [t for t in trades if isinstance(t, dict)]


class Memory(BaseMemory):
    def __init__(self, url: str = "sqlite:///memory.db"):
        if url.startswith("sqlite:///"):
            url = url.replace("sqlite://", "sqlite+aiosqlite://", 1)

        self.engine = create_async_engine(url, echo=False, future=True)
        self.Session: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine, expire_on_commit=False
        )

        self._queue: asyncio.Queue[dict[str, Any]] | None = None
        self._writer_task: asyncio.Task | None = None
        self._writer_started: asyncio.Event | None = None

        self._batch_size = int(os.getenv("MEMORY_BATCH_SIZE", "100") or 100)
        self._interval = float(os.getenv("MEMORY_FLUSH_INTERVAL", "1.0") or 1.0)
        self._queue_max = int(os.getenv("MEMORY_QUEUE_MAX", "0") or 0)
        self._dedupe_window_sec = float(
            os.getenv("MEMORY_DEDUPE_WINDOW_SEC", "0") or 0.0
        )

        self._last_seen_created: dict[str, datetime.datetime] = {}
        self._recent_keys: set[tuple[str, str, float, float, int]] = set()

        self._init_task = asyncio.create_task(self._init_models())

    async def _init_models(self) -> None:
        async with self.engine.begin() as conn:
            try:
                if str(self.engine.url).startswith("sqlite+aiosqlite"):
                    await conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
                    await conn.exec_driver_sql("PRAGMA synchronous=NORMAL;")
                    await conn.exec_driver_sql("PRAGMA temp_store=MEMORY;")
            except Exception:
                logger.debug("SQLite PRAGMA tuning failed", exc_info=True)
            await conn.run_sync(Base.metadata.create_all)

    def start_writer(
        self, batch_size: int | None = None, interval: float | None = None
    ) -> None:
        """Start background writer flushing queued trades."""
        if self._writer_task and not self._writer_task.done():
            return

        if batch_size is not None:
            self._batch_size = int(batch_size)
        if interval is not None:
            self._interval = float(interval)

        maxsize = self._queue_max if self._queue_max > 0 else 0
        self._queue = asyncio.Queue(maxsize=maxsize)
        self._writer_started = asyncio.Event()
        loop = asyncio.get_event_loop()
        self._writer_task = loop.create_task(self._writer())

    async def _writer(self) -> None:
        assert self._queue is not None
        if self._writer_started and not self._writer_started.is_set():
            self._writer_started.set()

        pending: list[dict[str, Any]] = []
        try:
            while True:
                try:
                    item = await asyncio.wait_for(
                        self._queue.get(), timeout=self._interval
                    )
                    pending.append(item)
                    self._queue.task_done()
                    if len(pending) >= self._batch_size:
                        await self._flush(pending)
                        pending.clear()
                except asyncio.TimeoutError:
                    if pending:
                        await self._flush(pending)
                        pending.clear()
        except asyncio.CancelledError:
            pass
        finally:
            if pending:
                await self._flush(pending)
                pending.clear()
            if self._queue:
                tail: list[dict[str, Any]] = []
                while True:
                    try:
                        tail.append(self._queue.get_nowait())
                        self._queue.task_done()
                    except asyncio.QueueEmpty:
                        break
                if tail:
                    await self._flush(tail)

    async def wait_ready(self) -> None:
        await self._init_task
        if self._writer_started is not None:
            await self._writer_started.wait()

    async def _flush(self, items: list[dict[str, Any]]) -> None:
        if not items:
            return
        async with self.Session() as session:
            try:
                session.add_all(Trade(**d) for d in items)
                await session.commit()
            except Exception:
                logger.exception("memory flush failed; batch discarded")

    async def log_trade(self, *, _broadcast: bool = True, **kwargs) -> int | None:
        await self._init_task

        created_at = kwargs.get("created_at") or utcnow()
        kwargs["created_at"] = created_at

        if self._dedupe_window_sec > 0:
            key = (
                str(kwargs.get("token")),
                str(kwargs.get("direction")),
                float(kwargs.get("amount", 0.0)),
                float(kwargs.get("price", 0.0)),
                int(
                    (kwargs.get("timestamp") or created_at).timestamp()
                    // max(1, int(self._dedupe_window_sec))
                ),
            )
            if key in self._recent_keys:
                return None
            self._recent_keys.add(key)

        token = str(kwargs.get("token", ""))
        if token:
            prev = self._last_seen_created.get(token)
            if prev is None or created_at > prev:
                self._last_seen_created[token] = created_at

        if self._queue is not None:
            try:
                if self._queue_max > 0 and self._queue.full():
                    try:
                        _ = self._queue.get_nowait()
                        self._queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                await self._queue.put(kwargs)
            except Exception:
                logger.exception("failed to enqueue trade")
            if _broadcast:
                with suppress(Exception):
                    publish("trade_logged", TradeLogged(**kwargs))
            return None

        async with self.Session() as session:
            trade = Trade(**kwargs)
            session.add(trade)
            await session.commit()
            if _broadcast:
                with suppress(Exception):
                    publish("trade_logged", TradeLogged(**kwargs))
            return trade.id

    async def latest_trade_time(self, token: str) -> datetime.datetime | None:
        await self._init_task
        in_mem = self._last_seen_created.get(token)

        async with self.Session() as session:
            q = (
                select(Trade.created_at)
                .filter(Trade.token == token)
                .order_by(Trade.created_at.desc())
                .limit(1)
            )
            result = await session.execute(q)
            db_time = result.scalar_one_or_none()

        if in_mem and (db_time is None or in_mem > db_time):
            return in_mem
        return db_time

    async def _log_var_async(self, value: float) -> None:
        await self._init_task
        async with self.Session() as session:
            session.add(VaRLog(value=value))
            await session.commit()

    def log_var(self, value: float):
        from .util import run_coro

        return run_coro(self._log_var_async(value))

    async def list_trades(
        self,
        *,
        token: str | None = None,
        limit: int | None = None,
        since_id: int | None = None,
    ) -> list[Trade]:
        await self._init_task
        async with self.Session() as session:
            q = select(Trade)
            if token is not None:
                q = q.filter(Trade.token == token)
            if since_id is not None:
                q = q.filter(Trade.id > since_id)
            q = q.order_by(Trade.id)
            if limit is not None:
                q = q.limit(limit)
            result = await session.execute(q)
            return list(result.scalars().all())

    async def _list_vars_async(self) -> list[VaRLog]:
        await self._init_task
        async with self.Session() as session:
            result = await session.execute(select(VaRLog))
            return list(result.scalars().all())

    def list_vars(self):
        from .util import run_coro

        return run_coro(self._list_vars_async())

    async def ingest_snapshot(
        self, path: str, *, broadcast: bool = False
    ) -> int:
        await self._init_task
        rows = load_snapshot(path)
        if not rows:
            return 0

        chunk = max(1, self._batch_size)
        total = 0
        for i in range(0, len(rows), chunk):
            batch = rows[i : i + chunk]
            async with self.Session() as session:
                session.add_all(Trade(**r) for r in batch)
                await session.commit()
            if broadcast:
                for r in batch:
                    with suppress(Exception):
                        publish("trade_logged", TradeLogged(**r))
            total += len(batch)
        return total

    async def close(self) -> None:
        await self._init_task
        if self._writer_task:
            self._writer_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._writer_task
            self._writer_task = None
            self._writer_started = None
        await self.engine.dispose()

    async def __aenter__(self) -> "Memory":
        await self._init_task
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
