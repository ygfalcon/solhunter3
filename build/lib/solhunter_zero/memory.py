from __future__ import annotations
import datetime
import os
import asyncio
import json
from contextlib import suppress
import logging
from sqlalchemy import (
    Column,
    Integer,
    Float,
    String,
    DateTime,
    Text,
    select,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)

from .base_memory import BaseMemory
from .event_bus import publish
from .schemas import TradeLogged

Base = declarative_base()

logger = logging.getLogger(__name__)

def utcnow():
    return datetime.datetime.utcnow()

class Trade(Base):
    __tablename__ = 'trades'
    id = Column(Integer, primary_key=True)
    token = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    amount = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=utcnow)
    reason = Column(Text)
    created_at = Column(DateTime, default=utcnow, index=True)


class VaRLog(Base):
    __tablename__ = "var_logs"

    id = Column(Integer, primary_key=True)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=utcnow)


def load_snapshot(path: str) -> list[dict]:
    """Deserialize memory snapshot from ``path``.

    The snapshot is expected to be a JSON file containing either a list of
    trade dictionaries or a mapping with a ``trades`` key. Entries are returned
    as a list of dictionaries suitable for :meth:`log_trade`.
    """

    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except Exception:
        return []
    if isinstance(data, dict):
        trades = data.get("trades", [])
    else:
        trades = data
    return [t for t in trades if isinstance(t, dict)]


class Memory(BaseMemory):
    def __init__(self, url: str = 'sqlite:///memory.db'):
        if url.startswith('sqlite:///'):
            url = url.replace('sqlite://', 'sqlite+aiosqlite://', 1)
        self.engine = create_async_engine(url, echo=False, future=True)
        self.Session = async_sessionmaker(bind=self.engine, expire_on_commit=False)
        self._queue: asyncio.Queue[dict] | None = None
        self._writer_task: asyncio.Task | None = None
        self._writer_started: asyncio.Event | None = None
        self._batch_size = 100
        self._interval = 1.0

        async def _init_models():
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        if loop.is_running():
            self._init_task = loop.create_task(_init_models())
        else:
            loop.run_until_complete(_init_models())
            self._init_task = loop.create_future()
            self._init_task.set_result(None)

    def start_writer(self, batch_size: int = 100, interval: float = 1.0) -> None:
        """Start background writer flushing queued trades."""
        env_batch = os.getenv("MEMORY_BATCH_SIZE")
        env_interval = os.getenv("MEMORY_FLUSH_INTERVAL")
        if env_batch is not None:
            batch_size = int(env_batch)
        if env_interval is not None:
            interval = float(env_interval)
        self._batch_size = batch_size
        self._interval = interval
        self._queue = asyncio.Queue()
        loop = asyncio.get_event_loop()
        self._writer_started = asyncio.Event()
        self._writer_task = loop.create_task(self._writer())

    async def _writer(self) -> None:
        assert self._queue is not None
        if self._writer_started and not self._writer_started.is_set():
            self._writer_started.set()
        pending: list[dict] = []
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
            while self._queue and not self._queue.empty():
                pending.append(self._queue.get_nowait())
                self._queue.task_done()
            if pending:
                await self._flush(pending)

    async def wait_ready(self) -> None:
        await self._init_task
        if self._writer_started is not None:
            await self._writer_started.wait()

    async def _flush(self, items: list[dict]) -> None:
        async with self.Session() as session:
            trades = [Trade(**d) for d in items]
            if trades:
                await session.run_sync(lambda s: s.bulk_save_objects(trades))
            await session.commit()

    async def log_trade(self, *, _broadcast: bool = True, **kwargs) -> int | None:
        await self._init_task
        kwargs.setdefault("created_at", utcnow())
        if self._queue is not None:
            await self._queue.put(kwargs)
            if _broadcast:
                try:
                    publish("trade_logged", TradeLogged(**kwargs))
                except Exception:
                    logger.warning("trade_logged publish failed", exc_info=True)
            return None
        async with self.Session() as session:
            trade = Trade(**kwargs)
            session.add(trade)
            await session.commit()
        if _broadcast:
            try:
                publish("trade_logged", TradeLogged(**kwargs))
            except Exception:
                logger.warning("trade_logged publish failed", exc_info=True)
        return trade.id

    async def latest_trade_time(self, token: str) -> datetime.datetime | None:
        """Return the most recent ``created_at`` for ``token`` if available."""
        await self._init_task
        async with self.Session() as session:
            q = (
                select(Trade.created_at)
                .filter(Trade.token == token)
                .order_by(Trade.created_at.desc())
                .limit(1)
            )
            result = await session.execute(q)
            return result.scalar_one_or_none()
    async def _log_var_async(self, value: float) -> None:
        await self._init_task
        async with self.Session() as session:
            rec = VaRLog(value=value)
            session.add(rec)
            await session.commit()

    def log_var(self, value: float):
        """Record a value-at-risk measurement."""
        from .util import run_coro

        return run_coro(self._log_var_async(value))

    async def list_trades(
        self,
        *,
        token: str | None = None,
        limit: int | None = None,
        since_id: int | None = None,
    ) -> list[Trade]:
        """Return trades optionally filtered by ``token`` or ``since_id``."""
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

    async def _list_vars_async(self):
        await self._init_task
        async with self.Session() as session:
            result = await session.execute(select(VaRLog))
            return list(result.scalars().all())

    def list_vars(self):
        from .util import run_coro

        return run_coro(self._list_vars_async())

    async def close(self) -> None:
        """Flush pending trades and dispose of the engine."""
        await self._init_task
        if self._writer_task:
            self._writer_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._writer_task
            self._writer_task = None
            self._writer_started = None
        if self._queue and not self._queue.empty():
            await self._flush([self._queue.get_nowait() for _ in range(self._queue.qsize())])
        await self.engine.dispose()

