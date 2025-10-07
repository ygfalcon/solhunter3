import asyncio
import logging
import os
from pathlib import Path
from typing import Sequence

from sqlalchemy import select

import aiohttp
from .http import get_session

from .offline_data import OfflineData, MarketSnapshot
from .data_pipeline import map_snapshot
from .agents.discovery import (
    DEFAULT_DISCOVERY_METHOD,
    DiscoveryAgent,
    resolve_discovery_method,
)
from .simulation import DEFAULT_METRICS_BASE_URL
from .news import fetch_sentiment_async
import threading

_sched_task: asyncio.Future | None = None
_sched_loop: asyncio.AbstractEventLoop | None = None

logger = logging.getLogger(__name__)

DEFAULT_LIMIT_GB = 50.0


async def _prune_db(data: OfflineData, db_path: str, limit_gb: float) -> None:
    """Remove the oldest ``MarketSnapshot`` rows until ``db_path`` drops below
    ``limit_gb`` in size."""

    path = Path(db_path)
    limit_bytes = limit_gb * 1024 ** 3
    while path.exists() and path.stat().st_size > limit_bytes:
        async with data.Session() as session:
            result = await session.execute(
                select(MarketSnapshot).order_by(MarketSnapshot.timestamp).limit(1)
            )
            snap = result.scalars().first()
            if snap is None:
                break
            await session.delete(snap)
            await session.commit()


async def sync_snapshots(
    tokens: Sequence[str],
    *,
    days: int = 3,
    db_path: str = "offline_data.db",
    base_url: str | None = None,
    limit_gb: float | None = None,
    concurrency: int = 5,
) -> None:
    """Download order-book snapshots and insert them into ``db_path``."""

    base_url = base_url or os.getenv("METRICS_BASE_URL", DEFAULT_METRICS_BASE_URL)
    limit_gb = (
        float(os.getenv("OFFLINE_DATA_LIMIT_GB", DEFAULT_LIMIT_GB))
        if limit_gb is None
        else limit_gb
    )
    data = OfflineData(f"sqlite:///{db_path}")

    feeds = [u for u in os.getenv("NEWS_FEEDS", "").split(",") if u]
    twitter_feeds = [u for u in os.getenv("TWITTER_FEEDS", "").split(",") if u]
    discord_feeds = [u for u in os.getenv("DISCORD_FEEDS", "").split(",") if u]
    sentiment = 0.0
    if feeds or twitter_feeds or discord_feeds:
        try:
            sentiment = await fetch_sentiment_async(
                feeds,
                twitter_urls=twitter_feeds,
                discord_urls=discord_feeds,
            )
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("failed to fetch sentiment: %s", exc)

    sem = asyncio.Semaphore(concurrency)
    session = await get_session()

    async def fetch_and_log(token: str) -> None:
        url = f"{base_url.rstrip('/')}/token/{token}/history?days={days}"
        try:
            async with sem:
                async with session.get(url, timeout=10) as resp:
                    resp.raise_for_status()
                    resp_data = await resp.json()
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("failed to fetch snapshots for %s: %s", token, exc)
            return
        for snap in resp_data.get("snapshots", []):
            try:
                mapped = map_snapshot(snap)
                mapped["sentiment"] = sentiment
                await data.log_snapshot(token=token, **mapped)
            except Exception as exc:  # pragma: no cover - bad data
                logger.warning("invalid snapshot for %s: %s", token, exc)


    await asyncio.gather(*(fetch_and_log(t) for t in tokens))

    await _prune_db(data, db_path, limit_gb)


async def sync_recent(days: int = 3, db_path: str = "offline_data.db") -> None:
    """Discover tokens and sync recent snapshots."""

    method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
    if method is None:
        method = DEFAULT_DISCOVERY_METHOD
    agent = DiscoveryAgent()
    tokens = await agent.discover_tokens(method=method)
    if tokens:
        await sync_snapshots(tokens, days=days, db_path=db_path)


async def _schedule_loop(interval: float, days: int, db_path: str, limit_gb: float) -> None:
    while True:
        try:
            await sync_recent(days=days, db_path=db_path)
        except Exception as exc:
            logger.warning("periodic sync failed: %s", exc)
        try:
            data = OfflineData(f"sqlite:///{db_path}")
            await _prune_db(data, db_path, limit_gb)
        except Exception as exc:
            logger.warning("pruning failed: %s", exc)
        await asyncio.sleep(interval)


def start_scheduler(
    interval: float = 3600.0,
    *,
    days: int = 3,
    db_path: str = "offline_data.db",
    limit_gb: float | None = None,
) -> asyncio.Future:
    """Start background task that periodically syncs recent snapshots."""

    global _sched_task, _sched_loop
    if _sched_task is not None and not _sched_task.done():
        return _sched_task
    if limit_gb is None:
        limit_gb = float(os.getenv("OFFLINE_DATA_LIMIT_GB", DEFAULT_LIMIT_GB))
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if _sched_loop is None:
            _sched_loop = asyncio.new_event_loop()
            threading.Thread(target=_sched_loop.run_forever, daemon=True).start()
        loop = _sched_loop
        _sched_task = asyncio.run_coroutine_threadsafe(
            _schedule_loop(interval, days, db_path, limit_gb), loop
        )
        return _sched_task
    _sched_task = loop.create_task(_schedule_loop(interval, days, db_path, limit_gb))
    return _sched_task


def stop_scheduler() -> None:
    """Cancel the running scheduler task, if any."""
    global _sched_task
    if _sched_task is not None:
        _sched_task.cancel()
        _sched_task = None

