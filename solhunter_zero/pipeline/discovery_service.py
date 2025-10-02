from __future__ import annotations

import asyncio
import logging
import time
from typing import Iterable, Optional

from ..agents.discovery import DiscoveryAgent
from .types import TokenCandidate

log = logging.getLogger(__name__)


class DiscoveryService:
    """Produce ``TokenCandidate`` batches for downstream scoring."""

    def __init__(
        self,
        queue: "asyncio.Queue[list[TokenCandidate]]",
        *,
        interval: float = 5.0,
        cache_ttl: float = 20.0,
        empty_cache_ttl: Optional[float] = None,
        backoff_factor: float = 2.0,
        max_backoff: Optional[float] = None,
        limit: Optional[int] = None,
        offline: bool = False,
        token_file: Optional[str] = None,
    ) -> None:
        self.queue = queue
        self.interval = max(0.1, float(interval))
        self.cache_ttl = max(0.0, float(cache_ttl))
        if empty_cache_ttl is None:
            empty_cache_ttl = self.cache_ttl
        self.empty_cache_ttl = max(0.0, float(empty_cache_ttl)) if empty_cache_ttl is not None else 0.0
        self.backoff_factor = max(1.0, float(backoff_factor))
        self.max_backoff = None if max_backoff is None else max(0.0, float(max_backoff))
        self.limit = limit
        self.offline = offline
        self.token_file = token_file
        self._agent = DiscoveryAgent()
        self._last_tokens: list[str] = []
        self._last_fetch_ts: float = 0.0
        self._cooldown_until: float = 0.0
        self._consecutive_empty: int = 0
        self._current_backoff: float = 0.0
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="discovery_service")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                tokens = await self._fetch()
                if tokens:
                    batch = self._build_candidates(tokens)
                    await self.queue.put(batch)
                    log.info("DiscoveryService queued %d tokens", len(batch))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive logging
                log.exception("DiscoveryService failure: %s", exc)
            await asyncio.sleep(self.interval)

    async def _fetch(self) -> list[str]:
        now = time.time()
        if now < self._cooldown_until:
            remaining = self._cooldown_until - now
            log.debug(
                "DiscoveryService cooldown active for %.2fs (last fetch yielded %d tokens)",
                remaining,
                len(self._last_tokens),
            )
            return list(self._last_tokens)
        tokens = await self._agent.discover_tokens(
            offline=self.offline,
            token_file=self.token_file,
        )
        if self.limit:
            tokens = tokens[: self.limit]
        fetch_ts = time.time()
        self._last_fetch_ts = fetch_ts
        self._last_tokens = list(tokens)

        cooldown = 0.0
        if tokens:
            self._consecutive_empty = 0
            self._current_backoff = 0.0
            if self.cache_ttl:
                cooldown = self.cache_ttl
        else:
            self._consecutive_empty += 1
            base_ttl = self.empty_cache_ttl
            if base_ttl:
                if self.backoff_factor > 1.0:
                    cooldown = base_ttl * (self.backoff_factor ** (self._consecutive_empty - 1))
                else:
                    cooldown = base_ttl
            self._current_backoff = cooldown

        if self.max_backoff is not None and cooldown:
            cooldown = min(cooldown, self.max_backoff)

        if cooldown:
            self._cooldown_until = fetch_ts + cooldown
            if tokens:
                log.info(
                    "DiscoveryService applying cache cooldown of %.2fs after %d tokens",
                    cooldown,
                    len(tokens),
                )
            else:
                log.info(
                    "DiscoveryService empty fetch #%d; backoff for %.2fs",
                    self._consecutive_empty,
                    cooldown,
                )
        else:
            self._cooldown_until = fetch_ts

        return list(tokens)

    def _build_candidates(self, tokens: Iterable[str]) -> list[TokenCandidate]:
        ts = time.time()
        return [TokenCandidate(token=str(tok), source="discovery", discovered_at=ts) for tok in tokens]
