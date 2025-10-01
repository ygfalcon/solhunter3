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
        limit: Optional[int] = None,
        offline: bool = False,
        token_file: Optional[str] = None,
    ) -> None:
        self.queue = queue
        self.interval = max(0.1, float(interval))
        self.cache_ttl = max(0.0, float(cache_ttl))
        self.limit = limit
        self.offline = offline
        self.token_file = token_file
        self._agent = DiscoveryAgent()
        self._last_tokens: list[str] = []
        self._last_ts: float = 0.0
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
        if self.cache_ttl and self._last_tokens and (now - self._last_ts) < self.cache_ttl:
            return list(self._last_tokens)
        tokens = await self._agent.discover_tokens(
            offline=self.offline,
            token_file=self.token_file,
        )
        if self.limit:
            tokens = tokens[: self.limit]
        if tokens:
            self._last_tokens = list(tokens)
            self._last_ts = now
        return tokens

    def _build_candidates(self, tokens: Iterable[str]) -> list[TokenCandidate]:
        ts = time.time()
        return [TokenCandidate(token=str(tok), source="discovery", discovered_at=ts) for tok in tokens]
