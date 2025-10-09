"""Enrichment stage producing token metadata."""

from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Dict, Iterable

from .types import DiscoveryCandidate, TokenSnapshot
from .utils import TTLCache

log = logging.getLogger(__name__)


EnrichmentFetcher = Callable[[Iterable[str]], Awaitable[Dict[str, TokenSnapshot]]]


class EnrichmentStage:
    """Fetch metadata for discovered tokens and emit TokenSnapshots."""

    def __init__(
        self,
        emit: Callable[[TokenSnapshot], Awaitable[None]],
        *,
        fetcher: EnrichmentFetcher,
        cache_ttl: float = 60.0,
        batch_size: int = 75,
    ) -> None:
        self._emit = emit
        self._fetcher = fetcher
        self._cache = TTLCache()
        self._cache_ttl = cache_ttl
        self._batch_size = batch_size
        self._lock = asyncio.Lock()

    async def submit(self, candidates: Iterable[DiscoveryCandidate]) -> None:
        to_fetch = []
        async with self._lock:
            for candidate in candidates:
                if not self._cache.add(candidate.mint, self._cache_ttl):
                    continue
                to_fetch.append(candidate.mint)
        if not to_fetch:
            return

        # Fetch metadata in batches to respect API limits.
        for i in range(0, len(to_fetch), self._batch_size or len(to_fetch)):
            batch = to_fetch[i : i + (self._batch_size or len(to_fetch))]
            if not batch:
                continue
            try:
                metadata = await self._fetcher(batch)
            except Exception:  # pragma: no cover - defensive
                log.exception("Enrichment fetcher failed for batch size %d", len(batch))
                for mint in batch:
                    self._cache.add(mint, -1)
                continue

            for mint, snapshot in metadata.items():
                await self._emit(snapshot)
