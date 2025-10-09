"""Discovery stage of the Golden Snapshot pipeline."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Awaitable, Callable, Iterable, Optional

from .types import DiscoveryCandidate
from .utils import CircuitBreaker, TTLCache

log = logging.getLogger(__name__)

_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


class DiscoveryStage:
    """Filters and forwards discovered mints."""

    def __init__(
        self,
        emit: Callable[[DiscoveryCandidate], Awaitable[None]],
        *,
        dedupe_ttl: float = 24 * 3600.0,
        failure_threshold: int = 5,
        failure_window: float = 30.0,
        cooldown: float = 60.0,
        allow_program_prefixes: Optional[Iterable[str]] = None,
    ) -> None:
        self._emit = emit
        self._seen = TTLCache()
        self._dedupe_ttl = dedupe_ttl
        self._allow_program_prefixes = set(allow_program_prefixes or [])
        self._breaker = CircuitBreaker(
            threshold=failure_threshold,
            window_sec=failure_window,
            cooldown_sec=cooldown,
        )
        self._lock = asyncio.Lock()

    async def submit(self, candidate: DiscoveryCandidate) -> bool:
        """Validate and forward ``candidate`` if it passes all gates."""

        async with self._lock:
            if self._breaker.is_open:
                log.debug("Discovery circuit breaker open; dropping %s", candidate.mint)
                return False

            if not self._validate(candidate.mint):
                log.debug("Rejected discovery candidate %s", candidate.mint)
                self._breaker.record_failure()
                return False

            if not self._seen.add(candidate.mint, self._dedupe_ttl):
                return False

            try:
                await self._emit(candidate)
            except Exception:  # pragma: no cover - defensive
                log.exception("Failed to emit discovery candidate %s", candidate.mint)
                self._breaker.record_failure()
                return False

            self._breaker.record_success()
            return True

    def _validate(self, mint: str) -> bool:
        if not mint or not _BASE58_RE.match(mint):
            return False
        if self._allow_program_prefixes:
            return True
        program_prefixes = {
            "JUP",  # Jupiter router
            "Tokenkeg",
            "Token2022",
            "Vote111",
        }
        if any(mint.startswith(prefix) for prefix in program_prefixes):
            return False
        return True

    def mark_failure(self) -> None:
        """Record an external failure (e.g. upstream RPC error)."""

        self._breaker.record_failure()

    def mark_success(self) -> None:
        self._breaker.record_success()

    @property
    def circuit_open(self) -> bool:
        return self._breaker.is_open

    def seen_recently(self, mint: str) -> bool:
        """Return ``True`` if ``mint`` has been observed within the TTL."""

        return self._seen.is_active(mint)
