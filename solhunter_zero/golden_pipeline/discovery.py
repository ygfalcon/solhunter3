"""Discovery stage of the Golden Snapshot pipeline."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Awaitable, Callable, Iterable, Optional

from .contracts import discovery_cursor_key, discovery_seen_key
from .kv import KeyValueStore
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
        kv: KeyValueStore | None = None,
    ) -> None:
        self._emit = emit
        self._seen = TTLCache()
        self._kv = kv
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

            if self._kv:
                key = discovery_seen_key(candidate.mint)
                stored = await self._kv.set_if_absent(
                    key,
                    "1",
                    ttl=self._dedupe_ttl,
                )
                if not stored:
                    return False
                self._seen.add(candidate.mint, self._dedupe_ttl)
            else:
                if not self._seen.add(candidate.mint, self._dedupe_ttl):
                    return False

            try:
                await self._emit(candidate)
            except Exception:  # pragma: no cover - defensive
                log.exception("Failed to emit discovery candidate %s", candidate.mint)
                self._breaker.record_failure()
                return False

            cursor_value = getattr(candidate, "cursor", None)
            if cursor_value:
                try:
                    cursor_text = str(cursor_value).strip()
                except Exception:
                    cursor_text = ""
                if cursor_text:
                    try:
                        await self.persist_cursor(cursor_text)
                    except Exception:  # pragma: no cover - defensive persistence
                        log.debug("Discovery cursor persistence failed", exc_info=True)

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

    @property
    def failure_count(self) -> int:
        """Return the number of recent DAS failures observed by the breaker."""

        return self._breaker.failure_count()

    def breaker_state(self) -> dict[str, float | int | bool]:
        """Expose breaker telemetry for monitoring and tests."""

        state = self._breaker.snapshot()
        state.update(
            threshold=self._breaker.threshold,
            window_sec=self._breaker.window_sec,
            cooldown_sec=self._breaker.cooldown_sec,
        )
        return state

    def seen_recently(self, mint: str) -> bool:
        """Return ``True`` if ``mint`` has been observed within the TTL."""

        return self._seen.is_active(mint)

    async def persist_cursor(self, cursor: str) -> None:
        """Persist the upstream cursor so discovery can resume on restart."""

        if self._kv:
            await self._kv.set(discovery_cursor_key(), cursor)

    async def load_cursor(self) -> str | None:
        if not self._kv:
            return None
        return await self._kv.get(discovery_cursor_key())
