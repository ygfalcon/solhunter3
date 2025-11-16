"""Discovery stage of the Golden Snapshot pipeline."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Iterable, Mapping, Optional

from .contracts import discovery_cursor_key, discovery_seen_key
from .kv import KeyValueStore
from .types import DiscoveryCandidate
from .utils import CircuitBreaker, TTLCache

log = logging.getLogger(__name__)

_BASE58_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


class BloomFilter:
    """Simple Bloom filter used to guard duplicate discovery per cycle."""

    def __init__(self, size: int = 1 << 14, hashes: int = 5) -> None:
        self.size = max(8, size)
        self.hashes = max(1, hashes)
        self._bits = bytearray((self.size + 7) // 8)

    def _iter_hashes(self, value: str) -> Iterable[int]:
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        for i in range(self.hashes):
            start = i * 4
            chunk = digest[start : start + 4]
            if len(chunk) < 4:
                chunk = (chunk + b"\x00" * 4)[:4]
            yield int.from_bytes(chunk, "big") % self.size

    def add(self, value: str) -> bool:
        seen = True
        for idx in self._iter_hashes(value):
            byte = idx // 8
            mask = 1 << (idx % 8)
            if not self._bits[byte] & mask:
                seen = False
                self._bits[byte] |= mask
        return not seen

    def clear(self) -> None:
        for i in range(len(self._bits)):
            self._bits[i] = 0


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
        on_metrics: Callable[[Mapping[str, int]], None] | None = None,
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
        self._metrics = DiscoveryStageMetrics()
        self._on_metrics = on_metrics
        self._bloom = BloomFilter()
        try:
            window_raw = os.getenv("DISCOVERY_BLOOM_WINDOW_SEC", "60")
            self._bloom_window = float(window_raw or 60.0)
        except Exception:
            self._bloom_window = 60.0
        if self._bloom_window <= 0:
            self._bloom_window = 0.0
        self._bloom_next_reset = (
            time.monotonic() + self._bloom_window if self._bloom_window > 0 else 0.0
        )

    async def submit(self, candidate: DiscoveryCandidate) -> bool:
        """Validate and forward ``candidate`` if it passes all gates."""

        accepted = False
        deduped = False
        emit_candidate: DiscoveryCandidate | None = None
        rejected = False

        async with self._lock:
            self._maybe_reset_bloom()

            if self._breaker.is_open:
                log.debug("Discovery circuit breaker open; dropping %s", candidate.mint)
            elif not self._validate(candidate.mint):
                log.debug("Rejected discovery candidate %s", candidate.mint)
                rejected = True
            else:
                if not self._bloom.add(candidate.mint):
                    deduped = True
                elif self._kv:
                    key = discovery_seen_key(candidate.mint)
                    stored: bool | None = None
                    try:
                        stored = await self._kv.set_if_absent(
                            key,
                            "1",
                            ttl=self._dedupe_ttl,
                        )
                    except Exception:
                        log.exception(
                            "Persistent dedupe store rejected key %s", key
                        )
                        self._record_failure()
                        self._breaker.open_for(self._breaker.cooldown_sec)
                    if stored is False:
                        deduped = True
                        self._seen.add(candidate.mint, self._dedupe_ttl)
                    elif stored:
                        self._seen.add(candidate.mint, self._dedupe_ttl)
                    else:
                        if not self._seen.add(candidate.mint, self._dedupe_ttl):
                            deduped = True
                else:
                    if not self._seen.add(candidate.mint, self._dedupe_ttl):
                        deduped = True

                if deduped:
                    self._metrics.dedupe_drops += 1
                else:
                    emit_candidate = candidate

        if rejected:
            self._record_failure()
        elif emit_candidate:
            try:
                await self._emit(emit_candidate)
            except Exception:  # pragma: no cover - defensive
                log.exception(
                    "Failed to emit discovery candidate %s", candidate.mint
                )
                self._record_failure()
            else:
                self._record_success()
                accepted = True

        self._forward_metrics()
        return accepted

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

        self._record_failure()
        self._forward_metrics()

    def mark_success(self) -> None:
        self._record_success()
        self._forward_metrics()

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

    @property
    def metrics(self) -> "DiscoveryStageMetrics":
        return self._metrics

    async def persist_cursor(self, cursor: str) -> None:
        """Persist the upstream cursor so discovery can resume on restart."""

        if self._kv:
            try:
                await self._kv.set(discovery_cursor_key(), cursor)
            except Exception:  # pragma: no cover - defensive
                log.exception("Failed to persist discovery cursor")
                was_open = self._breaker.is_open
                self._record_failure()
                breaker_opened = not was_open and self._breaker.is_open
                if not breaker_opened:
                    cooldown = getattr(self._breaker, "cooldown_sec", 0.0)
                    if cooldown <= 0:
                        cooldown = 60.0
                    self._breaker.open_for(cooldown)
                    if not was_open and self._breaker.is_open:
                        self._metrics.breaker_openings += 1

    async def load_cursor(self) -> str | None:
        if not self._kv:
            return None
        return await self._kv.get(discovery_cursor_key())

    def _maybe_reset_bloom(self) -> None:
        if self._bloom_window <= 0:
            return
        now = time.monotonic()
        if now >= self._bloom_next_reset:
            self._bloom.clear()
            self._bloom_next_reset = now + self._bloom_window

    def _record_success(self) -> None:
        self._breaker.record_success()
        self._metrics.successes += 1

    def _record_failure(self) -> None:
        was_open = self._breaker.is_open
        self._breaker.record_failure()
        self._metrics.failures += 1
        if not was_open and self._breaker.is_open:
            self._metrics.breaker_openings += 1

    def _forward_metrics(self) -> None:
        if not self._on_metrics:
            return
        try:
            self._on_metrics(self._metrics.snapshot())
        except Exception:  # pragma: no cover - defensive guard
            log.exception("Discovery metrics callback failed")


@dataclass(slots=True)
class DiscoveryStageMetrics:
    """Monotonic counters exposed by :class:`DiscoveryStage`."""

    successes: int = 0
    failures: int = 0
    dedupe_drops: int = 0
    breaker_openings: int = 0

    def snapshot(self) -> dict[str, int]:
        return {
            "success_total": self.successes,
            "failure_total": self.failures,
            "dedupe_drops": self.dedupe_drops,
            "breaker_openings": self.breaker_openings,
        }
