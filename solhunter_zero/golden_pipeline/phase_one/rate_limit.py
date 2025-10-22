"""Per-host concurrency and request rate limiting for Golden Stream."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Mapping, Optional


@dataclass(slots=True)
class _BucketState:
    capacity: float
    refill_rate: float
    tokens: float
    updated_at: float


class TokenBucket:
    """Token bucket limiting coroutine throughput."""

    __slots__ = ("_state", "_lock")

    def __init__(self, *, capacity: float, refill_rate: float) -> None:
        if capacity <= 0 or refill_rate <= 0:
            raise ValueError("capacity and refill_rate must be positive")
        now = time.monotonic()
        self._state = _BucketState(
            capacity=float(capacity),
            refill_rate=float(refill_rate),
            tokens=float(capacity),
            updated_at=now,
        )
        self._lock = asyncio.Lock()

    @property
    def capacity(self) -> float:
        return self._state.capacity

    @property
    def refill_rate(self) -> float:
        return self._state.refill_rate

    async def acquire(self, amount: float = 1.0) -> float:
        if amount <= 0:
            return 0.0
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._state.updated_at)
                if elapsed:
                    self._state.tokens = min(
                        self._state.capacity,
                        self._state.tokens + elapsed * self._state.refill_rate,
                    )
                    self._state.updated_at = now
                if self._state.tokens >= amount:
                    self._state.tokens -= amount
                    return 0.0
                deficit = amount - self._state.tokens
                wait = deficit / self._state.refill_rate
                self._state.updated_at = now
            await asyncio.sleep(wait)
            # Loop to check again after sleeping.

    async def restore(self, tokens: float) -> None:
        async with self._lock:
            clamped = max(0.0, min(self._state.capacity, float(tokens)))
            self._state.tokens = clamped
            self._state.updated_at = time.monotonic()

    async def snapshot(self) -> Dict[str, float]:
        async with self._lock:
            now = time.monotonic()
            elapsed = max(0.0, now - self._state.updated_at)
            if elapsed:
                self._state.tokens = min(
                    self._state.capacity,
                    self._state.tokens + elapsed * self._state.refill_rate,
                )
                self._state.updated_at = now
            return {
                "capacity": self._state.capacity,
                "tokens": self._state.tokens,
                "refill_rate": self._state.refill_rate,
            }


class HostRateLimiter:
    """Coordinate per-host concurrency and request budgets."""

    def __init__(self) -> None:
        self._semaphores: Dict[str, asyncio.Semaphore] = {}
        self._buckets: Dict[str, TokenBucket] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self._limits: Dict[str, int] = {}

    def configure_host(self, host: str, *, concurrency: int, rps: float) -> None:
        if concurrency <= 0:
            raise ValueError("concurrency must be positive")
        if rps <= 0:
            raise ValueError("rps must be positive")
        key = host.lower()
        self._semaphores[key] = asyncio.Semaphore(concurrency)
        burst = max(float(concurrency), float(rps) * 2.0)
        self._buckets[key] = TokenBucket(capacity=burst, refill_rate=float(rps))
        self._locks.setdefault(key, asyncio.Lock())
        self._limits[key] = int(concurrency)

    def ensure_host(self, host: str) -> None:
        key = host.lower()
        self._locks.setdefault(key, asyncio.Lock())
        self._semaphores.setdefault(key, asyncio.Semaphore(1))
        self._buckets.setdefault(key, TokenBucket(capacity=1.0, refill_rate=1.0))
        self._limits.setdefault(key, 1)

    def limiter(self, host: str) -> "_LimiterContext":
        key = host.lower()
        if key not in self._semaphores:
            raise KeyError(f"host {host} not configured")
        return _LimiterContext(self._semaphores[key], self._buckets[key])

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        info: Dict[str, Dict[str, float]] = {}
        for host, bucket in self._buckets.items():
            data = await bucket.snapshot()
            sem = self._semaphores.get(host)
            limit = float(self._limits.get(host, 1))
            if sem is not None:
                # ``Semaphore`` does not expose the configured limit directly. We
                # derive the current in-flight count from the stored limit.
                available = float(max(0, sem._value))  # type: ignore[attr-defined]
                inflight = max(0.0, limit - available)
                data["available_slots"] = available
                data["limit"] = limit
                data["in_flight"] = inflight
            info[host] = data
        return info

    async def restore(self, snapshot: Mapping[str, Mapping[str, float]]) -> None:
        for host, meta in snapshot.items():
            key = host.lower()
            bucket = self._buckets.get(key)
            if bucket is None:
                continue
            tokens = meta.get("tokens") if isinstance(meta, Mapping) else None
            if tokens is None:
                continue
            await bucket.restore(tokens)


class _LimiterContext:
    def __init__(self, sem: asyncio.Semaphore, bucket: TokenBucket) -> None:
        self._sem = sem
        self._bucket = bucket

    async def __aenter__(self) -> None:
        await self._sem.acquire()
        await self._bucket.acquire(1.0)

    async def __aexit__(self, exc_type, exc, tb) -> Optional[bool]:
        self._sem.release()
        return None

