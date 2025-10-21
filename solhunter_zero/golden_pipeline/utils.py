"""Utility helpers for the Golden Snapshot pipeline."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque
from dataclasses import asdict
from typing import Any, Callable, Deque, Dict, Iterable


def now_ts() -> float:
    """Return the current timestamp as a float."""

    return time.time()


class TTLCache:
    """A minimal TTL cache used for dedupe and circuit breaking."""

    __slots__ = ("_data",)

    def __init__(self) -> None:
        self._data: Dict[Any, float] = {}

    def add(self, key: Any, ttl: float) -> bool:
        """Return ``True`` if the key was added (i.e. not present or expired)."""

        expiry = self._data.get(key)
        now = now_ts()
        if expiry and expiry > now:
            return False
        if ttl <= 0:
            self._data.pop(key, None)
            return True
        self._data[key] = now + ttl
        return True

    def is_active(self, key: Any) -> bool:
        expiry = self._data.get(key)
        if not expiry:
            return False
        if expiry <= now_ts():
            self._data.pop(key, None)
            return False
        return True

    def clear_expired(self) -> None:
        now = now_ts()
        expired = [key for key, expiry in self._data.items() if expiry <= now]
        for key in expired:
            self._data.pop(key, None)


class CircuitBreaker:
    """Simple circuit breaker used to guard external integrations."""

    def __init__(self, *, threshold: int, window_sec: float, cooldown_sec: float) -> None:
        self.threshold = threshold
        self.window_sec = window_sec
        self.cooldown_sec = cooldown_sec
        self._failures: Deque[float] = deque()
        self._opened_until: float = 0.0

    def _prune(self) -> None:
        """Discard failures that fall outside the rolling window."""

        now = now_ts()
        while self._failures and now - self._failures[0] > self.window_sec:
            self._failures.popleft()

    def record_success(self) -> None:
        self._failures.clear()
        if self._opened_until and self._opened_until <= now_ts():
            self._opened_until = 0.0

    def record_failure(self) -> None:
        now = now_ts()
        self._failures.append(now)
        self._prune()
        if len(self._failures) >= self.threshold:
            self._opened_until = now + self.cooldown_sec

    @property
    def is_open(self) -> bool:
        self._prune()
        if self._opened_until <= 0:
            return False
        if self._opened_until <= now_ts():
            self._opened_until = 0.0
            self._failures.clear()
            return False
        return True

    def failure_count(self) -> int:
        """Return the number of recent failures within the breaker window."""

        self._prune()
        return len(self._failures)

    def snapshot(self) -> Dict[str, float | int | bool]:
        """Return a serialisable view of the breaker state for telemetry."""

        open_flag = self.is_open
        self._prune()
        now = now_ts()
        remaining = max(0.0, self._opened_until - now)
        return {
            "open": open_flag,
            "failure_count": len(self._failures),
            "opened_until": self._opened_until,
            "cooldown_remaining": remaining,
        }


def canonical_hash(payload: Any) -> str:
    """Hash arbitrary payload using canonical JSON encoding."""

    def _default(obj: Any) -> Any:
        if hasattr(obj, "to_dict"):
            return obj.to_dict()
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        raise TypeError(f"Object of type {type(obj)!r} is not JSON serialisable")

    encoded = json.dumps(payload, default=_default, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


async def gather_in_batches(
    items: Iterable[Any],
    *,
    batch_size: int,
    worker: Callable[[Iterable[Any]], "asyncio.Future[Any]"] | Callable[[Iterable[Any]], Any],
) -> list[Any]:
    """Run ``worker`` over ``items`` in batches and gather results."""

    if batch_size <= 0:
        batch_size = len(list(items)) or 1
    results: list[Any] = []
    batch: list[Any] = []
    for item in items:
        batch.append(item)
        if len(batch) >= batch_size:
            result = worker(batch)
            if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
                results.extend(await result)
            else:
                results.extend(result)
            batch = []
    if batch:
        result = worker(batch)
        if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
            results.extend(await result)
        else:
            results.extend(result)
    return results


def clamp(value: float, minimum: float, maximum: float) -> float:
    """Return ``value`` bounded by ``minimum`` and ``maximum``."""

    return max(minimum, min(maximum, value))
