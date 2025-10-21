"""Asynchronous key/value helpers used across the Golden pipeline."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Optional, Protocol


class KeyValueStore(Protocol):
    """Protocol describing the minimal async KV operations we rely on."""

    async def get(self, key: str) -> Optional[str]:
        ...

    async def set(self, key: str, value: str, *, ttl: float | None = None) -> None:
        ...

    async def set_if_absent(
        self, key: str, value: str, *, ttl: float | None = None
    ) -> bool:
        ...

    async def delete(self, key: str) -> None:
        ...

    async def scan_prefix(self, prefix: str) -> list[tuple[str, str]]:
        ...


@dataclass
class _Entry:
    value: str
    expires_at: float | None


class InMemoryKeyValueStore(KeyValueStore):
    """A tiny async-safe KV store with TTL support for tests and local runs."""

    def __init__(self) -> None:
        self._data: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[str]:
        async with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            if entry.expires_at is not None and entry.expires_at <= time.time():
                self._data.pop(key, None)
                return None
            return entry.value

    async def set(self, key: str, value: str, *, ttl: float | None = None) -> None:
        async with self._lock:
            self._data[key] = _Entry(value=value, expires_at=_resolve_expiry(ttl))

    async def set_if_absent(
        self, key: str, value: str, *, ttl: float | None = None
    ) -> bool:
        async with self._lock:
            entry = self._data.get(key)
            now = time.time()
            if entry and (entry.expires_at is None or entry.expires_at > now):
                return False
            self._data[key] = _Entry(value=value, expires_at=_resolve_expiry(ttl))
            return True

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._data.pop(key, None)

    async def scan_prefix(self, prefix: str) -> list[tuple[str, str]]:
        async with self._lock:
            now = time.time()
            items: list[tuple[str, str]] = []
            for key, entry in list(self._data.items()):
                if entry.expires_at is not None and entry.expires_at <= now:
                    self._data.pop(key, None)
                    continue
                if key.startswith(prefix):
                    items.append((key, entry.value))
            return items

    async def purge_expired(self) -> None:
        async with self._lock:
            now = time.time()
            expired = [
                key
                for key, entry in self._data.items()
                if entry.expires_at is not None and entry.expires_at <= now
            ]
            for key in expired:
                self._data.pop(key, None)


def _resolve_expiry(ttl: float | None) -> float | None:
    if ttl is None or ttl <= 0:
        return None if ttl is None else time.time() - 1.0
    return time.time() + ttl


__all__ = ["KeyValueStore", "InMemoryKeyValueStore"]
