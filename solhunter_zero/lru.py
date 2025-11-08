"""Lightweight LRU and TTL caches used throughout the project."""

from __future__ import annotations

import asyncio
import threading
import heapq
import time
from collections import OrderedDict
from typing import Any, Hashable

try:  # cachetools is optional when running tests
    from cachetools import LRUCache as _LRUCache
except Exception:  # pragma: no cover - fallback when cachetools missing
    class _LRUCache(dict):  # type: ignore
        def __init__(self, maxsize: int = 128, *args: Any, **kwargs: Any) -> None:
            self.maxsize = maxsize
            super().__init__(*args, **kwargs)

        def __setitem__(self, key: Hashable, value: Any) -> None:
            if len(self) >= self.maxsize:
                self.pop(next(iter(self)), None)
            super().__setitem__(key, value)

# Re-export :class:`cachetools.LRUCache` for external modules.
LRUCache = _LRUCache


class TTLCache:
    """A simple TTL cache usable from async code.

    The implementation intentionally avoids a dependency on ``cachetools`` so
    that tests can run without the optional package installed.  When
    ``cachetools`` is available the public API mirrors its behaviour closely.
    """

    def __init__(self, maxsize: int = 128, ttl: float = 60.0) -> None:
        self.maxsize = maxsize
        self.ttl = float(ttl)
        self._data: "OrderedDict[Hashable, tuple[Any, float]]" = OrderedDict()
        self._expiry_heap: list[tuple[float, Hashable]] = []
        # Track pending tasks per event loop to avoid cross-loop awaits
        self._pending: dict[tuple[Hashable, asyncio.AbstractEventLoop], asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._thread_lock = threading.RLock()

    # internal helpers -----------------------------------------------------
    def _purge(self) -> None:
        with self._thread_lock:
            now = time.monotonic()
            heap = self._expiry_heap
            data = self._data
            while heap and heap[0][0] <= now:
                exp, key = heapq.heappop(heap)
                item = data.get(key)
                if item is None:
                    continue
                _, real_exp = item
                if real_exp <= now:
                    data.pop(key, None)

    def _evict(self) -> None:
        with self._thread_lock:
            while len(self._data) > self.maxsize:
                self._data.popitem(last=False)

    # basic dict API -------------------------------------------------------
    def get(self, key: Hashable, default: Any = None) -> Any:
        with self._thread_lock:
            self._purge()
            item = self._data.get(key)
            if item is None:
                return default
            value, exp = item
            if exp <= time.monotonic():
                self._data.pop(key, None)
                return default
            return value

    def __contains__(self, key: Hashable) -> bool:  # pragma: no cover - trivial
        return self.get(key) is not None

    def __getitem__(self, key: Hashable) -> Any:  # pragma: no cover - trivial
        val = self.get(key)
        if val is None:
            raise KeyError(key)
        return val

    def __setitem__(self, key: Hashable, value: Any) -> None:
        self.set(key, value)

    def set(self, key: Hashable, value: Any) -> None:
        with self._thread_lock:
            self._purge()
            exp = time.monotonic() + self.ttl
            self._data[key] = (value, exp)
            heapq.heappush(self._expiry_heap, (exp, key))
            self._evict()

    def pop(self, key: Hashable, default: Any = None) -> Any:
        with self._thread_lock:
            item = self._data.pop(key, None)
        if item is None:
            return default
        return item[0]

    def clear(self) -> None:  # pragma: no cover - trivial
        with self._thread_lock:
            self._data.clear()
            self._expiry_heap.clear()

    def keys(self):  # pragma: no cover - trivial
        with self._thread_lock:
            self._purge()
            return list(self._data.keys())

    def __len__(self) -> int:  # pragma: no cover - trivial
        with self._thread_lock:
            self._purge()
            return len(self._data)

    # async helpers -------------------------------------------------------
    async def get_or_set_async(
        self, key: Hashable, coro: "asyncio.Future | asyncio.coroutine | Any"
    ) -> Any:
        """Return cached value or set result of awaited ``coro``.

        ``coro`` may be a coroutine object or a callable returning a coroutine.
        Only a single task will execute ``coro`` when a key is missing; other
        callers will await the same task.
        """

        val = self.get(key)
        if val is not None:
            return val

        async with self._lock:
            val = self.get(key)
            if val is not None:
                return val
            loop = asyncio.get_running_loop()
            pend_key = (key, loop)
            task = self._pending.get(pend_key)
            if task is None:
                if callable(coro):
                    task = asyncio.create_task(coro())
                else:
                    task = asyncio.create_task(coro)
                self._pending[pend_key] = task

        try:
            val = await task
            self.set(key, val)
            return val
        finally:
            # Remove only the entry for this loop
            self._pending.pop((key, asyncio.get_running_loop()), None)
