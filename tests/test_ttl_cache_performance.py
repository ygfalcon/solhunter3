import time
from collections import OrderedDict
from solhunter_zero.lru import TTLCache


class _OldTTLCache:
    def __init__(self, maxsize: int = 128, ttl: float = 60.0) -> None:
        self.maxsize = maxsize
        self.ttl = float(ttl)
        self._data = OrderedDict()

    def _purge(self) -> None:
        now = time.monotonic()
        keys = list(self._data.keys())
        for k in keys:
            _, exp = self._data[k]
            if exp <= now:
                self._data.pop(k, None)

    def _evict(self) -> None:
        while len(self._data) > self.maxsize:
            self._data.popitem(last=False)

    def set(self, key, value) -> None:
        self._purge()
        self._data[key] = (value, time.monotonic() + self.ttl)
        self._evict()

    def get(self, key, default=None):
        self._purge()
        item = self._data.get(key)
        if item is None:
            return default
        value, exp = item
        if exp <= time.monotonic():
            self._data.pop(key, None)
            return default
        return value


def _measure(cache_cls) -> float:
    cache = cache_cls(maxsize=6000, ttl=0.001)
    for i in range(5000):
        cache.set(i, i)
    time.sleep(0.002)
    start = time.perf_counter()
    for i in range(5000):
        cache.get(i)
    return time.perf_counter() - start


def test_heap_purge_performance():
    old = _measure(_OldTTLCache)
    new = _measure(TTLCache)
    assert new <= old
