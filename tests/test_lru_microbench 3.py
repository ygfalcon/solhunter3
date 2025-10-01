import time
from solhunter_zero.lru import LRUCache


def _heavy(n: int) -> int:
    total = 0
    for i in range(n):
        total += (i % 5) * (i % 7)
    return total


def test_lru_cache_hits_faster():
    cache = LRUCache(maxsize=2)
    n = 100000

    start = time.perf_counter()
    for _ in range(3):
        val = cache.get(n)
        if val is None:
            val = _heavy(n)
            cache[n] = val
    miss_time = time.perf_counter() - start

    start = time.perf_counter()
    for _ in range(3):
        val = cache.get(n)
        if val is None:
            val = _heavy(n)
            cache[n] = val
    hit_time = time.perf_counter() - start

    assert hit_time <= miss_time
