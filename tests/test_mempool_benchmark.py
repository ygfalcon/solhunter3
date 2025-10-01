import asyncio
import time

import solhunter_zero.mempool_scanner as mp_scanner
from solhunter_zero import resource_monitor as rm


async def _fake_stream(_url, **__):
    for t in ('a', 'b', 'c', 'd'):
        yield t


async def _fake_rank(_t, _u):
    await asyncio.sleep(0.05)
    return 0.0, {'whale_activity': 0.0, 'momentum': 0.0}


def test_stream_ranked_mempool_tokens_speed(monkeypatch):
    monkeypatch.setattr(mp_scanner, 'stream_mempool_tokens', _fake_stream)
    monkeypatch.setattr(mp_scanner, 'rank_token', _fake_rank)
    monkeypatch.setattr(mp_scanner.os, 'cpu_count', lambda: 2)
    monkeypatch.setattr(rm, 'get_cpu_usage', lambda: 0.0)

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens('rpc')
        async for _ in gen:
            pass

    start = time.perf_counter()
    asyncio.run(run())
    duration = time.perf_counter() - start

    assert duration < 0.12
