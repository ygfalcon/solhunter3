import asyncio
from typing import Any, Iterable, Optional

from solhunter_zero.golden_pipeline.bus import InMemoryBus
from solhunter_zero.golden_pipeline.pipeline import GoldenPipeline
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore


async def _noop_enrichment_fetcher(_mints: Iterable[str]) -> dict[str, Any]:
    return {}


class BlockingKeyValueStore(InMemoryKeyValueStore):
    """KV wrapper that can pause operations on specific keys."""

    def __init__(self) -> None:
        super().__init__()
        self._blocks: dict[str, asyncio.Event] = {}

    def block(self, key: str) -> asyncio.Event:
        event = self._blocks.get(key)
        if event is None:
            event = asyncio.Event()
            self._blocks[key] = event
        else:
            event.clear()
        return event

    async def _wait_if_blocked(self, key: str) -> None:
        event = self._blocks.get(key)
        if event is not None and not event.is_set():
            await event.wait()

    async def get(self, key: str) -> Optional[str]:
        await self._wait_if_blocked(key)
        return await super().get(key)

    async def set(self, key: str, value: str, *, ttl: float | None = None) -> None:
        await self._wait_if_blocked(key)
        await super().set(key, value, ttl=ttl)

async def _exercise_concurrent_sequences() -> None:
    kv = BlockingKeyValueStore()
    pipeline = GoldenPipeline(
        enrichment_fetcher=_noop_enrichment_fetcher,
        bus=InMemoryBus(),
        kv=kv,
        allow_inmemory_bus_for_tests=True,
    )

    slow_mint = "MintSlow11111111111111111111111111111111"
    fast_mint = "MintFast11111111111111111111111111111111"

    slow_key = pipeline._sequence_kv_key(slow_mint)
    release_slow = kv.block(slow_key)

    slow_task = asyncio.create_task(pipeline._next_sequence(slow_mint))
    try:
        # Allow the slow task to acquire its mint lock and block on the KV fetch.
        await asyncio.sleep(0)

        fast_result = await asyncio.wait_for(
            pipeline._next_sequence(fast_mint),
            timeout=0.25,
        )
        assert fast_result == 1
    finally:
        release_slow.set()

    slow_result = await asyncio.wait_for(slow_task, timeout=0.25)
    assert slow_result == 1

    # Hot mint cache should allow subsequent increments without touching the KV store.
    fast_follow_up = await pipeline._next_sequence(fast_mint)
    assert fast_follow_up == 2


def test_sequences_for_independent_mints_do_not_block_each_other() -> None:
    asyncio.run(_exercise_concurrent_sequences())
