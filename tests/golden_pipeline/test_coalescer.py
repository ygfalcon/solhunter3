"""Tests for the Golden Snapshot coalescer."""

import asyncio

import pytest

from solhunter_zero.golden_pipeline.coalescer import SnapshotCoalescer
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore
from solhunter_zero.golden_pipeline.types import (
    DepthSnapshot,
    GoldenSnapshot,
    OHLCVBar,
    TokenSnapshot,
)


def _make_token_snapshot(mint: str, *, asof: float) -> TokenSnapshot:
    return TokenSnapshot(
        mint=mint,
        symbol=f"{mint}-SYM",
        name=f"Token {mint}",
        decimals=6,
        token_program="token-program",
        asof=asof,
    )


def _make_bar(mint: str, *, asof: float) -> OHLCVBar:
    return OHLCVBar(
        mint=mint,
        open=1.0,
        high=1.1,
        low=0.9,
        close=1.05,
        vol_usd=123.0,
        vol_base=12.3,
        trades=10,
        buyers=5,
        flow_usd=12.0,
        zret=0.1,
        zvol=0.2,
        asof_close=asof,
    )


def _make_depth(mint: str, *, asof: float) -> DepthSnapshot:
    return DepthSnapshot(
        mint=mint,
        venue="venue-x",
        mid_usd=1.02,
        spread_bps=15.0,
        depth_pct={"2": 1000.0},
        asof=asof,
    )


def test_concurrent_mints_do_not_block_each_other() -> None:
    async def run() -> None:
        events = {"mint-a": asyncio.Event(), "mint-b": asyncio.Event()}
        events["mint-b"].set()
        emitted: list[str] = []

        async def emit(snapshot: GoldenSnapshot) -> None:
            emitted.append(snapshot.mint)
            await events[snapshot.mint].wait()

        coalescer = SnapshotCoalescer(emit)

        # Prime mint A so that the depth update will block on emit.
        await coalescer.update_metadata(_make_token_snapshot("mint-a", asof=1.0))
        await coalescer.update_bar(_make_bar("mint-a", asof=1.0))

        emit_a_task = asyncio.create_task(
            coalescer.update_depth(_make_depth("mint-a", asof=1.0))
        )
        await asyncio.sleep(0)
        assert not emit_a_task.done()

        # While mint A is blocked, updates for mint B should still flow through.
        task_meta_b = asyncio.create_task(
            coalescer.update_metadata(_make_token_snapshot("mint-b", asof=2.0))
        )
        task_bar_b = asyncio.create_task(
            coalescer.update_bar(_make_bar("mint-b", asof=2.0))
        )
        task_depth_b = asyncio.create_task(
            coalescer.update_depth(_make_depth("mint-b", asof=2.0))
        )

        await asyncio.sleep(0)

        assert task_meta_b.done()
        assert task_bar_b.done()
        assert task_depth_b.done()

        await asyncio.gather(task_meta_b, task_bar_b, task_depth_b)

        # Mint B should have emitted even though mint A is still waiting.
        assert "mint-b" in emitted
        assert not emit_a_task.done()

        events["mint-a"].set()
        await emit_a_task

        assert emitted.count("mint-a") == 1
        assert emitted.count("mint-b") == 1

    asyncio.run(run())


def test_hash_cache_expires_inactive_mints(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"value": 0.0}

    def fake_monotonic() -> float:
        return clock["value"]

    monkeypatch.setattr("solhunter_zero.lru.time.monotonic", fake_monotonic)

    async def run() -> None:
        emitted: list[GoldenSnapshot] = []

        async def emit(snapshot: GoldenSnapshot) -> None:
            emitted.append(snapshot)

        coalescer = SnapshotCoalescer(emit, hash_ttl=1.0, hash_cache_size=32)

        for idx in range(64):
            mint = f"mint-{idx}"
            asof = float(idx)
            await coalescer.update_metadata(_make_token_snapshot(mint, asof=asof))
            await coalescer.update_bar(_make_bar(mint, asof=asof))
            await coalescer.update_depth(_make_depth(mint, asof=asof))
            clock["value"] += 0.01

        assert len(coalescer._hash_cache) <= coalescer._hash_cache.maxsize

        clock["value"] += 2.0
        assert len(coalescer._hash_cache) == 0

    asyncio.run(run())


def test_prewarmed_hash_cache_skips_initial_emit() -> None:
    class RecordingKeyValueStore(InMemoryKeyValueStore):
        def __init__(self) -> None:
            super().__init__()
            self.get_calls = 0

        async def get(self, key: str) -> str | None:
            self.get_calls += 1
            return await super().get(key)

    async def run() -> None:
        kv = RecordingKeyValueStore()
        mint = "mint-prewarm"
        emitted: list[GoldenSnapshot] = []

        async def emit(snapshot: GoldenSnapshot) -> None:
            emitted.append(snapshot)

        coalescer = SnapshotCoalescer(emit, kv=kv)
        meta = _make_token_snapshot(mint, asof=1.0)
        bar = _make_bar(mint, asof=1.0)
        depth = _make_depth(mint, asof=1.0)
        await coalescer.update_metadata(meta)
        await coalescer.update_bar(bar)
        await coalescer.update_depth(depth)

        assert len(emitted) == 1

        kv.get_calls = 0
        emitted.clear()

        warmed: list[GoldenSnapshot] = []

        async def warmed_emit(snapshot: GoldenSnapshot) -> None:
            warmed.append(snapshot)

        coalescer_restarted = SnapshotCoalescer(warmed_emit, kv=kv)
        await asyncio.sleep(0)

        await coalescer_restarted.update_metadata(_make_token_snapshot(mint, asof=1.0))
        await coalescer_restarted.update_bar(_make_bar(mint, asof=1.0))
        await coalescer_restarted.update_depth(_make_depth(mint, asof=1.0))

        assert warmed == []
        assert kv.get_calls == 0

    asyncio.run(run())
