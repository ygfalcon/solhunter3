"""Unit tests for the Golden Snapshot agent stage."""

from __future__ import annotations

import asyncio
from collections import deque
from typing import Iterable, Sequence

import pytest

from solhunter_zero.golden_pipeline.agents import AgentStage, BaseAgent
from solhunter_zero.golden_pipeline.types import GoldenSnapshot, TradeSuggestion


class _StubAgent(BaseAgent):
    def __init__(
        self,
        name: str,
        *,
        delay: float = 0.0,
        suggestions: Iterable[TradeSuggestion] = (),
    ) -> None:
        super().__init__(name)
        self._delay = delay
        self._suggestions = tuple(suggestions)

    async def generate(self, snapshot: GoldenSnapshot) -> Sequence[TradeSuggestion]:
        if self._delay:
            await asyncio.sleep(self._delay)
        return self._suggestions


def _snapshot() -> GoldenSnapshot:
    return GoldenSnapshot(
        mint="FAST_MINT",
        asof=1_700_000_000.0,
        meta={},
        px={"spread_bps": 5.0},
        liq={"depth_pct": {"1": 50_000.0}},
        ohlcv5m={},
        hash="abc123",
    )


def _suggestion(agent: str) -> TradeSuggestion:
    snap = _snapshot()
    return TradeSuggestion(
        agent=agent,
        mint=snap.mint,
        side="buy",
        notional_usd=1.0,
        max_slippage_bps=10.0,
        risk={},
        confidence=1.0,
        inputs_hash=snap.hash,
        ttl_sec=30.0,
        generated_at=snap.asof,
    )


def test_fast_agent_emits_before_slow_agent() -> None:
    fast = _StubAgent("fast", suggestions=[_suggestion("fast")])
    slow = _StubAgent("slow", delay=0.2, suggestions=[_suggestion("slow")])

    emitted: deque[tuple[str, float]] = deque()
    fast_emitted = asyncio.Event()

    async def _emit(suggestion: TradeSuggestion) -> None:
        loop_time = asyncio.get_running_loop().time()
        emitted.append((suggestion.agent, loop_time))
        if suggestion.agent == "fast":
            fast_emitted.set()

    stage = AgentStage(
        _emit,
        agents=[fast, slow],
        agent_timeout_sec=1.0,
    )

    async def _run() -> None:
        task = asyncio.create_task(stage.submit(_snapshot()))
        start = asyncio.get_running_loop().time()
        await asyncio.wait_for(fast_emitted.wait(), timeout=0.15)
        fast_seen = emitted[0]
        assert fast_seen[0] == "fast"
        assert fast_seen[1] - start < 0.15
        await task

    asyncio.run(_run())

    assert [agent for agent, _ in emitted] == ["fast", "slow"]


def test_slow_agent_timeout_does_not_block_fast_agent() -> None:
    fast = _StubAgent("fast", suggestions=[_suggestion("fast")])

    class _TimeoutAgent(_StubAgent):
        async def generate(self, snapshot: GoldenSnapshot) -> Sequence[TradeSuggestion]:
            await asyncio.sleep(0.2)
            return await super().generate(snapshot)

    slow = _TimeoutAgent("slow", suggestions=[_suggestion("slow")])

    emitted: deque[str] = deque()
    fast_emitted = asyncio.Event()

    async def _emit(suggestion: TradeSuggestion) -> None:
        emitted.append(suggestion.agent)
        if suggestion.agent == "fast":
            fast_emitted.set()

    stage = AgentStage(
        _emit,
        agents=[fast, slow],
        agent_timeout_sec=0.05,
    )

    async def _run() -> None:
        task = asyncio.create_task(stage.submit(_snapshot()))
        await asyncio.wait_for(fast_emitted.wait(), timeout=0.15)
        await task

    asyncio.run(_run())

    assert list(emitted) == ["fast"]


def test_agent_stage_respects_near_fresh_window() -> None:
    snap = _snapshot()
    snap.metrics["depth_staleness_ms"] = 15_000.0
    snap.metrics["depth_near_fresh_window_ms"] = 20_000.0
    snap.metrics["depth_near_fresh"] = True

    agent = _StubAgent("fresh", suggestions=[_suggestion("fresh")])
    emitted: deque[str] = deque()

    async def _emit(suggestion: TradeSuggestion) -> None:
        emitted.append(suggestion.agent)

    stage = AgentStage(_emit, agents=[agent], depth_near_fresh_ms=20_000.0)

    asyncio.run(stage.submit(snap))

    assert list(emitted) == ["fresh"]


def test_agent_stage_skips_and_logs_on_stale_depth(caplog: pytest.LogCaptureFixture) -> None:
    snap = _snapshot()
    snap.metrics["depth_staleness_ms"] = 45_000.0
    snap.metrics["depth_near_fresh_window_ms"] = 20_000.0
    snap.metrics["depth_near_fresh"] = False

    agent = _StubAgent("stale", suggestions=[_suggestion("stale")])
    emitted: deque[str] = deque()

    async def _emit(suggestion: TradeSuggestion) -> None:
        emitted.append(suggestion.agent)

    stage = AgentStage(_emit, agents=[agent], depth_near_fresh_ms=20_000.0)

    with caplog.at_level("INFO"):
        asyncio.run(stage.submit(snap))
        asyncio.run(stage.submit(snap))

    assert not emitted
    records = [record for record in caplog.records if "stale depth" in record.message]
    assert len(records) == 1

    # When depth becomes fresh we clear the latch so future stale episodes log again.
    caplog.clear()
    snap.metrics["depth_staleness_ms"] = 10_000.0
    snap.metrics["depth_near_fresh"] = True
    asyncio.run(stage.submit(snap))
    snap.metrics["depth_staleness_ms"] = 50_000.0
    snap.metrics["depth_near_fresh"] = False
    with caplog.at_level("INFO"):
        asyncio.run(stage.submit(snap))
    records = [record for record in caplog.records if "stale depth" in record.message]
    assert len(records) == 1
