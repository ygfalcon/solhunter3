from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import pytest

from solhunter_zero import event_bus
from solhunter_zero.golden_pipeline.agents import BaseAgent
from solhunter_zero.golden_pipeline.bus import EventBusAdapter
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore
from solhunter_zero.golden_pipeline.pipeline import GoldenPipeline
from solhunter_zero.golden_pipeline.types import TokenSnapshot
from solhunter_zero.runtime.runtime_wiring import initialise_runtime_wiring
from solhunter_zero.ui import UIState, start_websockets, stop_websockets

from tools.demo_payloads import (
    ARTIFACT_DIR,
    DemoToken,
    build_depth_snapshots,
    build_token_snapshot,
    build_trade_events,
    candidate_to_dataclass,
    generate_candidates,
)

STREAM_DISCOVERY = "x:mint.discovered"
STREAM_GOLDEN = "x:mint.golden"
STREAM_SUGGESTED = "x:trade.suggested"


@pytest.fixture(scope="session", autouse=True)
def demo_env() -> Iterable[None]:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    for existing in ARTIFACT_DIR.glob("*"):
        try:
            existing.unlink()
        except IsADirectoryError:
            pass
    overrides = {
        "ENVIRONMENT": "production",
        "SOLHUNTER_MODE": "live",
        "GOLDEN_PIPELINE": "1",
        "EVENT_BUS_URL": "ws://127.0.0.1:8779",
        "BROKER_CHANNEL": "solhunter-events-v3",
        "REDIS_URL": "redis://localhost:6379/1",
        "PRICE_PROVIDERS": "pyth,dexscreener,birdeye,synthetic",
        "SEED_TOKENS": ",".join(
            [
                "So11111111111111111111111111111111111111112",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9",
                "Es9vMFrzaCERzSi1jS6t4G8iKrrf5gkP8KkP4dDLf2N9",
            ]
        ),
        "UI_EVENT_WS_PORT": "0",
        "UI_RL_WS_PORT": "0",
        "UI_LOG_WS_PORT": "0",
    }
    previous: Dict[str, str | None] = {key: os.environ.get(key) for key in overrides}
    os.environ.update(overrides)
    try:
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@pytest.fixture(scope="session")
def demo_tokens() -> List[DemoToken]:
    tokens = generate_candidates(20)
    for index, token in enumerate(tokens):
        token.payload.setdefault("_order", index)
    return tokens


class DemoBus(EventBusAdapter):
    def __init__(self) -> None:
        super().__init__(event_bus)


class TopicRecorder:
    def __init__(self, topics: Sequence[str], loop: asyncio.AbstractEventLoop) -> None:
        self._topics = tuple(topics)
        self._loop = loop
        self._subscriptions: List[Any] = []
        self.queues: Dict[str, asyncio.Queue[Dict[str, Any]]] = {
            topic: asyncio.Queue() for topic in topics
        }
        self.events: Dict[str, List[Dict[str, Any]]] = {topic: [] for topic in topics}

    def start(self) -> None:
        for topic in self._topics:
            queue = self.queues[topic]
            events = self.events[topic]

            def _handler(
                payload: Mapping[str, Any],
                _topic: str = topic,
                _queue: asyncio.Queue[Dict[str, Any]] = queue,
                _events: List[Dict[str, Any]] = events,
            ) -> None:
                data = dict(payload)
                _events.append(data)
                if self._loop.is_running():
                    self._loop.call_soon_threadsafe(_queue.put_nowait, data)
                else:
                    _queue.put_nowait(data)

            unsubscribe = event_bus.subscribe(topic, _handler)
            self._subscriptions.append(unsubscribe)

    def stop(self) -> None:
        for unsub in self._subscriptions:
            try:
                unsub()
            except Exception:
                pass
        self._subscriptions.clear()

    async def wait_for(
        self, topic: str, count: int, *, timeout: float = 3.0
    ) -> List[Dict[str, Any]]:
        deadline = time.monotonic() + timeout
        queue = self.queues[topic]
        while len(self.events[topic]) < count and time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(queue.get(), timeout=remaining)
            except asyncio.TimeoutError:
                break
        return list(self.events[topic])


class DemoAgent(BaseAgent):
    def __init__(self, tokens: Sequence[DemoToken]) -> None:
        super().__init__("demo-agent")
        self._tokens = {token.mint: token for token in tokens}

    async def generate(self, snapshot) -> Sequence[Any]:  # type: ignore[override]
        token = self._tokens.get(snapshot.mint)
        if token is None:
            return []
        order = int(token.payload.get("_order", 0))
        if order % 3 == 2:
            return []
        depth_map = snapshot.liq.get("depth_usd_by_pct") or snapshot.liq.get("depth_pct") or {}
        depth_1 = float(depth_map.get("1") or depth_map.get("1.0") or 0.0)
        if depth_1 <= 0:
            return []
        edge = 0.012 + (order % 5) * 0.0015
        breakeven = 40.0 + (order % 4) * 4.0
        buffer = 24.0 + (order % 3) * 4.0
        notional = min(200.0, max(80.0, depth_1 * 0.07))
        suggestion = self.build_suggestion(
            snapshot=snapshot,
            side="BUY",
            notional_usd=notional,
            max_slippage_bps=float(snapshot.px.get("spread_bps", 40.0) + 10.0),
            risk={
                "expected_edge": edge,
                "expected_edge_bps": edge * 10_000.0,
                "breakeven_bps": breakeven,
                "depth_1pct_quote": depth_1,
            },
            confidence=0.67,
            ttl_sec=15.0,
            gating={
                "edge_pass": True,
                "breakeven_bps": breakeven,
                "edge_buffer_bps": buffer,
            },
            slices=[{"venue": "synthetic", "qty": notional / max(snapshot.px.get("mid_usd", 0.0001), 0.0001)}],
            edge=edge,
            breakeven_bp=breakeven,
            edge_buffer_bp=buffer,
            integrity={"golden_hash": snapshot.hash, "schema_version": 3},
        )
        return [suggestion]


@dataclass
class DemoContext:
    pipeline: GoldenPipeline
    bus: DemoBus
    tokens: Sequence[DemoToken]
    recorder: TopicRecorder
    ui_state: UIState
    wiring: Any
    ws_threads: Dict[str, Any]
    loop: asyncio.AbstractEventLoop

    def mark_runtime_ready(self) -> None:
        async def _mark() -> None:
            now = time.time()
            event_bus.publish(
                "runtime.stage_changed",
                {"stage": "bus:ws", "ok": True, "detail": "connected", "ts": now},
            )
            event_bus.publish(
                "runtime.stage_changed",
                {
                    "stage": "agents:event_runtime",
                    "ok": True,
                    "detail": "demo",
                    "ts": now + 0.01,
                    "agent_count": len(self.tokens),
                    "evaluation_concurrency": 8,
                    "executor_concurrency": 3,
                },
            )

        self.loop.run_until_complete(_mark())

    def feed_tokens(self) -> None:
        async def _feed() -> None:
            for token in self.tokens:
                candidate = candidate_to_dataclass(token)
                accepted = await self.pipeline.submit_discovery(candidate)
                if not accepted:
                    raise AssertionError(f"discovery rejected {candidate.mint}")
                await asyncio.sleep(0)
                snapshot = build_token_snapshot(token, asof=candidate.seen_at)
                await self.pipeline.inject_token_snapshot(snapshot)
                trades = build_trade_events(token, base_ts=candidate.seen_at + 0.2)
                for trade in trades:
                    await self.pipeline.submit_market_event(trade)
                depths = build_depth_snapshots(token, base_ts=candidate.seen_at + 0.4)
                for depth in depths:
                    await self.pipeline.submit_depth(depth)
                fresh_depths = build_depth_snapshots(token, base_ts=candidate.seen_at + 1.2)
                for depth in fresh_depths:
                    await self.pipeline.submit_depth(depth)
                await asyncio.sleep(0)
            await self.pipeline.flush_market()

        self.loop.run_until_complete(_feed())

    def close(self) -> None:
        self.recorder.stop()
        try:
            voting = getattr(self.pipeline, "_voting_stage", None)
            if voting is not None:
                timers = getattr(voting, "_timers", {})
                if isinstance(timers, dict):
                    for timer in list(timers.values()):
                        try:
                            timer.cancel()
                        except Exception:
                            pass
                    timers.clear()
                deadlines = getattr(voting, "_timer_deadlines", None)
                if isinstance(deadlines, dict):
                    deadlines.clear()
        except Exception:
            pass
        try:
            stop_websockets()
        except Exception:
            pass
        event_bus.reset()


@pytest.fixture
def demo_context(demo_tokens: Sequence[DemoToken]) -> Iterable[DemoContext]:
    event_bus.reset()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ui_state = UIState()
    wiring = initialise_runtime_wiring(ui_state)
    threads = start_websockets()
    recorder = TopicRecorder((STREAM_DISCOVERY, STREAM_GOLDEN, STREAM_SUGGESTED), loop)
    recorder.start()
    bus = DemoBus()
    kv = InMemoryKeyValueStore()
    agent = DemoAgent(demo_tokens)

    async def fetch_metadata(mints: Iterable[str]) -> Dict[str, TokenSnapshot]:
        snapshots: Dict[str, TokenSnapshot] = {}
        for mint in mints:
            token = next((t for t in demo_tokens if t.mint == mint), None)
            if token:
                snapshots[mint] = build_token_snapshot(token)
        return snapshots

    pipeline = GoldenPipeline(
        enrichment_fetcher=fetch_metadata,
        agents=[agent],
        bus=bus,
        kv=kv,
        max_agent_spread_bps=120.0,
        min_agent_depth1_usd=500.0,
        allow_inmemory_bus_for_tests=True,
    )

    context = DemoContext(
        pipeline=pipeline,
        bus=bus,
        tokens=demo_tokens,
        recorder=recorder,
        ui_state=ui_state,
        wiring=wiring,
        ws_threads=threads,
        loop=loop,
    )
    try:
        yield context
    finally:
        try:
            loop.run_until_complete(asyncio.sleep(0))
        except Exception:
            pass
        context.close()
        wiring.close()
        try:
            loop.close()
        finally:
            asyncio.set_event_loop(None)


__all__ = [
    "ARTIFACT_DIR",
    "DemoContext",
    "demo_context",
    "demo_tokens",
]
