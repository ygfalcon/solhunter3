from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Iterable, Mapping

import sys
import types

if "solhunter_zero.agent_manager" not in sys.modules:
    agent_manager_stub = types.ModuleType("solhunter_zero.agent_manager")

    class AgentManager:  # pragma: no cover - minimal stub for tests
        pass

    agent_manager_stub.AgentManager = AgentManager
    sys.modules["solhunter_zero.agent_manager"] = agent_manager_stub

if "solhunter_zero.portfolio" not in sys.modules:
    portfolio_stub = types.ModuleType("solhunter_zero.portfolio")

    class Portfolio:  # pragma: no cover - minimal stub for tests
        def __init__(self, path: str | None = None) -> None:
            self.path = path

        def record_prices(
            self, prices: Mapping[str, float], *, window: int | None = None
        ) -> None:
            self._last_prices = dict(prices)

    portfolio_stub.Portfolio = Portfolio
    sys.modules["solhunter_zero.portfolio"] = portfolio_stub

if "solhunter_zero.token_scanner" not in sys.modules:
    token_scanner_stub = types.ModuleType("solhunter_zero.token_scanner")
    token_scanner_stub.TRENDING_METADATA = {}
    sys.modules["solhunter_zero.token_scanner"] = token_scanner_stub

import pytest

from scripts.capture_daemon import CaptureConfig, capture_events
from scripts.replay_capture import ReplayConfig, load_capture, replay_capture
from solhunter_zero.event_bus import BUS as RUNTIME_BUS, reset as reset_event_bus
from solhunter_zero.golden_pipeline.contracts import STREAMS
from solhunter_zero.golden_pipeline.service import GoldenPipelineService
from solhunter_zero.golden_pipeline.types import TokenSnapshot
from solhunter_zero.portfolio import Portfolio
from solhunter_zero.schemas import to_dict


MINT = "MintCaptureReplay1111111111111111111111111111111"
INPUT_TOPICS = ["token_discovered", "price_update", "depth_update"]
UI_TOPICS = [STREAMS.golden_snapshot, STREAMS.trade_suggested]
EVENT_SEQUENCE: list[tuple[str, Mapping[str, Any]]] = [
    ("token_discovered", {"tokens": [MINT]}),
    ("price_update", {"token": MINT, "price": 1.052, "venue": "sim", "pool": "sim-1"}),
    (
        "depth_update",
        {
            MINT: {
                "bids": 15000.0,
                "asks": 15200.0,
                "depth": 18000.0,
                "venue": "sim",
                "spread_bps": 26.0,
            }
        },
    ),
    ("price_update", {"token": MINT, "price": 1.061, "venue": "sim", "pool": "sim-1"}),
    (
        "depth_update",
        {
            MINT: {
                "bids": 16200.0,
                "asks": 15800.0,
                "depth": 20500.0,
                "venue": "sim",
                "spread_bps": 24.0,
            }
        },
    ),
]


class DeterministicClock:
    def __init__(self, start: float = 1_700_000_000.0, step: float = 0.05) -> None:
        self._wall = start
        self._mono = 10_000.0
        self._step = step

    def time(self) -> float:
        value = self._wall
        self._wall += self._step
        return value

    def monotonic(self) -> float:
        value = self._mono
        self._mono += self._step
        return value


class DummyAgentManager:
    async def evaluate_with_swarm(self, mint: str, portfolio: Portfolio) -> SimpleNamespace:
        action = {
            "side": "buy",
            "price": 1.06,
            "notional_usd": 2_400.0,
            "max_slippage_bps": 45.0,
            "confidence": 0.28,
            "ttl_sec": 1.5,
            "pattern": "first_pullback",
            "expected_edge_bps": 64.0,
            "fees_bps": 2.5,
            "latency_bps": 1.2,
        }
        return SimpleNamespace(actions=[action])


def _patch_clock(monkeypatch: pytest.MonkeyPatch, clock: DeterministicClock) -> None:
    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.service.time.time", clock.time, raising=False
    )
    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.service.time.monotonic",
        clock.monotonic,
        raising=False,
    )
    for target in (
        "solhunter_zero.golden_pipeline.utils.now_ts",
        "solhunter_zero.golden_pipeline.market.now_ts",
        "solhunter_zero.golden_pipeline.coalescer.now_ts",
        "solhunter_zero.golden_pipeline.execution.now_ts",
        "solhunter_zero.golden_pipeline.agents.now_ts",
        "solhunter_zero.golden_pipeline.voting.now_ts",
    ):
        monkeypatch.setattr(target, clock.time, raising=False)
    monkeypatch.setattr("solhunter_zero.event_bus.websockets", None, raising=False)


def _normalise_payload(payload: Any) -> Any:
    data = to_dict(payload)
    if isinstance(data, dict):
        return {k: _normalise_payload(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_normalise_payload(v) for v in data]
    if isinstance(data, float):
        return round(data, 8)
    return data


def _summarise_metrics(metrics: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}
    for key, stats in metrics.items():
        summary[key] = {
            stat: (round(value, 8) if isinstance(value, float) else value)
            for stat, value in stats.items()
        }
    return summary


def _summarise_outputs(outputs: Mapping[str, list[Mapping[str, Any]]]) -> dict[str, Any]:
    golden = outputs.get(STREAMS.golden_snapshot, [])
    suggestions = outputs.get(STREAMS.trade_suggested, [])
    return {
        "golden_hashes": sorted(entry.get("hash") for entry in golden),
        "golden_count": len(golden),
        "agents": sorted(entry.get("agent") for entry in suggestions),
        "suggestion_count": len(suggestions),
    }


async def _fake_enrichment_fetcher(mints: Iterable[str]) -> dict[str, TokenSnapshot]:
    snapshots: dict[str, TokenSnapshot] = {}
    for mint in mints:
        snapshots[str(mint)] = TokenSnapshot(
            mint=str(mint),
            symbol="TEST",
            name="Test Token",
            decimals=6,
            token_program="Tokenkeg",
            venues=("sim",),
            flags={"source": "capture"},
            asof=1_700_000_000.0,
        )
    return snapshots


def _subscribe_ui() -> tuple[dict[str, list[Mapping[str, Any]]], list[Callable[[], None]]]:
    store: dict[str, list[Mapping[str, Any]]] = {topic: [] for topic in UI_TOPICS}
    unsubscribers = []
    for topic in UI_TOPICS:
        def _handler(payload: Any, name: str = topic) -> None:
            store[name].append(_normalise_payload(payload))

        unsubscribers.append(RUNTIME_BUS.subscribe(topic, _handler))
    return store, unsubscribers


async def _drive_and_capture(path: Path) -> tuple[dict[str, list[Mapping[str, Any]]], dict[str, dict[str, Any]]]:
    clock = DeterministicClock()
    monkeypatch = pytest.MonkeyPatch()
    service: GoldenPipelineService | None = None
    try:
        _patch_clock(monkeypatch, clock)
        portfolio = Portfolio(path=None)
        manager = DummyAgentManager()
        service = GoldenPipelineService(
            agent_manager=manager,
            portfolio=portfolio,
            enrichment_fetcher=_fake_enrichment_fetcher,
            event_bus=RUNTIME_BUS,
        )
        await service.start()

        outputs, unsubs = _subscribe_ui()
        config = CaptureConfig(
            output=path,
            topics=INPUT_TOPICS,
            max_events=len(EVENT_SEQUENCE),
            force=True,
            bus=RUNTIME_BUS,
        )
        capture_task = asyncio.create_task(
            capture_events(config, register_signals=False)
        )
        await asyncio.sleep(0)

        for topic, payload in EVENT_SEQUENCE:
            RUNTIME_BUS.publish(topic, payload)
            await asyncio.sleep(0.05)

        await asyncio.wait_for(capture_task, timeout=5.0)
        await asyncio.sleep(0.25)
        await service.pipeline.flush_market()
        await asyncio.sleep(0.25)

        metrics = _summarise_metrics(service.pipeline.metrics_snapshot())
        for unsub in unsubs:
            unsub()
        return outputs, metrics
    finally:
        if service is not None:
            await service.stop()
        monkeypatch.undo()


async def _drive_and_replay(path: Path) -> tuple[dict[str, list[Mapping[str, Any]]], dict[str, dict[str, Any]]]:
    clock = DeterministicClock()
    monkeypatch = pytest.MonkeyPatch()
    service: GoldenPipelineService | None = None
    try:
        _patch_clock(monkeypatch, clock)
        portfolio = Portfolio(path=None)
        manager = DummyAgentManager()
        service = GoldenPipelineService(
            agent_manager=manager,
            portfolio=portfolio,
            enrichment_fetcher=_fake_enrichment_fetcher,
            event_bus=RUNTIME_BUS,
        )
        await service.start()

        outputs, unsubs = _subscribe_ui()
        config = ReplayConfig(path=path, speed=0.0, topics=INPUT_TOPICS, bus=RUNTIME_BUS)
        await asyncio.wait_for(replay_capture(config), timeout=5.0)
        await asyncio.sleep(0.25)
        await service.pipeline.flush_market()
        await asyncio.sleep(0.25)

        metrics = _summarise_metrics(service.pipeline.metrics_snapshot())
        for unsub in unsubs:
            unsub()
        return outputs, metrics
    finally:
        if service is not None:
            await service.stop()
        monkeypatch.undo()


async def _exercise_capture_replay(tmp_path: Path) -> None:
    capture_path = tmp_path / "golden-capture.jsonl"
    reset_event_bus()
    first_outputs, first_metrics = await _drive_and_capture(capture_path)

    metadata, recorded = load_capture(capture_path)
    assert metadata.get("topics") == INPUT_TOPICS
    assert len(recorded) == len(EVENT_SEQUENCE)

    first_summary = _summarise_outputs(first_outputs)
    reset_event_bus()

    second_outputs, second_metrics = await _drive_and_replay(capture_path)
    second_summary = _summarise_outputs(second_outputs)

    assert second_summary == first_summary
    assert second_metrics == first_metrics


def test_event_bus_capture_and_replay(tmp_path: Path) -> None:
    asyncio.run(_exercise_capture_replay(tmp_path))
