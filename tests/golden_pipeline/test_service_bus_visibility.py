from __future__ import annotations

import asyncio
import logging
import sys
import types
from collections import defaultdict
from typing import Any, Callable

import pytest

if "base58" not in sys.modules:
    base58_mod = types.ModuleType("base58")
    base58_mod.b58decode = lambda *a, **k: b""
    base58_mod.b58encode = lambda *a, **k: b""
    sys.modules["base58"] = base58_mod

from solhunter_zero.golden_pipeline.service import GoldenPipelineService
from solhunter_zero.golden_pipeline.types import DepthSnapshot


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


class FlakyEventBus:
    def __init__(self, failures_before_success: int | None) -> None:
        self.failures_before_success = failures_before_success
        self.attempts = 0
        self._subscribers: dict[str, list[Callable[[Any], Any]]] = defaultdict(list)

    def subscribe(self, topic: str, handler: Callable[[Any], Any]):
        self._subscribers[topic].append(handler)

        def unsubscribe() -> None:
            handlers = self._subscribers.get(topic)
            if handlers and handler in handlers:
                handlers.remove(handler)

        return unsubscribe

    def publish(self, topic: str, payload: Any, *, dedupe_key: str | None = None, _broadcast: bool = True) -> None:
        self.attempts += 1
        handlers = list(self._subscribers.get(topic, ()))
        if self.failures_before_success is None or self.attempts <= self.failures_before_success:
            return
        for handler in handlers:
            result = handler(payload)
            if asyncio.iscoroutine(result):
                asyncio.create_task(result)


class RecordingPipeline:
    def __init__(self) -> None:
        self.depth_snapshots: list[DepthSnapshot] = []

    async def submit_depth(self, snapshot: DepthSnapshot) -> None:
        self.depth_snapshots.append(snapshot)


MINT = "MintAphex1111111111111111111111111111111"


@pytest.mark.anyio("asyncio")
async def test_ensure_bus_visible_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    bus = FlakyEventBus(failures_before_success=2)
    service = GoldenPipelineService.__new__(GoldenPipelineService)
    service._event_bus = bus  # type: ignore[attr-defined]

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("solhunter_zero.golden_pipeline.service.asyncio.sleep", fake_sleep)
    monkeypatch.setenv("BROKER_CHANNEL", "test-channel")
    caplog.set_level(logging.INFO)

    await service._ensure_bus_visible()

    assert bus.attempts == 3
    assert sleeps == [0.5, 1.0]
    assert any("succeeded after" in record.message for record in caplog.records)


@pytest.mark.anyio("asyncio")
async def test_ensure_bus_visible_exhausts_retries(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    bus = FlakyEventBus(failures_before_success=None)
    service = GoldenPipelineService.__new__(GoldenPipelineService)
    service._event_bus = bus  # type: ignore[attr-defined]

    sleeps: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("solhunter_zero.golden_pipeline.service.asyncio.sleep", fake_sleep)
    monkeypatch.setenv("BROKER_CHANNEL", "test-channel")
    caplog.set_level(logging.WARNING)

    with pytest.raises(RuntimeError):
        await service._ensure_bus_visible()

    assert bus.attempts == 5
    assert sleeps == [0.5, 1.0, 2.0, 4.0]
    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 4
    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors


@pytest.mark.anyio("asyncio")
async def test_on_depth_prefers_existing_buckets() -> None:
    service = GoldenPipelineService.__new__(GoldenPipelineService)
    service._running = True  # type: ignore[attr-defined]
    service._pending = set()  # type: ignore[attr-defined]
    service._last_price = {}  # type: ignore[attr-defined]
    pipeline = RecordingPipeline()
    service.pipeline = pipeline  # type: ignore[attr-defined]

    payload = {
        MINT: {
            "venue": "test_venue",
            "depth": 37.5,
            "bids": 12.0,
            "asks": 9.0,
            "depth_pct": {"1%": "1234", 2: 5678, "5.0": 9012},
            "spread_bps": 22.0,
        }
    }

    service._on_depth(payload)  # type: ignore[attr-defined]
    await asyncio.gather(*list(service._pending))  # type: ignore[attr-defined]

    assert len(pipeline.depth_snapshots) == 1
    snapshot = pipeline.depth_snapshots[0]
    assert snapshot.mint == MINT
    assert snapshot.depth_pct == {"1": 1234.0, "2": 5678.0, "5": 9012.0}


@pytest.mark.anyio("asyncio")
async def test_on_depth_generates_synthetic_buckets() -> None:
    service = GoldenPipelineService.__new__(GoldenPipelineService)
    service._running = True  # type: ignore[attr-defined]
    service._pending = set()  # type: ignore[attr-defined]
    service._last_price = {}  # type: ignore[attr-defined]
    pipeline = RecordingPipeline()
    service.pipeline = pipeline  # type: ignore[attr-defined]

    payload = {
        MINT: {
            "venue": "test_venue",
            "depth": 2_000.0,
            "bids": 1_200.0,
            "asks": 800.0,
        }
    }

    service._on_depth(payload)  # type: ignore[attr-defined]
    await asyncio.gather(*list(service._pending))  # type: ignore[attr-defined]

    assert len(pipeline.depth_snapshots) == 1
    snapshot = pipeline.depth_snapshots[0]
    assert snapshot.depth_pct["1"] == pytest.approx(2_000.0)
    assert snapshot.depth_pct["2"] == pytest.approx(3_000.0)
    assert snapshot.depth_pct["5"] == pytest.approx(4_000.0)
