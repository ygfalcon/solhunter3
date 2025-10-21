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
