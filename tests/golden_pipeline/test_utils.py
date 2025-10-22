import asyncio
from collections.abc import Iterable

import pytest

from solhunter_zero.golden_pipeline import utils
from solhunter_zero.golden_pipeline.utils import gather_in_batches


def test_gather_in_batches_generator_auto_batch_size() -> None:
    generator = (i for i in range(5))
    calls: list[list[int]] = []

    async def worker(batch: Iterable[int]) -> list[int]:
        calls.append(list(batch))
        await asyncio.sleep(0)
        return [item * 2 for item in batch]

    result = asyncio.run(gather_in_batches(generator, batch_size=0, worker=worker))

    assert result == [0, 2, 4, 6, 8]
    assert calls == [[0, 1, 2, 3, 4]]


def test_circuit_breaker_on_open_callback(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 1_000.0}

    def fake_now() -> float:
        return clock["now"]

    monkeypatch.setattr(utils, "now_ts", fake_now)
    events: list[float] = []
    breaker = utils.CircuitBreaker(
        threshold=2,
        window_sec=30.0,
        cooldown_sec=15.0,
        on_open=events.append,
    )

    breaker.record_failure()
    assert events == []

    clock["now"] += 1.0
    breaker.record_failure()
    assert len(events) == 1
    assert events[0] == pytest.approx(15.0)

    clock["now"] += 1.0
    breaker.record_failure()
    assert len(events) == 1

    clock["now"] += 20.0
    breaker.record_success()
    breaker.record_failure()
    clock["now"] += 0.5
    breaker.record_failure()
    assert len(events) == 2
    assert events[1] == pytest.approx(15.0)


def test_circuit_breaker_open_for(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = {"now": 250.0}

    def fake_now() -> float:
        return clock["now"]

    monkeypatch.setattr(utils, "now_ts", fake_now)
    events: list[float] = []
    breaker = utils.CircuitBreaker(
        threshold=3,
        window_sec=10.0,
        cooldown_sec=5.0,
        on_open=events.append,
    )

    breaker.open_for(3.0)
    assert events == [pytest.approx(3.0)]

    breaker.open_for(1.0)
    assert len(events) == 1

    clock["now"] += 4.0
    breaker.record_failure()
    breaker.record_failure()
    clock["now"] += 0.1
    breaker.record_failure()

    assert len(events) == 2
    assert events[1] == pytest.approx(5.0)
