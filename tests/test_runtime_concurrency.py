"""Concurrency safety tests for runtime UI providers."""

from __future__ import annotations

import threading
from typing import Callable, Dict, List

import pytest

from solhunter_zero.runtime.trading_runtime import TradingRuntime


def _exercise_concurrent_access(
    mutate: Callable[[int], None], snapshot: Callable[[], List[Dict[str, object]]]
) -> None:
    iterations = 500
    writers = [
        threading.Thread(target=lambda: [mutate(i) for i in range(iterations)])
        for _ in range(4)
    ]
    readers = [
        threading.Thread(target=lambda: [snapshot() for _ in range(iterations)])
        for _ in range(4)
    ]

    for thread in writers + readers:
        thread.start()
    for thread in writers + readers:
        thread.join()


@pytest.mark.parametrize("kind", ["trade", "log"])
def test_snapshot_thread_safety(kind: str) -> None:
    runtime = TradingRuntime()

    if kind == "trade":
        mutate = lambda idx: runtime._append_trade({"id": idx})
        snapshot = runtime._snapshot_trades
    else:
        mutate = lambda idx: runtime._append_ui_log({"seq": idx})
        snapshot = runtime._snapshot_ui_logs

    _exercise_concurrent_access(mutate, snapshot)

    # Sanity check that snapshots remain bounded and return shallow copies.
    items = snapshot()
    assert all(isinstance(entry, dict) for entry in items)
    assert len(items) <= 200
