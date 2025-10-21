"""Unit tests for the ExecutionContext pruning behaviour."""

import pytest

from solhunter_zero.golden_pipeline import execution as execution_module
from solhunter_zero.golden_pipeline.execution import ExecutionContext
from solhunter_zero.golden_pipeline.types import GoldenSnapshot


def _make_snapshot(idx: int) -> GoldenSnapshot:
    return GoldenSnapshot(
        mint=f"mint-{idx}",
        asof=float(idx),
        meta={},
        px={},
        liq={},
        ohlcv5m={},
        hash=f"hash-{idx}",
        metrics={},
    )


def test_execution_context_prunes_to_max_entries() -> None:
    ctx = ExecutionContext(max_entries=3, ttl_seconds=None)
    snapshots = [_make_snapshot(i) for i in range(5)]
    for snapshot in snapshots:
        ctx.record(snapshot)

    # The newest snapshots should be retained while the oldest are pruned.
    assert ctx.get(snapshots[4].hash) is snapshots[4]
    assert ctx.get(snapshots[3].hash) is snapshots[3]
    assert ctx.get(snapshots[2].hash) is snapshots[2]
    assert ctx.get(snapshots[1].hash) is None
    assert ctx.get(snapshots[0].hash) is None


def test_execution_context_retains_recent_snapshots_for_decisions() -> None:
    ctx = ExecutionContext(max_entries=2, ttl_seconds=None)
    older = _make_snapshot(10)
    recent = _make_snapshot(11)
    latest = _make_snapshot(12)

    ctx.record(older)
    ctx.record(recent)
    assert ctx.get(recent.hash) is recent

    # Recording the latest snapshot should prune only the oldest entry.
    ctx.record(latest)
    assert ctx.get(latest.hash) is latest
    assert ctx.get(recent.hash) is recent
    assert ctx.get(older.hash) is None


def test_execution_context_prunes_expired_snapshots(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = ExecutionContext(max_entries=10, ttl_seconds=5.0)
    times = iter([0.0, 1.0, 10.0])

    monkeypatch.setattr(execution_module, "now_ts", lambda: next(times))

    snapshot = _make_snapshot(42)
    ctx.record(snapshot)

    # Within the TTL the snapshot can be retrieved.
    assert ctx.get(snapshot.hash) is snapshot

    # Advancing time beyond the TTL should prune the stored snapshot.
    assert ctx.get(snapshot.hash) is None

