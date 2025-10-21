"""Rolling Golden Snapshot telemetry helpers."""

from __future__ import annotations

import math
from collections import deque
from statistics import median
from typing import Deque, Dict, Iterable, Mapping, MutableMapping, Optional

from .types import GoldenSnapshot


class _RollingMetric:
    """Track the most recent observations for a single metric."""

    def __init__(self, *, maxlen: int = 512) -> None:
        self._values: Deque[float] = deque(maxlen=maxlen)

    def add(self, value: float | int | None) -> None:
        if value is None:
            return
        try:
            numeric = float(value)
        except (TypeError, ValueError):  # pragma: no cover - defensive guard
            return
        if math.isnan(numeric) or math.isinf(numeric):  # pragma: no cover - guard
            return
        self._values.append(numeric)

    def percentile(self, pct: float) -> Optional[float]:
        if not self._values:
            return None
        pct = min(max(pct, 0.0), 100.0)
        sorted_values = sorted(self._values)
        if not sorted_values:
            return None
        rank = (pct / 100.0) * (len(sorted_values) - 1)
        lower = int(math.floor(rank))
        upper = int(math.ceil(rank))
        if lower == upper:
            return sorted_values[lower]
        weight = rank - lower
        return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight

    def maximum(self) -> Optional[float]:
        if not self._values:
            return None
        return max(self._values)

    def median(self) -> Optional[float]:
        if not self._values:
            return None
        return median(self._values)

    def count(self) -> int:
        return len(self._values)

    def snapshot(self) -> Dict[str, Optional[float]]:
        return {
            "count": float(self.count()),
            "p50": self.median(),
            "p95": self.percentile(95.0),
            "p99": self.percentile(99.0),
            "max": self.maximum(),
        }


class GoldenMetrics:
    """Capture latency and freshness stats for Golden Snapshots."""

    def __init__(self, *, window: int = 512) -> None:
        self._window = window
        self._metrics: MutableMapping[str, _RollingMetric] = {}

    def _metric(self, name: str) -> _RollingMetric:
        metric = self._metrics.get(name)
        if metric is None:
            metric = _RollingMetric(maxlen=self._window)
            self._metrics[name] = metric
        return metric

    def record(self, snapshot: GoldenSnapshot) -> None:
        metrics = snapshot.metrics or {}
        if not isinstance(metrics, Mapping):
            return
        for key, value in metrics.items():
            if key.startswith("_"):
                continue
            self._metric(key).add(value)

    def update_from_many(self, snapshots: Iterable[GoldenSnapshot]) -> None:
        for snapshot in snapshots:
            self.record(snapshot)

    def snapshot(self) -> Dict[str, Dict[str, Optional[float]]]:
        return {name: metric.snapshot() for name, metric in self._metrics.items()}

    def latest(self) -> Dict[str, Optional[float]]:
        return {name: metric.percentile(100.0) for name, metric in self._metrics.items()}

    def record_discovery(self, counters: Mapping[str, int | float]) -> None:
        for key, value in counters.items():
            metric_name = f"discovery.{key}"
            self._metric(metric_name).add(value)

