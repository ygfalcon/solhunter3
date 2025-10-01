from __future__ import annotations

"""Decision metrics aggregator.

Subscribes to action_decision and runtime.stage_changed to compute:
- decision_rate (decisions/min)
- buy/sell counts in a window
- avg_size per decision
- time_to_first_decision (once)

Publishes 'decision_metrics' periodically.
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Deque, Dict

from .. import event_bus


@dataclass
class _Dec:
    ts: float
    side: str
    size: float


class DecisionMetrics:
    def __init__(self, interval: float = 5.0, window_sec: float = 60.0) -> None:
        self.interval = float(interval)
        self.window_sec = float(window_sec)
        self._buf: Deque[_Dec] = deque(maxlen=5000)
        self._task: asyncio.Task | None = None
        self._start: float | None = None
        self._ttf_published = False

    def start(self) -> None:
        event_bus.subscribe("action_decision", self._on_decision)
        event_bus.subscribe("runtime.stage_changed", self._on_stage)
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._tick())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def _on_stage(self, payload: Dict[str, Any]) -> None:
        st = payload.get("stage") if isinstance(payload, dict) else getattr(payload, "stage", None)
        if st == "runtime:ready" and self._start is None:
            self._start = time.time()

    def _on_decision(self, payload: Dict[str, Any]) -> None:
        side = str(payload.get("side", "hold"))
        size = float(payload.get("size", 0.0))
        self._buf.append(_Dec(ts=time.time(), side=side, size=size))
        if not self._ttf_published and self._start is not None:
            self._ttf_published = True
            ttf = time.time() - self._start
            try:
                event_bus.publish("decision_metrics", {"ttf_decision": ttf})
            except Exception:
                pass

    async def _tick(self) -> None:
        while True:
            try:
                now = time.time()
                cut = now - self.window_sec
                while self._buf and self._buf[0].ts < cut:
                    self._buf.popleft()
                if self._buf:
                    n = len(self._buf)
                    buys = sum(1 for d in self._buf if d.side == "buy")
                    sells = sum(1 for d in self._buf if d.side == "sell")
                    avg_size = sum(d.size for d in self._buf) / max(1, n)
                    rate = n / (self.window_sec / 60.0)
                    event_bus.publish(
                        "decision_metrics",
                        {
                            "window_sec": self.window_sec,
                            "count": n,
                            "decision_rate": rate,
                            "buys": buys,
                            "sells": sells,
                            "avg_size": avg_size,
                        },
                    )
            except Exception:
                pass
            await asyncio.sleep(max(0.5, self.interval))

