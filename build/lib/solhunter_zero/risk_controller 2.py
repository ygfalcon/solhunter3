from __future__ import annotations

"""Adaptive risk controller.

Listens to decision and risk metrics and publishes a dynamic risk multiplier
(`risk_updated`) to dampen trading during volatile/overactive periods and
allow growth in calm periods. Keeps values within [0.5, 2.0] by default.
"""

import asyncio
import os
from dataclasses import dataclass
from typing import Any, Dict

from . import event_bus


@dataclass
class _State:
    decision_rate: float = 0.0
    avg_size: float = 0.0
    buys: int = 0
    sells: int = 0
    cvar: float = 0.0
    covariance: float = 0.0
    correlation: float = 0.0


class RiskController:
    def __init__(self, interval: float = 5.0) -> None:
        self.interval = float(interval)
        self.state = _State()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        event_bus.subscribe("decision_metrics", self._on_decision_metrics)
        event_bus.subscribe("risk_metrics", self._on_risk_metrics)
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._loop())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def _on_decision_metrics(self, payload: Dict[str, Any]) -> None:
        try:
            self.state.decision_rate = float(payload.get("decision_rate", 0.0))
            self.state.avg_size = float(payload.get("avg_size", 0.0))
            self.state.buys = int(payload.get("buys", 0))
            self.state.sells = int(payload.get("sells", 0))
        except Exception:
            pass

    def _on_risk_metrics(self, payload: Dict[str, Any]) -> None:
        try:
            self.state.covariance = float(payload.get("covariance", 0.0))
            self.state.cvar = float(payload.get("portfolio_cvar", 0.0))
            self.state.correlation = float(payload.get("correlation", 0.0))
        except Exception:
            pass

    async def _loop(self) -> None:
        base = float(os.getenv("BASE_RISK_MULTIPLIER", os.getenv("RISK_MULTIPLIER", "1.0") or 1.0))
        lo = float(os.getenv("RISK_MULTIPLIER_MIN", "0.5") or 0.5)
        hi = float(os.getenv("RISK_MULTIPLIER_MAX", "2.0") or 2.0)
        while True:
            try:
                s = self.state
                # Normalize metrics to 0..1 ranges with gentle influence
                rate_penalty = min(1.0, s.decision_rate / 120.0)  # 120 decisions/min is very high
                cvar_penalty = max(0.0, min(1.0, s.cvar))
                corr_penalty = (max(-1.0, min(1.0, s.correlation)) + 1.0) / 2.0  # 0..1
                # Combine penalties (weighted)
                penalty = 0.5 * rate_penalty + 0.3 * cvar_penalty + 0.2 * corr_penalty
                mult = base * (1.0 - 0.5 * penalty)
                mult = max(lo, min(hi, mult))
                # Publish and set env for downstream users
                os.environ["RISK_MULTIPLIER"] = str(mult)
                event_bus.publish("risk_updated", {"multiplier": mult})
            except Exception:
                pass
            await asyncio.sleep(max(1.0, self.interval))

