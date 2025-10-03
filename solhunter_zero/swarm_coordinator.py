from __future__ import annotations

from typing import Iterable, Dict, Any, Tuple
import inspect
import asyncio
import logging

from .agents import BaseAgent
from .agents.memory import MemoryAgent
from .agents.rl_weight_agent import RLWeightAgent
from .agents.hierarchical_rl_agent import HierarchicalRLAgent

logger = logging.getLogger(__name__)


class SwarmCoordinator:
    """Compute dynamic agent weights based on historical ROI."""

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        base_weights: Dict[str, float] | None = None,
        regime_weights: Dict[str, Dict[str, float]] | None = None,
    ):
        self.memory_agent = memory_agent
        self.base_weights = base_weights or {}
        self.regime_weights = regime_weights or {}

    async def _roi_by_agent(
        self, agent_names: Iterable[str]
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        rois = {name: 0.0 for name in agent_names}
        exposures = {name: 0.0 for name in agent_names}
        if not self.memory_agent or not getattr(self.memory_agent, "memory", None):
            return rois, exposures

        memory = self.memory_agent.memory
        loader = getattr(memory, "list_trades", None)
        if loader is None:
            return rois, exposures

        try:
            trades_obj = loader(limit=1000)
            if inspect.isawaitable(trades_obj):
                trades = await trades_obj
            else:
                trades = trades_obj or []
        except Exception as exc:
            logger.debug("Trade history fetch failed: %s", exc)
            return rois, exposures

        def _lookup(obj: Any, key: str) -> Any:
            if isinstance(obj, dict):
                return obj.get(key)
            return getattr(obj, key, None)

        def _as_float(value: Any) -> float | None:
            if value is None:
                return None
            try:
                return float(value)
            except (TypeError, ValueError):
                return None

        realized: Dict[str, Dict[str, float]] = {}
        fallback: Dict[str, Dict[str, float]] = {}

        for trade in trades:
            reason = _lookup(trade, "reason")
            if reason not in rois:
                continue

            realized_roi = _as_float(_lookup(trade, "realized_roi"))
            realized_notional = _as_float(_lookup(trade, "realized_notional"))
            realized_amount = _as_float(_lookup(trade, "realized_amount"))
            realized_price = _as_float(_lookup(trade, "realized_price"))

            notional = realized_notional
            if notional is None and realized_amount is not None and realized_price is not None:
                notional = realized_amount * realized_price
            if notional is None:
                notional = _as_float(_lookup(trade, "notional"))

            if notional is None:
                amount = _as_float(_lookup(trade, "amount"))
                price = _as_float(_lookup(trade, "price"))
                if amount is not None and price is not None:
                    notional = amount * price

            if realized_roi is not None and notional:
                stats = realized.setdefault(str(reason), {"weighted": 0.0, "notional": 0.0})
                stats["weighted"] += realized_roi * notional
                stats["notional"] += notional
                continue

            direction = _lookup(trade, "direction")
            direction_key = str(direction).lower() if direction else ""
            if direction_key not in {"buy", "sell"}:
                continue

            amount = _as_float(_lookup(trade, "amount"))
            price = _as_float(_lookup(trade, "price"))
            if amount is None or price is None:
                continue
            subtotal = amount * price
            info = fallback.setdefault(str(reason), {"buy": 0.0, "sell": 0.0})
            info[direction_key] += subtotal

        for name in rois:
            stats = realized.get(name)
            if stats and stats["notional"] > 0:
                exposures[name] = stats["notional"]
                rois[name] = stats["weighted"] / stats["notional"]
                continue
            info = fallback.get(name)
            if not info:
                continue
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent > 0:
                exposures[name] = spent
                rois[name] = (revenue - spent) / spent
        return rois, exposures

    async def compute_weights(
        self, agents: Iterable[BaseAgent], *, regime: str | None = None
    ) -> Dict[str, float]:
        rl_agent = next((a for a in agents if isinstance(a, RLWeightAgent)), None)
        hier_agent = next((a for a in agents if isinstance(a, HierarchicalRLAgent)), None)
        agents = [a for a in agents if not isinstance(a, (RLWeightAgent, HierarchicalRLAgent))]
        names = [a.name for a in agents]
        rois, exposures = await self._roi_by_agent(names)
        base = dict(self.base_weights)
        if regime and regime in self.regime_weights:
            base.update(self.regime_weights[regime])

        async def _apply_learning(weights: Dict[str, float]) -> Dict[str, float]:
            if rl_agent:
                try:
                    rl_weights = await rl_agent.train(names)
                    for n, w in rl_weights.items():
                        if n in weights:
                            weights[n] *= float(w)
                except Exception as exc:
                    logger.debug("RLWeightAgent error: %s", exc)
            if hier_agent:
                try:
                    h_weights = hier_agent.train(names)
                    for n, w in h_weights.items():
                        if n in weights:
                            weights[n] *= float(w)
                except Exception as exc:
                    logger.debug("HierarchicalRLAgent error: %s", exc)
            return weights

        if not rois:
            weights = {name: base.get(name, 1.0) for name in names}
            return await _apply_learning(weights)

        min_roi = min(rois.values())
        max_roi = max(rois.values())
        if max_roi == min_roi:
            weights = {name: base.get(name, 1.0) for name in names}
            return await _apply_learning(weights)

        max_exposure = max(exposures.values()) if exposures else 0.0
        if max_exposure <= 0:
            exposure_scale = {name: 1.0 for name in names}
        else:
            exposure_scale = {
                name: (exposures.get(name, 0.0) / max_exposure) if exposures.get(name) else 0.0
                for name in names
            }

        weights = {}
        for name in names:
            roi = rois.get(name, 0.0)
            norm = (roi - min_roi) / (max_roi - min_roi)
            exposure_factor = exposure_scale.get(name, 1.0)
            weights[name] = base.get(name, 1.0) * norm * exposure_factor

        return await _apply_learning(weights)
