from __future__ import annotations

from typing import Iterable, Dict, Any
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

    async def _roi_by_agent(self, agent_names: Iterable[str]) -> Dict[str, float]:
        rois = {name: 0.0 for name in agent_names}
        if not self.memory_agent or not getattr(self.memory_agent, "memory", None):
            return rois

        memory = self.memory_agent.memory
        loader = getattr(memory, "list_trades", None)
        if loader is None:
            return rois

        try:
            trades_obj = loader(limit=1000)
            if inspect.isawaitable(trades_obj):
                trades = await trades_obj
            else:
                trades = trades_obj or []
        except Exception as exc:
            logger.debug("Trade history fetch failed: %s", exc)
            return rois

        summary: Dict[str, Dict[str, float]] = {}
        for t in trades:
            reason = getattr(t, "reason", None)
            if reason is None and isinstance(t, dict):
                reason = t.get("reason")
            if reason not in rois:
                continue
            direction = getattr(t, "direction", None)
            if direction is None and isinstance(t, dict):
                direction = t.get("direction")
            if not direction:
                continue
            direction_key = str(direction).lower()
            if direction_key not in {"buy", "sell"}:
                continue
            amount = getattr(t, "amount", None)
            if amount is None and isinstance(t, dict):
                amount = t.get("amount", 0.0)
            price = getattr(t, "price", None)
            if price is None and isinstance(t, dict):
                price = t.get("price", 0.0)
            try:
                subtotal = float(amount or 0.0) * float(price or 0.0)
            except Exception:
                continue
            info = summary.setdefault(str(reason), {"buy": 0.0, "sell": 0.0})
            info[direction_key] += subtotal
        for name in rois:
            info = summary.get(name)
            if not info:
                continue
            spent = info.get("buy", 0.0)
            revenue = info.get("sell", 0.0)
            if spent > 0:
                rois[name] = (revenue - spent) / spent
        return rois

    async def compute_weights(
        self, agents: Iterable[BaseAgent], *, regime: str | None = None
    ) -> Dict[str, float]:
        rl_agent = next((a for a in agents if isinstance(a, RLWeightAgent)), None)
        hier_agent = next((a for a in agents if isinstance(a, HierarchicalRLAgent)), None)
        agents = [a for a in agents if not isinstance(a, (RLWeightAgent, HierarchicalRLAgent))]
        names = [a.name for a in agents]
        rois = await self._roi_by_agent(names)
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

        weights = {}
        for name in names:
            roi = rois.get(name, 0.0)
            norm = (roi - min_roi) / (max_roi - min_roi)
            weights[name] = base.get(name, 1.0) * norm

        return await _apply_learning(weights)
