from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Dict, Any, Tuple
import inspect
import asyncio
import logging
import math

from .agents import BaseAgent
from .agents.memory import MemoryAgent
from .agents.rl_weight_agent import RLWeightAgent

try:  # Optional hierarchical RL agent (torch dependency)
    from .agents.hierarchical_rl_agent import HierarchicalRLAgent
except Exception:  # pragma: no cover - import guard
    HierarchicalRLAgent = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


@dataclass
class AgentPerformance:
    """Aggregated realized performance metrics for a single agent."""

    weighted_roi: float = 0.0
    weighted_roi_squared: float = 0.0
    notional: float = 0.0
    wins: int = 0
    losses: int = 0
    fallback_buy: float = 0.0
    fallback_sell: float = 0.0

    def register_realized(self, roi: float, notional: float) -> None:
        if notional <= 0:
            return
        self.weighted_roi += roi * notional
        self.weighted_roi_squared += (roi * roi) * notional
        self.notional += notional
        if roi > 0:
            self.wins += 1
        elif roi < 0:
            self.losses += 1

    def register_directional(self, direction: str, subtotal: float) -> None:
        if direction == "buy":
            self.fallback_buy += max(subtotal, 0.0)
        elif direction == "sell":
            self.fallback_sell += max(subtotal, 0.0)

    @property
    def roi(self) -> float:
        if self.notional > 0:
            return self.weighted_roi / self.notional
        if self.fallback_buy > 0:
            return (self.fallback_sell - self.fallback_buy) / self.fallback_buy
        return 0.0

    @property
    def exposure(self) -> float:
        if self.notional > 0:
            return self.notional
        return self.fallback_buy

    @property
    def trade_count(self) -> int:
        return self.wins + self.losses

    def win_rate(self) -> float:
        total = self.trade_count
        if total <= 0:
            return 0.5
        return self.wins / total

    def volatility(self) -> float:
        if self.notional <= 0:
            return 0.0
        mean = self.weighted_roi / self.notional
        mean_sq = self.weighted_roi_squared / self.notional
        variance = max(0.0, mean_sq - mean * mean)
        return math.sqrt(variance)

    def tactical_factor(self) -> float:
        """Return a multiplicative boost favouring consistent performers."""

        win_component = 0.25 + 0.75 * self.win_rate()
        consistency = 1.0 / (1.0 + self.volatility())
        consistency_component = 0.25 + 0.75 * consistency
        trade_count = self.trade_count
        if trade_count <= 0:
            trade_component = 0.5
        else:
            trade_component = 0.5 + 0.5 * min(
                1.0, math.log1p(trade_count) / math.log(1.0 + 10.0)
            )
        score = win_component * consistency_component * trade_component
        baseline = 0.625 * 1.0 * 0.5  # Neutral win-rate, perfect consistency, sparse data
        if baseline <= 0:
            return max(0.25, min(4.0, score))
        return max(0.25, min(4.0, score / baseline))


class SwarmCoordinator:
    """Compute dynamic agent weights from realized performance data.

    The coordinator favours agents with higher realized ROI while scaling the
    contribution of each agent by the realized notional they have traded.  When
    realized execution metrics are unavailable it falls back to estimating ROI
    from spend and revenue totals.
    """

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        base_weights: Dict[str, float] | None = None,
        regime_weights: Dict[str, Dict[str, float]] | None = None,
    ):
        self.memory_agent = memory_agent
        self.base_weights = base_weights or {}
        self.regime_weights = regime_weights or {}

    async def _agent_performance(
        self, agent_names: Iterable[str]
    ) -> Dict[str, AgentPerformance]:
        profiles = {name: AgentPerformance() for name in agent_names}
        if not self.memory_agent or not getattr(self.memory_agent, "memory", None):
            return profiles

        memory = self.memory_agent.memory
        loader = getattr(memory, "list_trades", None)
        if loader is None:
            return profiles

        try:
            trades_obj = loader(limit=1000)
            if inspect.isawaitable(trades_obj):
                trades = await trades_obj
            else:
                trades = trades_obj or []
        except Exception as exc:
            logger.debug("Trade history fetch failed: %s", exc)
            return profiles

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

        def _extract_metric(trade: Any, *keys: str) -> float | None:
            """Return the first numeric metric found for ``keys``."""

            containers: list[Any] = [trade]
            # Common containers used when logging metrics from the execution
            # pipeline.
            for attr in (
                "metrics",
                "result",
                "data",
                "details",
                "extra",
                "metadata",
                "payload",
            ):
                value = _lookup(trade, attr)
                if isinstance(value, dict):
                    containers.append(value)
                    # Nested containers may also appear under the same keys
                    for sub_key in ("metrics", "data", "details"):
                        sub_val = value.get(sub_key) if isinstance(value, dict) else None
                        if isinstance(sub_val, dict):
                            containers.append(sub_val)

            for container in containers:
                for key in keys:
                    result = _lookup(container, key)
                    numeric = _as_float(result)
                    if numeric is not None:
                        return numeric
            return None

        for trade in trades:
            reason = _lookup(trade, "reason")
            if reason not in profiles:
                continue

            key = str(reason)
            stats = profiles[key]

            realized_roi = _extract_metric(
                trade,
                "realized_roi",
                "roi",
                "return",
                "realized_return",
            )
            realized_notional = _extract_metric(
                trade,
                "realized_notional",
                "notional",
                "filled_notional",
                "gross_notional",
                "trade_value",
                "tradeValue",
                "value",
            )
            realized_amount = _extract_metric(
                trade,
                "realized_amount",
                "filled_amount",
                "filledAmount",
                "amount",
                "executed_amount",
                "executedAmount",
                "qty",
                "quantity",
                "size",
            )
            realized_price = _extract_metric(
                trade,
                "realized_price",
                "execution_price",
                "executionPrice",
                "avg_price",
                "average_price",
                "fill_price",
                "price",
            )

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
                stats.register_realized(float(realized_roi), float(notional))
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
            stats.register_directional(direction_key, subtotal)

        return profiles

    async def _roi_by_agent(
        self, agent_names: Iterable[str]
    ) -> Tuple[Dict[str, float], Dict[str, float]]:
        profiles = await self._agent_performance(agent_names)
        rois = {name: profiles.get(name, AgentPerformance()).roi for name in agent_names}
        exposures = {
            name: profiles.get(name, AgentPerformance()).exposure for name in agent_names
        }
        return rois, exposures

    async def compute_weights(
        self, agents: Iterable[BaseAgent], *, regime: str | None = None
    ) -> Dict[str, float]:
        rl_agent = next((a for a in agents if isinstance(a, RLWeightAgent)), None)
        if HierarchicalRLAgent is not None:
            hier_agent = next((a for a in agents if isinstance(a, HierarchicalRLAgent)), None)
            agents = [
                a
                for a in agents
                if not isinstance(a, (RLWeightAgent, HierarchicalRLAgent))
            ]
        else:
            hier_agent = None
            agents = [a for a in agents if not isinstance(a, RLWeightAgent)]
        names = [a.name for a in agents]
        profiles = await self._agent_performance(names)
        rois = {name: profiles.get(name, AgentPerformance()).roi for name in names}
        exposures = {name: profiles.get(name, AgentPerformance()).exposure for name in names}
        base = dict(self.base_weights)
        if regime and regime in self.regime_weights:
            base.update(self.regime_weights[regime])

        async def _apply_learning(weights: Dict[str, float]) -> Dict[str, float]:
            rl_weights: Dict[str, float] | None = None
            hier_weights: Dict[str, float] | None = None

            async def _run_rl() -> None:
                nonlocal rl_weights
                if not rl_agent:
                    return
                try:
                    rl_weights = await rl_agent.train(names)
                except Exception as exc:
                    logger.debug("RLWeightAgent error: %s", exc)
                    rl_weights = None

            async def _run_hier() -> None:
                nonlocal hier_weights
                if not hier_agent:
                    return
                try:
                    hier_weights = await asyncio.to_thread(hier_agent.train, names)
                except Exception as exc:
                    logger.debug("HierarchicalRLAgent error: %s", exc)
                    hier_weights = None

            tasks: list[asyncio.Task[None]] = []
            if rl_agent:
                tasks.append(asyncio.create_task(_run_rl()))
            if hier_agent:
                tasks.append(asyncio.create_task(_run_hier()))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            if rl_weights:
                for n, w in rl_weights.items():
                    if n in weights:
                        try:
                            weights[n] *= float(w)
                        except Exception:
                            continue
            if hier_weights:
                for n, w in hier_weights.items():
                    if n in weights:
                        try:
                            weights[n] *= float(w)
                        except Exception:
                            continue
            return weights

        min_roi = min(rois.values()) if rois else 0.0
        max_roi = max(rois.values()) if rois else 0.0

        total_exposure = sum(value for value in exposures.values() if value > 0)
        if total_exposure <= 0:
            exposure_scale = {name: 1.0 for name in names}
        else:
            exposure_scale = {
                name: (exposures.get(name, 0.0) / total_exposure)
                if exposures.get(name)
                else 0.1
                for name in names
            }

        weights = {}
        for name in names:
            roi = rois.get(name, 0.0)
            roi_range = max_roi - min_roi
            if roi_range <= 0:
                norm = 1.0 if roi >= 0 else 0.0
            else:
                norm = (roi - min_roi) / roi_range
            norm = max(0.0, min(1.0, norm))
            exposure_factor = exposure_scale.get(name, 1.0)
            profile = profiles.get(name, AgentPerformance())
            tactical = profile.tactical_factor()
            weights[name] = base.get(name, 1.0) * norm * exposure_factor * tactical

        return await _apply_learning(weights)
