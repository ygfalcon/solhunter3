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
        *,
        temperature: float = 1.0,
        exposure_floor: float = 0.05,
    ):
        self.memory_agent = memory_agent
        self.base_weights = {k: float(v) for k, v in (base_weights or {}).items()}
        self.regime_weights = {
            rk: {k: float(v) for k, v in rv.items()}
            for rk, rv in (regime_weights or {}).items()
        }
        self.temperature = max(1e-6, float(temperature))
        self.exposure_floor = max(0.0, float(exposure_floor))

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
        if not names:
            return {}
        profiles = await self._agent_performance(names)
        rois: Dict[str, float] = {}
        exposures: Dict[str, float] = {}
        for name in names:
            profile = profiles.get(name, AgentPerformance())
            roi_val = profile.roi
            try:
                roi_f = float(roi_val)
            except (TypeError, ValueError):
                roi_f = 0.0
            if not math.isfinite(roi_f):
                roi_f = 0.0
            rois[name] = roi_f
            exposure_val = profile.exposure
            try:
                exp_f = float(exposure_val)
            except (TypeError, ValueError):
                exp_f = 0.0
            if not math.isfinite(exp_f) or exp_f < 0:
                exp_f = 0.0
            exposures[name] = exp_f
        base = {name: float(self.base_weights.get(name, 1.0)) for name in names}
        if regime and regime in self.regime_weights:
            for n, v in self.regime_weights[regime].items():
                if n in base:
                    try:
                        factor = float(v)
                    except (TypeError, ValueError):
                        factor = 1.0
                    if math.isfinite(factor):
                        base[n] *= factor

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

        min_roi = min(rois.values()) if rois else 0.0
        max_roi = max(rois.values()) if rois else 0.0

        exp_vals = []
        for name in names:
            exp_vals.append(max(0.0, exposures.get(name, 0.0)))
        total_exposure = sum(exp_vals)
        if total_exposure <= 0:
            exposure_scale = {name: 1.0 for name in names}
        else:
            exposure_scale: Dict[str, float] = {}
            for idx, name in enumerate(names):
                raw = exp_vals[idx] / total_exposure if total_exposure > 0 else 0.0
                exposure_scale[name] = max(self.exposure_floor, raw)
            scale_sum = sum(exposure_scale.values())
            if scale_sum > 0:
                for key in exposure_scale:
                    exposure_scale[key] /= scale_sum
            else:
                exposure_scale = {name: 1.0 / len(names) for name in names}

        weights = {}
        for name in names:
            roi = rois.get(name, 0.0)
            roi_range = max_roi - min_roi
            if roi_range <= 0:
                norm = 1.0 if roi >= 0 else 0.0
            else:
                norm = (roi - min_roi) / roi_range
            norm = max(0.0, min(1.0, norm))
            if not math.isfinite(norm):
                norm = 0.0
            norm = norm ** (1.0 / self.temperature)
            exposure_factor = exposure_scale.get(name, 1.0)
            profile = profiles.get(name, AgentPerformance())
            tactical = profile.tactical_factor()
            base_weight = base.get(name, 1.0)
            try:
                base_weight = float(base_weight)
            except (TypeError, ValueError):
                base_weight = 1.0
            w = base_weight * norm * exposure_factor * tactical
            if not math.isfinite(w) or w < 0:
                w = 0.0
            weights[name] = w

        weights = await _apply_learning(weights)
        for key, value in list(weights.items()):
            if not isinstance(value, (int, float)):
                weights[key] = 0.0
                continue
            value_f = float(value)
            if not math.isfinite(value_f) or value_f < 0:
                weights[key] = 0.0
            else:
                weights[key] = value_f
        weight_sum = sum(weights.get(name, 0.0) for name in names)
        if weight_sum <= 0:
            count = len(names)
            return {name: 1.0 / count for name in names} if count else {}
        return {name: weights.get(name, 0.0) / weight_sum for name in names}
