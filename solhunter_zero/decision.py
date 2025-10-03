from __future__ import annotations
from typing import List, Mapping
import statistics

from .simulation import SimulationResult


_DEFAULT_THRESHOLDS = {
    "min_success": 0.6,
    "min_roi": 1.0,
    "min_sharpe": 1.0,
    "min_volume": 0.0,
    "min_liquidity": 0.0,
    "max_slippage": 1.0,
    "min_volume_spike": 1.0,
    "min_sentiment": 0.0,
    "min_order_strength": 0.0,
    "gas_cost": 0.0,
}


def _coerce_threshold_profile(
    profile: Mapping[str, Mapping[str, float]] | None,
) -> dict[str, dict[str, float]]:
    if not profile:
        return {}
    coerced: dict[str, dict[str, float]] = {}
    for regime, values in profile.items():
        inner: dict[str, float] = {}
        for key, value in dict(values).items():
            try:
                inner[str(key)] = float(value)
            except Exception:
                continue
        coerced[str(regime)] = inner
    return coerced


def should_buy(
    sim_results: List[SimulationResult],
    *,
    regime: str | None = None,
    threshold_profile: Mapping[str, Mapping[str, float]] | None = None,
    min_success: float | None = None,
    min_roi: float | None = None,
    min_sharpe: float | None = None,
    min_volume: float | None = None,
    min_liquidity: float | None = None,
    max_slippage: float | None = None,
    min_volume_spike: float | None = None,
    min_sentiment: float | None = None,
    min_order_strength: float | None = None,
    gas_cost: float | None = None,
) -> bool:
    """Decide whether to buy a token based on simulation results.

    The decision now incorporates the Sharpe ratio to account for volatility.
    Thresholds for average success probability, ROI and Sharpe ratio are
    configurable either via direct keyword arguments or the optional
    ``threshold_profile`` mapping keyed by market regime.
    """

    if not sim_results:
        return False

    thresholds = dict(_DEFAULT_THRESHOLDS)
    profile = _coerce_threshold_profile(threshold_profile)
    for scope in ("default", regime or ""):
        if not scope:
            continue
        scoped = profile.get(scope)
        if not scoped:
            continue
        thresholds.update({k: scoped[k] for k in thresholds.keys() & scoped.keys()})

    overrides = {
        "min_success": min_success,
        "min_roi": min_roi,
        "min_sharpe": min_sharpe,
        "min_volume": min_volume,
        "min_liquidity": min_liquidity,
        "max_slippage": max_slippage,
        "min_volume_spike": min_volume_spike,
        "min_sentiment": min_sentiment,
        "min_order_strength": min_order_strength,
        "gas_cost": gas_cost,
    }
    for key, value in overrides.items():
        if value is not None:
            thresholds[key] = float(value)

    first = sim_results[0]

    if first.volume < thresholds["min_volume"]:
        return False
    if first.liquidity < thresholds["min_liquidity"]:
        return False
    if first.slippage > thresholds["max_slippage"]:
        return False
    if getattr(first, "volume_spike", 1.0) < thresholds["min_volume_spike"]:
        return False
    if getattr(first, "sentiment", 0.0) < thresholds["min_sentiment"]:
        return False
    if getattr(first, "order_book_strength", 0.0) < thresholds["min_order_strength"]:
        return False

    successes = [r.success_prob for r in sim_results]
    rois = [r.expected_roi for r in sim_results]

    avg_success = sum(successes) / len(successes)
    avg_roi = sum(rois) / len(rois) - thresholds["gas_cost"]
    roi_std = statistics.stdev(rois) if len(rois) > 1 else 0.0
    sharpe = avg_roi / roi_std if roi_std > 0 else 0.0

    return (
        avg_success >= thresholds["min_success"]
        and avg_roi >= thresholds["min_roi"]
        and sharpe >= thresholds["min_sharpe"]
    )


def should_sell(
    sim_results: List[SimulationResult],
    *,
    max_success: float = 0.4,
    max_roi: float = 0.0,
    min_liquidity: float = 0.0,
    max_slippage: float = 1.0,
    trailing_stop: float | None = None,
    current_price: float | None = None,
    high_price: float | None = None,
    gas_cost: float = 0.0,
    holding_duration: float | None = None,
    max_holding_duration: float | None = None,
    current_drawdown: float | None = None,
    max_drawdown: float | None = None,
    realized_roi: float | None = None,
    take_profit: float | None = None,
    stop_loss: float | None = None,
) -> bool:
    """Decide whether to sell a token based on simulation results.

    The function looks at the average expected ROI and the average success
    probability.  If either indicates poor future performance we recommend
    selling.  By default a negative expected return or a success probability
    below ``max_success`` triggers a sell.
    """

    trailing_hit = False
    if (
        trailing_stop is not None
        and current_price is not None
        and high_price is not None
        and high_price > 0
    ):
        trailing_hit = current_price <= high_price * (1 - trailing_stop)

    duration_hit = (
        holding_duration is not None
        and max_holding_duration is not None
        and max_holding_duration > 0
        and holding_duration >= max_holding_duration
    )

    drawdown_hit = (
        current_drawdown is not None
        and max_drawdown is not None
        and max_drawdown > 0
        and current_drawdown >= max_drawdown
    )

    realized_hit = False
    if realized_roi is not None:
        if take_profit is not None and take_profit > 0 and realized_roi >= take_profit:
            realized_hit = True
        if stop_loss is not None and stop_loss > 0 and realized_roi <= -abs(stop_loss):
            realized_hit = True

    metrics_triggered = trailing_hit or duration_hit or drawdown_hit or realized_hit

    if not sim_results:
        return metrics_triggered

    first = sim_results[0]
    if first.liquidity < min_liquidity:
        return True
    if first.slippage > max_slippage:
        return True

    successes = [r.success_prob for r in sim_results]
    rois = [r.expected_roi for r in sim_results]

    avg_success = sum(successes) / len(successes)
    avg_roi = sum(rois) / len(rois) - gas_cost

    return metrics_triggered or avg_success <= max_success or avg_roi <= max_roi
