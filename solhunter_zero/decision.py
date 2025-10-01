from __future__ import annotations
from typing import List
import statistics

from .simulation import SimulationResult


def should_buy(
    sim_results: List[SimulationResult],
    *,
    min_success: float = 0.6,
    min_roi: float = 1.0,
    min_sharpe: float = 1.0,
    min_volume: float = 0.0,
    min_liquidity: float = 0.0,
    max_slippage: float = 1.0,
    min_volume_spike: float = 1.0,
    min_sentiment: float = 0.0,
    min_order_strength: float = 0.0,
    gas_cost: float = 0.0,
) -> bool:
    """Decide whether to buy a token based on simulation results.

    The decision now incorporates the Sharpe ratio to account for volatility.
    Thresholds for average success probability, ROI and Sharpe ratio are
    configurable.
    """

    if not sim_results:
        return False

    first = sim_results[0]

    if first.volume < min_volume:
        return False
    if first.liquidity < min_liquidity:
        return False
    if first.slippage > max_slippage:
        return False
    if getattr(first, "volume_spike", 1.0) < min_volume_spike:
        return False
    if getattr(first, "sentiment", 0.0) < min_sentiment:
        return False
    if getattr(first, "order_book_strength", 0.0) < min_order_strength:
        return False

    successes = [r.success_prob for r in sim_results]
    rois = [r.expected_roi for r in sim_results]

    avg_success = sum(successes) / len(successes)
    avg_roi = sum(rois) / len(rois) - gas_cost
    roi_std = statistics.stdev(rois) if len(rois) > 1 else 0.0
    sharpe = avg_roi / roi_std if roi_std > 0 else 0.0

    return (
        avg_success >= min_success and avg_roi >= min_roi and sharpe >= min_sharpe
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
) -> bool:
    """Decide whether to sell a token based on simulation results.

    The function looks at the average expected ROI and the average success
    probability.  If either indicates poor future performance we recommend
    selling.  By default a negative expected return or a success probability
    below ``max_success`` triggers a sell.
    """

    if not sim_results:
        return False
    if sim_results[0].liquidity < min_liquidity:
        return True
    if sim_results[0].slippage > max_slippage:
        return True

    successes = [r.success_prob for r in sim_results]
    rois = [r.expected_roi for r in sim_results]

    avg_success = sum(successes) / len(successes)
    avg_roi = sum(rois) / len(rois) - gas_cost

    trailing_hit = False
    if (
        trailing_stop is not None
        and current_price is not None
        and high_price is not None
        and high_price > 0
    ):
        trailing_hit = current_price <= high_price * (1 - trailing_stop)

    return trailing_hit or avg_success <= max_success or avg_roi <= max_roi
