from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, List, Tuple, Dict, Optional

FEE_RATE = 0.001  # 0.1% per trade

import numpy as np


# Strategies now optionally accept historical liquidity/depth data to
# simulate slippage and execution costs.
StrategyFunc = Callable[[List[float], Optional[List[float]]], List[float]]


def _net_return(p0: float, p1: float, liquidity: Optional[float] = None) -> float:
    """Return the realised return after fees and slippage."""
    if p0 <= 0:
        return 0.0
    raw = (p1 - p0) / p0
    slip = 0.0
    if liquidity:
        slip = min(0.05, 1.0 / max(liquidity, 1e-9))
    return raw - FEE_RATE - slip


@dataclass
class StrategyResult:
    """Result of a backtest for a single strategy."""

    name: str
    roi: float
    sharpe: float
    max_drawdown: float
    volatility: float
    cumulative_returns: List[float]


def buy_and_hold(prices: List[float], liquidity: Optional[List[float]] = None) -> List[float]:
    """Return daily returns for a buy and hold approach including costs."""

    rets: List[float] = []
    for i in range(1, len(prices)):
        liq = liquidity[i] if liquidity and i < len(liquidity) else None
        rets.append(_net_return(prices[i - 1], prices[i], liq))
    return rets


def momentum(prices: List[float], liquidity: Optional[List[float]] = None) -> List[float]:
    """Return returns when buying only on positive momentum."""

    returns: List[float] = []
    for i in range(1, len(prices)):
        r = (prices[i] - prices[i - 1]) / prices[i - 1]
        if r > 0:
            liq = liquidity[i] if liquidity and i < len(liquidity) else None
            returns.append(_net_return(prices[i - 1], prices[i], liq))
    return returns


DEFAULT_STRATEGIES: List[Tuple[str, StrategyFunc]] = [
    ("buy_hold", buy_and_hold),
    ("momentum", momentum),
]


def backtest_strategies(
    prices: List[float],
    liquidity: Optional[List[float]] = None,
    strategies: Iterable[Tuple[str, StrategyFunc]] | None = None,
) -> List[StrategyResult]:
    """Backtest ``strategies`` on ``prices`` and return ranked results."""

    if strategies is None:
        strategies = DEFAULT_STRATEGIES

    results: List[StrategyResult] = []
    for name, strat in strategies:
        rets = strat(prices, liquidity)
        if not rets:
            roi = 0.0
            sharpe = 0.0
            max_dd = 0.0
            vol = 0.0
            cum_returns: List[float] = []
        else:
            arr = np.array(rets, dtype=float)
            cum = np.cumprod(1 + arr)
            cum_returns = (cum - 1).tolist()
            roi = float(cum_returns[-1])
            vol = float(arr.std())
            mean = float(arr.mean())
            sharpe = mean / vol if vol > 0 else 0.0
            running_max = np.maximum.accumulate(cum)
            drawdowns = (running_max - cum) / running_max
            max_dd = float(drawdowns.max())
        results.append(
            StrategyResult(
                name=name,
                roi=roi,
                sharpe=sharpe,
                max_drawdown=max_dd,
                volatility=vol,
                cumulative_returns=cum_returns,
            )
        )

    results.sort(key=lambda r: (r.roi, r.sharpe), reverse=True)
    return results


def backtest_weighted(
    prices: List[float],
    weights: Dict[str, float],
    liquidity: Optional[List[float]] = None,
    strategies: Iterable[Tuple[str, StrategyFunc]] | None = None,
) -> StrategyResult:
    """Backtest combined strategies using ``weights``."""

    if strategies is None:
        strategies = DEFAULT_STRATEGIES

    weight_sum = sum(float(weights.get(n, 1.0)) for n, _ in strategies)
    if weight_sum <= 0:
        return StrategyResult(
            name="weighted",
            roi=0.0,
            sharpe=0.0,
            max_drawdown=0.0,
            volatility=0.0,
            cumulative_returns=[],
        )

    arrs = []
    for name, strat in strategies:
        rets = strat(prices, liquidity)
        if rets:
            arrs.append((np.array(rets, dtype=float), float(weights.get(name, 1.0))))

    if not arrs:
        return StrategyResult(
            name="weighted",
            roi=0.0,
            sharpe=0.0,
            max_drawdown=0.0,
            volatility=0.0,
            cumulative_returns=[],
        )

    length = min(len(a) for a, _ in arrs)
    agg = np.zeros(length, dtype=float)
    for a, w in arrs:
        agg += w * a[:length]

    agg /= weight_sum
    cum = np.cumprod(1 + agg)
    cum_returns = (cum - 1).tolist()
    roi = float(cum_returns[-1])
    vol = float(agg.std())
    mean = float(agg.mean())
    sharpe = mean / vol if vol > 0 else 0.0
    running_max = np.maximum.accumulate(cum)
    drawdowns = (running_max - cum) / running_max
    max_dd = float(drawdowns.max())
    return StrategyResult(
        name="weighted",
        roi=roi,
        sharpe=sharpe,
        max_drawdown=max_dd,
        volatility=vol,
        cumulative_returns=cum_returns,
    )


def backtest_configs(
    prices: List[float],
    configs: Iterable[Tuple[str, Dict[str, float]]],
    strategies: Iterable[Tuple[str, StrategyFunc]] | None = None,
    liquidity: Optional[List[float]] = None,
) -> List[StrategyResult]:
    """Backtest multiple configs and return sorted results."""

    results = []
    for name, weights in configs:
        res = backtest_weighted(prices, weights, liquidity=liquidity, strategies=strategies)
        results.append(
            StrategyResult(
                name=name,
                roi=res.roi,
                sharpe=res.sharpe,
                max_drawdown=res.max_drawdown,
                volatility=res.volatility,
                cumulative_returns=res.cumulative_returns,
            )
        )

    results.sort(key=lambda r: (r.roi, r.sharpe), reverse=True)
    return results
