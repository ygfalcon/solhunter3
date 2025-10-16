from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping, Tuple

import numpy as np

from .backtester import backtest_weighted
from .risk import RiskManager, portfolio_cvar
from .portfolio import hedge_allocation


@dataclass
class PipelineResult:
    weights: Dict[str, float]
    risk: RiskManager
    sharpe: float
    roi: float


def rolling_backtest(
    history: Mapping[str, List[float]],
    weights: Dict[str, float],
    risk: RiskManager,
    window: int = 30,
    step: int = 7,
) -> PipelineResult:
    """Run a simple rolling backtest updating weights and risk."""

    tokens = list(history.keys())
    min_len = min(len(v) for v in history.values())
    if min_len < window:
        return PipelineResult(weights, risk, 0.0, 0.0)

    start = 0
    rois = []
    sharpes = []
    while start + window < min_len:
        slice_hist = {k: v[start : start + window] for k, v in history.items()}
        prices = slice_hist[tokens[0]]
        res = backtest_weighted(prices, weights)
        rois.append(res.roi)
        sharpes.append(res.sharpe)
        cvar = portfolio_cvar(slice_hist, weights)
        risk = risk.adjusted(portfolio_cvar=cvar, cvar_threshold=cvar * 0.5)
        start += step

    if sharpes:
        avg_sharpe = float(np.mean(sharpes))
        avg_roi = float(np.mean(rois))
    else:
        avg_sharpe = 0.0
        avg_roi = 0.0
    return PipelineResult(weights, risk, avg_sharpe, avg_roi)


# ---------------------------------------------------------------------------
# Bayesian optimisation helper
# ---------------------------------------------------------------------------


def bayesian_optimize_parameters(
    history: List[float],
    bounds: Mapping[str, Tuple[float, float]],
    iterations: int = 20,
) -> Dict[str, float]:
    """Optimise ``bounds`` using a Gaussian process."""

    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C

    xs = []
    ys = []

    def evaluate(params: Dict[str, float]) -> float:
        weights = {"strategy": 1.0}
        rm = RiskManager()
        res = backtest_weighted(history, weights)
        return res.sharpe

    kernel = C(1.0) * RBF(1.0)
    gp = GaussianProcessRegressor(kernel=kernel, alpha=1e-6)

    for _ in range(iterations):
        if xs:
            gp.fit(np.array(xs), np.array(ys))
            cand = {}
            for k, (lo, hi) in bounds.items():
                pred_x = np.linspace(lo, hi, 5).reshape(-1, 1)
                mu, sigma = gp.predict(
                    np.hstack([pred_x] * len(bounds)), return_std=True
                )
                best = pred_x[int(np.argmin(mu - sigma))][0]
                cand[k] = float(best)
        else:
            cand = {k: np.random.uniform(lo, hi) for k, (lo, hi) in bounds.items()}
        score = evaluate(cand)
        xs.append([cand[k] for k in bounds])
        ys.append(score)
    best_idx = int(np.argmax(ys))
    best_params = {k: xs[best_idx][i] for i, k in enumerate(bounds)}
    return best_params
