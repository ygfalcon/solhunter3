from __future__ import annotations

from typing import List, Dict, Any, Sequence

import numpy as np

from . import BaseAgent
from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async
from ..risk import RiskManager, compute_live_covariance


class PortfolioOptimizer(BaseAgent):
    """Optimise portfolio weights using mean-variance allocation."""

    name = "portfolio_optimizer"

    def __init__(
        self,
        risk_manager: RiskManager | None = None,
        *,
        count: int = 10,
        threshold: float = 0.05,
    ) -> None:
        self.risk_manager = risk_manager or RiskManager()
        self.count = int(count)
        self.threshold = float(threshold)

    # ------------------------------------------------------------------
    def _expected_return(self, token: str) -> float:
        from ..simulation import run_simulations

        sims = run_simulations(token, count=self.count)
        return sims[0].expected_roi if sims else 0.0

    def _mean_variance_weights(
        self, cov: np.ndarray, returns: Sequence[float]
    ) -> np.ndarray:
        r = np.asarray(list(returns), dtype=float)
        n = r.size
        if cov.size != n * n:
            cov = np.eye(n)
        try:
            inv = np.linalg.pinv(cov)
            w = inv @ r
        except Exception:
            w = np.ones(n)
        w = np.maximum(w, 0)
        s = w.sum()
        if s > 0:
            w /= s
        else:
            w = np.full(n, 1.0 / n)
        return w

    def _rebalance(
        self,
        tokens: Sequence[str],
        weights: Dict[str, float],
        prices: Dict[str, float],
        portfolio: Portfolio,
    ) -> List[Dict[str, Any]]:
        actions: List[Dict[str, Any]] = []
        current = portfolio.weights(prices)
        total = portfolio.total_value(prices)
        for tok in tokens:
            target = weights.get(tok, 0.0)
            curr = current.get(tok, 0.0)
            diff = target - curr
            if abs(diff) <= self.threshold:
                continue
            price = prices.get(tok, 0.0)
            if price <= 0:
                continue
            amount = abs(diff) * total / price
            if diff > 0:
                actions.append({"token": tok, "side": "buy", "amount": amount, "price": price})
            else:
                pos = portfolio.balances.get(tok)
                if pos:
                    amount = min(amount, pos.amount)
                    actions.append({"token": tok, "side": "sell", "amount": amount, "price": price})
        return actions

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        tokens = list(set(portfolio.balances.keys()) | {token})
        prices = await fetch_token_prices_async(tokens)
        portfolio.record_prices(prices)
        returns = {tok: self._expected_return(tok) for tok in tokens}
        cov = compute_live_covariance({t: portfolio.price_history.get(t, []) for t in tokens})
        base_weights = self._mean_variance_weights(cov, [returns[t] for t in tokens])
        weights = {tok: w for tok, w in zip(tokens, base_weights)}

        portfolio.update_risk_metrics()
        rm = self.risk_manager.adjusted(portfolio_metrics=portfolio.risk_metrics)
        max_alloc = rm.max_allocation
        weights = {tok: min(w, max_alloc) for tok, w in weights.items()}
        total_w = sum(weights.values())
        if total_w > 1.0:
            weights = {k: v / total_w for k, v in weights.items()}

        return self._rebalance(tokens, weights, prices, portfolio)
