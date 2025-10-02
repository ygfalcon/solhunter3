from __future__ import annotations

from typing import List, Dict, Any

from . import BaseAgent

from ..portfolio import Portfolio, calculate_order_size
from ..prices import fetch_token_prices_async


class PortfolioAgent(BaseAgent):
    """Maintain portfolio exposure limits."""

    name = "portfolio"

    def __init__(self, max_allocation: float = 0.2, buy_risk: float = 0.05) -> None:
        self.max_allocation = max_allocation
        self.buy_risk = buy_risk
        self._skipped_adjustments: List[Dict[str, Any]] = []

    @property
    def skipped_adjustments(self) -> List[Dict[str, Any]]:
        """Return actions that were skipped due to missing price data."""

        return list(self._skipped_adjustments)

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        allocation = portfolio.percent_allocated(token)
        pos = portfolio.balances.get(token)
        actions: List[Dict[str, Any]] = []
        skipped: List[Dict[str, Any]] = []

        if allocation > self.max_allocation and pos:
            excess = allocation - self.max_allocation
            amount = pos.amount * excess / allocation
            if amount > 0:
                actions.append({"token": token, "side": "sell", "amount": amount, "price": 0.0})
        elif allocation < self.max_allocation and not portfolio.balances:
            size = calculate_order_size(
                1.0,
                self.buy_risk,
                risk_tolerance=self.buy_risk,
                max_allocation=self.max_allocation,
                current_allocation=allocation,
            )
            if size > 0:
                actions.append({"token": token, "side": "buy", "amount": size, "price": 0.0})

        if not actions:
            self._skipped_adjustments = []
            return actions

        symbols = {str(act.get("token")) for act in actions if act.get("token")}
        prices: Dict[str, float] = {}
        if symbols:
            try:
                fetched = await fetch_token_prices_async(symbols)
                if isinstance(fetched, dict):
                    prices = {str(k): float(v) for k, v in fetched.items() if isinstance(v, (int, float))}
            except Exception:
                prices = {}

        hydrated: List[Dict[str, Any]] = []
        for act in actions:
            symbol = str(act.get("token")) if act.get("token") is not None else ""
            price = float(prices.get(symbol, 0.0)) if symbol else 0.0
            if price <= 0:
                skipped.append(
                    {
                        "token": act.get("token"),
                        "side": act.get("side"),
                        "reason": "missing_price",
                    }
                )
                continue
            act["price"] = price
            hydrated.append(act)

        self._skipped_adjustments = skipped
        return hydrated

