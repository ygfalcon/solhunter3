from __future__ import annotations

from typing import List, Dict, Any

from . import BaseAgent

from ..portfolio import Portfolio, calculate_order_size


class PortfolioAgent(BaseAgent):
    """Maintain portfolio exposure limits."""

    name = "portfolio"

    def __init__(self, max_allocation: float = 0.2, buy_risk: float = 0.05) -> None:
        self.max_allocation = max_allocation
        self.buy_risk = buy_risk

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

        return actions

