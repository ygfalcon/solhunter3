from __future__ import annotations

from typing import List, Dict, Any

import logging

from . import BaseAgent

from ..portfolio import Portfolio, calculate_order_size
from .price_utils import resolve_price

logger = logging.getLogger(__name__)


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
                actions.append({"token": token, "side": "sell", "amount": amount})
        elif allocation < self.max_allocation and not portfolio.balances:
            size = calculate_order_size(
                1.0,
                self.buy_risk,
                risk_tolerance=self.buy_risk,
                max_allocation=self.max_allocation,
                current_allocation=allocation,
            )
            if size > 0:
                actions.append({"token": token, "side": "buy", "amount": size})

        if not actions:
            self._skipped_adjustments = []
            return actions

        hydrated: List[Dict[str, Any]] = []
        for act in actions:
            token = str(act.get("token")) if act.get("token") is not None else ""
            if not token:
                continue
            price, context = await resolve_price(token, portfolio)
            if price <= 0:
                logger.info(
                    "%s agent skipping %s for %s due to missing price: %s",
                    self.name,
                    act.get("side"),
                    token,
                    context,
                )
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
