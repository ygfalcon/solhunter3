from __future__ import annotations

from typing import List, Dict, Any

from . import BaseAgent
from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async


class PortfolioManager(BaseAgent):
    """Manage portfolio allocations and track performance."""

    name = "portfolio_manager"

    def __init__(
        self,
        rebalance_threshold: float = 0.1,
        exit_threshold: float = -0.2,
        reentry_threshold: float = 0.0,
    ) -> None:
        self.rebalance_threshold = rebalance_threshold
        self.exit_threshold = exit_threshold
        self.reentry_threshold = reentry_threshold
        self.pnl_history: List[float] = []
        self.exited = False

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        symbols = set(portfolio.balances.keys()) | {token}
        prices = await fetch_token_prices_async(symbols)

        value = portfolio.total_value(prices)
        entry_value = sum(p.amount * p.entry_price for p in portfolio.balances.values())
        roi = (value - entry_value) / entry_value if entry_value else 0.0
        self.pnl_history.append(roi)

        actions: List[Dict[str, Any]] = []

        if not self.exited and roi <= self.exit_threshold and portfolio.balances:
            for tok, pos in portfolio.balances.items():
                price = prices.get(tok, 0.0)
                actions.append({"token": tok, "side": "sell", "amount": pos.amount, "price": price})
            self.exited = True
            return actions

        if self.exited and roi >= self.reentry_threshold:
            price = prices.get(token, 0.0)
            actions.append({"token": token, "side": "buy", "amount": 1.0, "price": price})
            self.exited = False
            return actions

        if token in portfolio.balances and portfolio.balances:
            tokens = list(portfolio.balances.keys())
            total = value
            target = total / len(tokens)
            pos = portfolio.balances[token]
            price = prices.get(token, pos.entry_price)
            current = pos.amount * price
            upper = target * (1 + self.rebalance_threshold)
            lower = target * (1 - self.rebalance_threshold)
            if current > upper:
                diff_val = current - target
                amount = diff_val / price
                actions.append({"token": token, "side": "sell", "amount": amount, "price": price})
            elif current < lower:
                diff_val = target - current
                amount = diff_val / price
                actions.append({"token": token, "side": "buy", "amount": amount, "price": price})

        return actions
