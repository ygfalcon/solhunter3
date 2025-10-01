from __future__ import annotations

from typing import List, Dict, Any

from . import BaseAgent
from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async


class ExitAgent(BaseAgent):
    """Propose exit trades using ROI thresholds or trailing stops."""

    name = "exit"

    def __init__(self, trailing: float = 0.0, stop_loss: float = 0.0, take_profit: float = 0.0):
        self.trailing = trailing
        self.stop_loss = stop_loss
        self.take_profit = take_profit

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        if token not in portfolio.balances:
            return []
        prices = await fetch_token_prices_async({token})
        price = prices.get(token, 0.0)
        pos = portfolio.balances[token]

        if price:
            roi = portfolio.position_roi(token, price)
            if self.stop_loss and roi <= -self.stop_loss:
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
            if self.take_profit and roi >= self.take_profit:
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]

        if self.trailing and portfolio.trailing_stop_triggered(token, price, self.trailing):
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
        return []
