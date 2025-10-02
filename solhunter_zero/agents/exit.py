from __future__ import annotations

import logging

from typing import List, Dict, Any

from . import BaseAgent
from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async


logger = logging.getLogger(__name__)


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
        try:
            quotes = await fetch_token_prices_async({token})
        except Exception as exc:  # pragma: no cover - network/runtime failures
            logger.warning("exit agent failed to fetch price for %s: %s", token, exc)
            return []

        price = 0.0
        if isinstance(quotes, dict):
            raw = quotes.get(token)
            if isinstance(raw, (int, float)):
                price = float(raw)

        if price <= 0:
            logger.warning(
                "exit agent could not obtain a valid price for %s; aborting proposal",
                token,
            )
            return []
        pos = portfolio.balances[token]

        roi = portfolio.position_roi(token, price)
        if self.stop_loss and roi <= -self.stop_loss:
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
        if self.take_profit and roi >= self.take_profit:
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]

        if self.trailing and portfolio.trailing_stop_triggered(token, price, self.trailing):
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
        return []
