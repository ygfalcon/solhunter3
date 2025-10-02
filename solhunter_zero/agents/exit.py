from __future__ import annotations
import logging
from typing import Any, Dict, List

from . import BaseAgent
from ..portfolio import Portfolio
from ..prices import resolve_token_price


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
        resolved = await resolve_token_price(token)
        price = float(resolved) if resolved is not None else 0.0
        pos = portfolio.balances[token]

        if price:
            roi = portfolio.position_roi(token, price)
            if self.stop_loss and roi <= -self.stop_loss:
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
            if self.take_profit and roi >= self.take_profit:
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]

        if self.trailing:
            if price <= 0:
                logger.info(
                    "ExitAgent skipping trailing stop due to non-positive price",
                    extra={"token": token},
                )
            elif portfolio.trailing_stop_triggered(token, price, self.trailing):
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
        return []
