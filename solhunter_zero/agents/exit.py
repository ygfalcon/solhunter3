from __future__ import annotations

import logging

from typing import List, Dict, Any

from . import BaseAgent
from ..portfolio import Portfolio
from .price_utils import resolve_price
from ..decision import should_sell


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
        price, price_context = await resolve_price(token, portfolio)
        if price <= 0:
            logger.warning(
                "exit agent could not obtain a valid price for %s; aborting proposal",
                token,
                extra={"price_context": price_context},
            )
            return []
        pos = portfolio.balances[token]

        roi = portfolio.position_roi(token, price)

        trailing = self.trailing if self.trailing else None
        if trailing and price > 0:
            # Update the high watermark before evaluating the trailing stop via
            # ``should_sell`` so the comparison uses the freshest price data.
            portfolio.trailing_stop_triggered(token, price, trailing)

        if should_sell(
            [],
            trailing_stop=trailing,
            current_price=price if price > 0 else None,
            high_price=pos.high_price,
            realized_roi=roi,
            take_profit=self.take_profit or None,
            stop_loss=self.stop_loss or None,
        ):
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]
        return []
