from __future__ import annotations

from typing import List, Dict, Any

from . import BaseAgent
from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async


class HedgingAgent(BaseAgent):
    """Rebalance holdings based on correlation metrics."""

    name = "hedging"

    def __init__(self, stable_token: str = "USDC", threshold: float = 0.05) -> None:
        self.stable_token = stable_token
        self.threshold = threshold

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        symbols = set(portfolio.balances.keys()) | {self.stable_token}
        prices = await fetch_token_prices_async(symbols)
        portfolio.record_prices(prices)

        weights = portfolio.weights(prices)
        hedged = portfolio.hedged_weights(prices)
        actions: List[Dict[str, Any]] = []
        total = portfolio.total_value(prices)

        for tok, w in weights.items():
            target = hedged.get(tok, 0.0)
            diff = w - target
            if abs(diff) <= self.threshold:
                continue
            price = prices.get(tok, 0.0)
            amount = abs(diff) * total / price if price > 0 else 0.0
            side = "sell" if diff > 0 else "buy"
            actions.append({"token": tok, "side": side, "amount": amount, "price": price})

        return actions
