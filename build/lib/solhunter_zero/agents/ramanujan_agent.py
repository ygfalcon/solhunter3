from __future__ import annotations

import hashlib
import math
from typing import List, Dict, Any

import logging

from . import BaseAgent
from ..portfolio import Portfolio
from .price_utils import resolve_price

logger = logging.getLogger(__name__)


class RamanujanAgent(BaseAgent):
    """Simple deterministic conviction scoring using token hash."""

    name = "ramanujan"

    def __init__(self, threshold: float = 0.5, amount: float = 1.0) -> None:
        self.threshold = threshold
        self.amount = amount

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        # Deterministic score based on token hash.
        digest = int(hashlib.sha256(token.encode()).hexdigest(), 16)
        score = math.tanh(math.sin(digest % 1000))

        if score > self.threshold:
            price, context = await resolve_price(token, portfolio)
            if price <= 0:
                logger.info(
                    "%s agent skipping buy for %s due to missing price: %s",
                    self.name,
                    token,
                    context,
                )
                return []
            return [
                {
                    "token": token,
                    "side": "buy",
                    "amount": self.amount,
                    "price": price,
                }
            ]
        if score < -self.threshold:
            pos = portfolio.balances.get(token)
            if pos:
                price, context = await resolve_price(token, portfolio)
                if price <= 0:
                    logger.info(
                        "%s agent skipping sell for %s due to missing price: %s",
                        self.name,
                        token,
                        context,
                    )
                    return []
                return [
                    {
                        "token": token,
                        "side": "sell",
                        "amount": pos.amount,
                        "price": price,
                    }
                ]
        return []
