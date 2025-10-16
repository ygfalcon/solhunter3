from __future__ import annotations

import inspect
import random
from collections import defaultdict
from typing import List, Dict, Any

import logging

from . import BaseAgent
from .memory import MemoryAgent
from .price_utils import resolve_price
from ..portfolio import Portfolio

logger = logging.getLogger(__name__)


class ReinforcementAgent(BaseAgent):
    """Q-learning agent trained on past trades."""

    name = "reinforcement"

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        learning_rate: float = 0.1,
        epsilon: float = 0.1,
        discount: float = 0.95,
    ) -> None:
        self.memory_agent = memory_agent or MemoryAgent()
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.discount = discount
        self.q: Dict[str, Dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
        self._last_id: int = 0

    async def train(self) -> None:
        """Update Q-values from trade history."""
        loader = getattr(self.memory_agent.memory, "list_trades", None)
        if loader is None:
            return
        trades = loader(since_id=self._last_id)
        if inspect.isawaitable(trades):  # type: ignore[name-defined]
            trades = await trades
        profits: Dict[str, float] = defaultdict(float)
        for t in trades:
            value = float(t.amount) * float(t.price)
            if t.direction == "buy":
                profits[t.token] -= value
            else:
                profits[t.token] += value
            tid = getattr(t, "id", None)
            if tid is not None and tid > self._last_id:
                self._last_id = tid
        for token, reward in profits.items():
            q = self.q[token]
            q["buy"] += self.learning_rate * (reward - q["buy"])
            q["sell"] += self.learning_rate * (-reward - q["sell"])

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        await self.train()
        q = self.q[token]
        if random.random() < self.epsilon:
            action = random.choice(["buy", "sell"])
        else:
            action = "buy" if q["buy"] >= q["sell"] else "sell"

        if action == "buy":
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
                    "amount": 1.0,
                    "price": price,
                }
            ]

        position = portfolio.balances.get(token)
        if position:
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
                    "amount": position.amount,
                    "price": price,
                }
            ]
        return []
