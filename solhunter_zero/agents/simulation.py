from __future__ import annotations

from typing import List, Dict, Any
import asyncio
import os
import time

from . import BaseAgent
from ..simulation import run_simulations
from ..decision import should_buy, should_sell
from ..portfolio import Portfolio
from ..prices import fetch_token_prices_async


class SimulationAgent(BaseAgent):
    """Run Monte Carlo simulations and propose trades based on the results."""

    name = "simulation"

    def __init__(self, count: int = 100):
        self.count = count
        self._cache: Dict[str, tuple[float, List[Dict[str, Any]]]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        ttl = float(os.getenv("SIMULATION_CACHE_TTL", "5") or 5.0)
        now = time.time()
        cached = self._cache.get(token)
        if cached and (now - cached[0]) < ttl:
            return list(cached[1])

        lock = self._locks.setdefault(token, asyncio.Lock())
        if lock.locked() and cached:
            return list(cached[1])

        async with lock:
            cached = self._cache.get(token)
            if cached and (time.time() - cached[0]) < ttl:
                return list(cached[1])

            sims = await asyncio.to_thread(run_simulations, token, self.count)
            actions: List[Dict[str, Any]] = []
            if sims:
                prices = await fetch_token_prices_async({token})
                price = prices.get(token, 0.0)
                if should_sell(sims):
                    pos = portfolio.balances.get(token)
                    if pos:
                        actions.append({"token": token, "side": "sell", "amount": pos.amount, "price": price})
                elif should_buy(sims):
                    actions.append({"token": token, "side": "buy", "amount": 1.0, "price": price})

            self._cache[token] = (time.time(), actions)
            return list(actions)
