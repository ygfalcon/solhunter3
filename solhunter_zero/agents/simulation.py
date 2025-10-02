from __future__ import annotations

from typing import List, Dict, Any, Mapping
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

    def __init__(
        self,
        count: int = 100,
        *,
        threshold_profile: Mapping[str, Mapping[str, float]] | None = None,
    ):
        self.count = count
        self._cache: Dict[str, tuple[float, List[Dict[str, Any]]]] = {}
        self._locks: Dict[str, asyncio.Lock] = {}
        self.threshold_profile = self._coerce_profile(threshold_profile)

    @staticmethod
    def _coerce_profile(
        profile: Mapping[str, Mapping[str, float]] | None,
    ) -> Dict[str, Dict[str, float]]:
        if not profile:
            return {}
        coerced: Dict[str, Dict[str, float]] = {}
        for regime, values in profile.items():
            inner: Dict[str, float] = {}
            for key, value in dict(values).items():
                try:
                    inner[str(key)] = float(value)
                except Exception:
                    continue
            coerced[str(regime)] = inner
        return coerced

    def apply_threshold_profile(
        self, profile: Mapping[str, Mapping[str, float]] | None
    ) -> None:
        """Update the threshold profile used when evaluating simulations."""

        self.threshold_profile = self._coerce_profile(profile)

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
        regime: str | None = None,
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
                elif should_buy(
                    sims,
                    regime=regime,
                    threshold_profile=self.threshold_profile,
                ):
                    actions.append({"token": token, "side": "buy", "amount": 1.0, "price": price})

            self._cache[token] = (time.time(), actions)
            return list(actions)
