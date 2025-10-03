from __future__ import annotations

from typing import Sequence, Dict, List, Any

import logging

from . import BaseAgent
from .memory import MemoryAgent
from ..portfolio import Portfolio
from ..simulation import run_simulations
from .price_utils import resolve_price

logger = logging.getLogger(__name__)


class OpportunityCostAgent(BaseAgent):
    """Flag holdings that underperform simulated alternatives."""

    name = "opportunity_cost"

    def __init__(self, candidates: Sequence[str] | None = None, memory_agent: MemoryAgent | None = None) -> None:
        self.candidates = list(candidates or [])
        self.memory_agent = memory_agent
        self._misses: Dict[str, int] = {}

    # ------------------------------------------------------------------
    def _emotion_streak(self, token: str) -> int:
        if not self.memory_agent:
            return 0
        mem = getattr(self.memory_agent, "memory", None)
        if not mem or not hasattr(mem, "list_trades"):
            return 0
        trades = [
            t
            for t in mem.list_trades(token=token, limit=100)
            if hasattr(t, "emotion")
        ]
        if not trades:
            return 0
        trades.sort(key=lambda t: getattr(t, "timestamp", 0))
        last = trades[-1]
        neg = {"anxious", "regret", "bearish", "fear"}
        pos = {"confident", "bullish"}
        last_emo = getattr(last, "emotion", "")
        if last_emo in pos:
            sign = 1
        elif last_emo in neg:
            sign = -1
        else:
            return 0
        streak = 0
        for t in reversed(trades):
            emo = getattr(t, "emotion", "")
            if (sign > 0 and emo in pos) or (sign < 0 and emo in neg):
                streak += sign
            else:
                break
        return streak

    # ------------------------------------------------------------------
    def _score(self, token: str) -> float:
        sims = run_simulations(token, count=1)
        roi = sims[0].expected_roi if sims else 0.0
        mem = getattr(self.memory_agent, "memory", None) if self.memory_agent else None
        if mem and hasattr(mem, "list_trades") and mem.list_trades(token=token, limit=1):
            roi += 0.01 * self._emotion_streak(token)
        return roi

    # ------------------------------------------------------------------
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

        tokens = set(self.candidates)
        tokens.add(token)

        scores = {tok: self._score(tok) for tok in tokens}
        ranked = sorted(scores, key=scores.get, reverse=True)
        rank = ranked.index(token) + 1

        if rank > 5:
            self._misses[token] = self._misses.get(token, 0) + 1
        else:
            self._misses[token] = 0

        if self._misses.get(token, 0) >= 2:
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
