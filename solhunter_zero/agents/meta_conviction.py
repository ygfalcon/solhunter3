from __future__ import annotations

import asyncio
import os
from typing import List, Dict, Any, Iterable

import numpy as np

from .. import models
from .. import simulation
from ..simulation import fetch_token_metrics

from . import BaseAgent
from .simulation import SimulationAgent
from .conviction import ConvictionAgent
from .ramanujan_agent import RamanujanAgent
from ..portfolio import Portfolio


class MetaConvictionAgent(BaseAgent):
    """Combine several conviction agents into a single decision."""

    name = "meta_conviction"

    def __init__(
        self,
        weights: Dict[str, float] | None = None,
        *,
        model_path: str | None = None,
        feeds: Iterable[str] | None = None,
        twitter_feeds: Iterable[str] | None = None,
        discord_feeds: Iterable[str] | None = None,
    ) -> None:
        self.sim_agent = SimulationAgent()
        self.conv_agent = ConvictionAgent(
            model_path=model_path,
            feeds=feeds,
            twitter_feeds=twitter_feeds,
            discord_feeds=discord_feeds,
        )
        self.ram_agent = RamanujanAgent()
        self.model_path = model_path or os.getenv("PRICE_MODEL_PATH")
        self.weights = weights or {
            "simulation": 1.0,
            "conviction": 1.0,
            "ramanujan": 1.0,
            "prediction": 1.0,
        }

    def _predict_return(self, token: str) -> float:
        if not self.model_path:
            return 0.0
        model = simulation.get_price_model(self.model_path)
        if not model:
            return 0.0
        metrics = fetch_token_metrics(token)
        ph = metrics.get("price_history") or []
        lh = metrics.get("liquidity_history") or []
        dh = metrics.get("depth_history") or []
        th = metrics.get("tx_count_history") or []
        n = min(len(ph), len(lh), len(dh), len(th or ph))
        if n < 30:
            return 0.0
        seq = np.column_stack([ph[-30:], lh[-30:], dh[-30:], (th or [0] * n)[-30:]])
        try:
            return float(model.predict(seq))
        except Exception:
            return 0.0

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
        regime: str | None = None,
    ) -> List[Dict[str, Any]]:
        results = await asyncio.gather(
            self.sim_agent.propose_trade(
                token,
                portfolio,
                depth=depth,
                imbalance=imbalance,
                regime=regime,
            ),
            self.conv_agent.propose_trade(token, portfolio, depth=depth, imbalance=imbalance),
            self.ram_agent.propose_trade(token, portfolio, depth=depth, imbalance=imbalance),
        )

        agents = [self.sim_agent, self.conv_agent, self.ram_agent]
        conviction = 0.0
        for agent, res in zip(agents, results):
            if not res:
                continue
            side = res[0].get("side")
            weight = self.weights.get(agent.name, 1.0)
            if side == "buy":
                conviction += weight
            elif side == "sell":
                conviction -= weight

        pred = self._predict_return(token)
        if pred > self.conv_agent.threshold:
            conviction += self.weights.get("prediction", 1.0)
        elif pred < -self.conv_agent.threshold:
            conviction -= self.weights.get("prediction", 1.0)

        if conviction > 0:
            return [{"token": token, "side": "buy", "amount": 1.0, "price": 0.0}]
        if conviction < 0:
            pos = portfolio.balances.get(token)
            if pos:
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": 0.0}]
        return []

    def apply_threshold_profile(self, profile):
        """Propagate the profile to the internal simulation agent."""

        self.sim_agent.apply_threshold_profile(profile)
