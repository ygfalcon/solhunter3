from __future__ import annotations

from typing import List, Dict, Any, Iterable

import os
import numpy as np

from .. import models
from ..simulation import (
    run_simulations,
    predict_price_movement as _predict_price_movement,
    predict_token_activity as _predict_activity,
)
from ..news import fetch_sentiment


def predict_price_movement(
    token: str, *, sentiment: float | None = None, model_path: str | None = None
) -> float:
    """Wrapper around :func:`simulation.predict_price_movement`."""
    return _predict_price_movement(token, sentiment=sentiment, model_path=model_path)


def predict_token_activity(token: str, *, model_path: str | None = None) -> float:
    """Wrapper around :func:`simulation.predict_token_activity`."""
    return _predict_activity(token, model_path=model_path)

from . import BaseAgent
from ..portfolio import Portfolio


class ConvictionAgent(BaseAgent):
    """Basic conviction calculation using expected ROI."""

    name = "conviction"

    def __init__(
        self,
        threshold: float = 0.05,
        count: int = 100,
        *,
        model_path: str | None = None,
        activity_model_path: str | None = None,
        feeds: Iterable[str] | None = None,
        twitter_feeds: Iterable[str] | None = None,
        discord_feeds: Iterable[str] | None = None,
    ):
        self.threshold = threshold
        self.count = count
        self.model_path = model_path or os.getenv("PRICE_MODEL_PATH")
        self.activity_model_path = activity_model_path or os.getenv("ACTIVITY_MODEL_PATH")
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []

    def _predict_return(self, token: str) -> float:
        sentiment = 0.0
        if self.feeds or self.twitter_feeds or self.discord_feeds:
            try:
                sentiment = fetch_sentiment(
                    self.feeds,
                    twitter_urls=self.twitter_feeds,
                    discord_urls=self.discord_feeds,
                )
            except Exception:
                sentiment = 0.0
        return predict_price_movement(token, sentiment=sentiment, model_path=self.model_path)

    def _predict_activity(self, token: str) -> float:
        return predict_token_activity(token, model_path=self.activity_model_path)

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        sims = run_simulations(token, count=self.count, order_book_strength=depth)
        if not sims:
            return []
        avg_roi = sum(r.expected_roi for r in sims) / len(sims)
        pred = self._predict_return(token)
        activity = self._predict_activity(token)
        if abs(pred) >= self.threshold * 0.5:
            avg_roi = (avg_roi + pred) / 2
        if activity:
            avg_roi += activity
        if imbalance is not None:
            avg_roi += imbalance * 0.05
        if avg_roi > self.threshold:
            return [{"token": token, "side": "buy", "amount": 1.0, "price": 0.0}]
        if avg_roi < -self.threshold:
            pos = portfolio.balances.get(token)
            if pos:
                return [{"token": token, "side": "sell", "amount": pos.amount, "price": 0.0}]
        return []
