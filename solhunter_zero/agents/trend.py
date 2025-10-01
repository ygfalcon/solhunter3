from __future__ import annotations

from typing import List, Dict, Any, Iterable

from . import BaseAgent
from ..portfolio import Portfolio
from ..scanner_common import fetch_trending_tokens_async
from ..simulation import fetch_token_metrics_async
from ..news import fetch_sentiment_async


class TrendAgent(BaseAgent):
    """Buy trending tokens when volume and sentiment are strong."""

    name = "trend"

    def __init__(
        self,
        volume_threshold: float = 0.0,
        sentiment_threshold: float = 0.0,
        feeds: Iterable[str] | None = None,
        twitter_feeds: Iterable[str] | None = None,
        discord_feeds: Iterable[str] | None = None,
    ) -> None:
        self.volume_threshold = volume_threshold
        self.sentiment_threshold = sentiment_threshold
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []

    async def _current_sentiment(self) -> float:
        if not (self.feeds or self.twitter_feeds or self.discord_feeds):
            return 0.0
        try:
            return await fetch_sentiment_async(
                self.feeds,
                twitter_urls=self.twitter_feeds,
                discord_urls=self.discord_feeds,
            )
        except Exception:
            return 0.0

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        trending = await fetch_trending_tokens_async()
        if token not in trending:
            return []
        metrics = await fetch_token_metrics_async(token)
        volume = float(metrics.get("volume", 0.0))
        sentiment = await self._current_sentiment()
        if volume >= self.volume_threshold and sentiment >= self.sentiment_threshold:
            strength = sentiment * 2
            return [
                {
                    "token": token,
                    "side": "buy",
                    "amount": 1.0,
                    "price": 0.0,
                    "volume": volume,
                    "sentiment": strength,
                }
            ]
        return []
