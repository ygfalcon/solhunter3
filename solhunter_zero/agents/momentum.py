from __future__ import annotations

from typing import List, Dict, Any, Iterable, AsyncGenerator

from . import BaseAgent
from ..portfolio import Portfolio
from ..mempool_scanner import stream_ranked_mempool_tokens
from ..news import fetch_sentiment_async


class MomentumAgent(BaseAgent):
    """Trade based on mempool momentum adjusted by social sentiment."""

    name = "momentum"

    def __init__(
        self,
        threshold: float = 0.0,
        *,
        feeds: Iterable[str] | None = None,
        twitter_feeds: Iterable[str] | None = None,
        discord_feeds: Iterable[str] | None = None,
    ) -> None:
        self.threshold = float(threshold)
        self.scores: Dict[str, float] = {}
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []

    async def stream_scores(
        self, rpc_url: str, **kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Update internal score table by listening to mempool events."""
        async for event in stream_ranked_mempool_tokens(rpc_url, **kwargs):
            score = float(event.get("combined_score", 0.0))
            self.scores[event["address"]] = score
            yield event

    async def _sentiment(self) -> float:
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
        score = self.scores.get(token, 0.0)
        sentiment = await self._sentiment()
        amount_scale = max(0.0, 1.0 + sentiment)

        if score >= self.threshold:
            amount = max(0.0, score) * amount_scale
            if amount > 0:
                return [{"token": token, "side": "buy", "amount": amount, "price": 0.0}]

        if token in portfolio.balances and score < self.threshold * 0.5:
            pos = portfolio.balances[token]
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": 0.0}]

        return []
