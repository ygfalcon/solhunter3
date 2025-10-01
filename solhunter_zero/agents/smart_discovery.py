from __future__ import annotations

import asyncio
from typing import List, Dict, Any, Iterable

from sklearn.ensemble import GradientBoostingRegressor

from . import BaseAgent
from ..mempool_scanner import stream_ranked_mempool_tokens
from .. import onchain_metrics, news
from ..portfolio import Portfolio


class SmartDiscoveryAgent(BaseAgent):
    """Rank new tokens using mempool, on-chain metrics and social sentiment."""

    name = "smart_discovery"

    def __init__(
        self,
        mempool_score_threshold: float = 0.0,
        trend_volume_threshold: float = 0.0,
        *,
        feeds: Iterable[str] | None = None,
        twitter_feeds: Iterable[str] | None = None,
        discord_feeds: Iterable[str] | None = None,
    ) -> None:
        self.mempool_score_threshold = float(mempool_score_threshold)
        self.trend_volume_threshold = float(trend_volume_threshold)
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []
        self.metrics: Dict[str, Dict[str, float]] = {}

    async def discover_tokens(self, rpc_url: str, limit: int = 10) -> List[str]:
        gen = stream_ranked_mempool_tokens(
            rpc_url, threshold=self.mempool_score_threshold
        )
        events: List[Dict[str, Any]] = []
        try:
            while len(events) < limit:
                evt = await asyncio.wait_for(anext(gen), timeout=0.5)
                events.append(evt)
        except (StopAsyncIteration, asyncio.TimeoutError):
            pass
        finally:
            await gen.aclose()

        tokens = [e["address"] for e in events]
        volumes = await asyncio.gather(
            *[
                asyncio.to_thread(onchain_metrics.fetch_volume_onchain, t, rpc_url)
                for t in tokens
            ]
        )
        liquidities = await asyncio.gather(
            *[
                asyncio.to_thread(onchain_metrics.fetch_liquidity_onchain, t, rpc_url)
                for t in tokens
            ]
        )
        sentiment = 0.0
        if self.feeds or self.twitter_feeds or self.discord_feeds:
            try:
                sentiment = await news.fetch_sentiment_async(
                    self.feeds,
                    twitter_urls=self.twitter_feeds,
                    discord_urls=self.discord_feeds,
                )
            except Exception:
                sentiment = 0.0

        feats: List[List[float]] = []
        kept_tokens: List[str] = []
        for tok, evt, vol, liq in zip(tokens, events, volumes, liquidities):
            if float(vol) < self.trend_volume_threshold:
                continue
            score = float(evt.get("combined_score", evt.get("score", 0.0)))
            feats.append([score, float(vol), float(liq), sentiment])
            kept_tokens.append(tok)

        if not feats:
            return []

        y = [sum(f) for f in feats]
        model = GradientBoostingRegressor()
        model.fit(feats, y)
        preds = model.predict(feats)

        ranked = sorted(zip(kept_tokens, preds), key=lambda x: x[1], reverse=True)
        self.metrics = {tok: {"predicted_score": float(p)} for tok, p in ranked}
        return [tok for tok, _ in ranked]

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        return []
