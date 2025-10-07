from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import List, Dict, Any, Iterable

from sklearn.ensemble import GradientBoostingRegressor

from . import BaseAgent
from .price_utils import resolve_price
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
        trade_volume_threshold: float = 0.0,
        trade_liquidity_threshold: float = 0.0,
    ) -> None:
        self.mempool_score_threshold = float(mempool_score_threshold)
        self.trend_volume_threshold = float(trend_volume_threshold)
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []
        self.trade_volume_threshold = float(trade_volume_threshold)
        self.trade_liquidity_threshold = float(trade_liquidity_threshold)
        self.metrics: Dict[str, Dict[str, Any]] = {}
        self._executor: ThreadPoolExecutor | None = None

    def _get_executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            workers = min(4, max(2, (os.cpu_count() or 4)))
            self._executor = ThreadPoolExecutor(
                max_workers=workers,
                thread_name_prefix="smart-discovery",
            )
        return self._executor

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
        loop = asyncio.get_running_loop()
        executor = self._get_executor()
        volumes = await asyncio.gather(
            *[
                loop.run_in_executor(
                    executor,
                    partial(onchain_metrics.fetch_volume_onchain, t, rpc_url),
                )
                for t in tokens
            ]
        )
        liquidities = await asyncio.gather(
            *[
                loop.run_in_executor(
                    executor,
                    partial(onchain_metrics.fetch_liquidity_onchain, t, rpc_url),
                )
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
        details: Dict[str, Dict[str, float]] = {}
        for tok, evt, vol, liq in zip(tokens, events, volumes, liquidities):
            volume_val = float(vol or 0.0)
            liquidity_val = float(liq or 0.0)
            if volume_val < self.trend_volume_threshold:
                continue
            if liquidity_val < self.trade_liquidity_threshold:
                continue
            score = float(evt.get("combined_score", evt.get("score", 0.0)))
            feats.append([score, volume_val, liquidity_val, sentiment])
            kept_tokens.append(tok)
            details[tok] = {
                "mempool_score": score,
                "volume": volume_val,
                "liquidity": liquidity_val,
            }

        if not feats:
            self.metrics = {}
            return []

        y = [sum(f) for f in feats]
        model = GradientBoostingRegressor()
        model.fit(feats, y)
        preds = model.predict(feats)

        ranked = sorted(zip(kept_tokens, preds), key=lambda x: x[1], reverse=True)
        ranked_metrics: Dict[str, Dict[str, Any]] = {}
        for idx, (tok, pred_score) in enumerate(ranked, start=1):
            tok_details = details.get(tok, {})
            ranked_metrics[tok] = {
                "predicted_score": float(pred_score),
                "sentiment": float(sentiment),
                "rank": idx,
                "volume": float(tok_details.get("volume", 0.0)),
                "liquidity": float(tok_details.get("liquidity", 0.0)),
                "mempool_score": float(tok_details.get("mempool_score", 0.0)),
            }
        self.metrics = ranked_metrics
        return [tok for tok, _ in ranked]

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        stats = self.metrics.get(token)
        if not stats:
            return []

        predicted_score = float(stats.get("predicted_score", 0.0))
        if predicted_score <= 0:
            return []

        volume = float(stats.get("volume", 0.0))
        liquidity = float(stats.get("liquidity", 0.0))
        if volume < self.trade_volume_threshold:
            return []
        if liquidity < self.trade_liquidity_threshold:
            return []

        price, price_context = await resolve_price(token, portfolio)
        if price <= 0:
            return []

        metadata = {
            "predicted_score": predicted_score,
            "sentiment": float(stats.get("sentiment", 0.0)),
            "rank": int(stats.get("rank", 0)),
            "volume": volume,
            "liquidity": liquidity,
            "mempool_score": float(stats.get("mempool_score", 0.0)),
            "price_context": price_context,
        }

        return [
            {
                "token": token,
                "side": "buy",
                "amount": 1.0,
                "price": float(price),
                "metadata": metadata,
            }
        ]
