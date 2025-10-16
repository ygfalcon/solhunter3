from __future__ import annotations

from typing import List, Dict, Any, Iterable, AsyncGenerator, Callable, Awaitable
import inspect
import logging

from . import BaseAgent
from ..portfolio import Portfolio
from ..mempool_scanner import stream_ranked_mempool_tokens
from ..news import fetch_sentiment_async
from ..prices import (
    fetch_token_prices_async,
    get_cached_price,
    update_price_cache,
)


logger = logging.getLogger(__name__)


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
        price_helper: Callable[[str], float | Awaitable[float | None] | None]
        | None = None,
    ) -> None:
        self.threshold = float(threshold)
        self.scores: Dict[str, float] = {}
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []
        self._price_helper = price_helper

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
                price = await self._resolve_price(token, portfolio)
                if price is None:
                    logger.warning(
                        "MomentumAgent failed to hydrate price for buy action", extra={"token": token}
                    )
                    return []
                return [
                    {"token": token, "side": "buy", "amount": amount, "price": price}
                ]

        if token in portfolio.balances and score < self.threshold * 0.5:
            pos = portfolio.balances[token]
            price = await self._resolve_price(token, portfolio)
            if price is None:
                logger.warning(
                    "MomentumAgent failed to hydrate price for sell action", extra={"token": token}
                )
                return []
            return [
                {"token": token, "side": "sell", "amount": pos.amount, "price": price}
            ]

        return []

    async def _resolve_price(
        self, token: str, portfolio: Portfolio
    ) -> float | None:
        """Return a positive price for ``token`` or ``None`` if unavailable."""

        helper = self._price_helper
        if helper is not None:
            try:
                candidate = helper(token)
                if inspect.isawaitable(candidate):
                    candidate = await candidate  # type: ignore[func-returns-value]
                if candidate is not None:
                    price = float(candidate)
                    if price > 0:
                        return price
            except Exception:
                logger.debug("MomentumAgent price_helper failed", exc_info=True)

        cached = get_cached_price(token)
        if cached is not None and cached > 0:
            return float(cached)

        history = portfolio.price_history.get(token, [])
        if history:
            last = history[-1]
            try:
                price = float(last)
            except Exception:
                price = 0.0
            if price > 0:
                return price

        position = portfolio.balances.get(token)
        if position and position.entry_price > 0:
            return float(position.entry_price)

        try:
            prices = await fetch_token_prices_async([token])
        except Exception:
            logger.debug("MomentumAgent price fetch failed", exc_info=True)
            return None

        fetched = float(prices.get(token, 0.0) or 0.0)
        if fetched > 0:
            update_price_cache(token, fetched)
            return fetched
        return None
