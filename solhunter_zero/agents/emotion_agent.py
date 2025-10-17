from __future__ import annotations

from typing import Any, Dict, List, Iterable, Callable, Optional

import logging
import time

from . import BaseAgent
from ..portfolio import Portfolio


from .. import news


logger = logging.getLogger(__name__)


def _clamp(x: float, lo: float = -1.0, hi: float = 1.0) -> float:
    return max(lo, min(hi, x))


class EmotionAgent(BaseAgent):
    """Assign emotion tags to executed trades and expose market sentiment."""

    name = "emotion"

    def __init__(
        self,
        feeds: Iterable[str] | None = None,
        twitter_feeds: Iterable[str] | None = None,
        discord_feeds: Iterable[str] | None = None,
        *,
        sentiment_source: Optional[
            Callable[[List[str], Optional[Iterable[str]], List[str], List[str]], float]
        ] = None,
        score_w_sentiment: float = 2.0,
        score_w_regret: float = 1.0,
        score_w_misfires: float = 1.0,
        cache_ttl: float = 30.0,
    ) -> None:
        self.feeds = list(feeds) if feeds else []
        self.twitter_feeds = list(twitter_feeds) if twitter_feeds else []
        self.discord_feeds = list(discord_feeds) if discord_feeds else []
        self.sentiment: float = 0.0

        self._sentiment_source = sentiment_source
        self._w_sent = float(score_w_sentiment)
        self._w_regret = float(score_w_regret)
        self._w_mis = float(score_w_misfires)

        self._cache_ttl = float(cache_ttl)
        self._last_fetch_ts: float = 0.0

    # ------------------------------------------------------------------
    def _stale(self) -> bool:
        return (time.time() - self._last_fetch_ts) >= self._cache_ttl

    # ------------------------------------------------------------------
    def update_sentiment(
        self, allowed: Iterable[str] | None = None, *, force: bool = False
    ) -> float:
        """Refresh ``self.sentiment`` by querying configured news feeds."""
        if not (self.feeds or self.twitter_feeds or self.discord_feeds):
            self.sentiment = 0.0
            return self.sentiment
        if not force and not self._stale():
            return self.sentiment
        try:
            fetch = self._sentiment_source or news.fetch_sentiment
            value = float(
                fetch(
                    self.feeds,
                    allowed,
                    twitter_urls=self.twitter_feeds,
                    discord_urls=self.discord_feeds,
                )
            )
            self.sentiment = _clamp(value)
            self._last_fetch_ts = time.time()
        except Exception as exc:  # pragma: no cover - unexpected errors
            logger.debug("EmotionAgent: sentiment fetch failed: %s", exc, exc_info=True)
            self.sentiment = 0.0
            self._last_fetch_ts = time.time()
        return self.sentiment

    def score(
        self, conviction_delta: float, regret: float, misfires: float, sentiment: float = 0.0
    ) -> float:
        """Combine factors into a single score."""
        return (
            conviction_delta
            - self._w_regret * regret
            - self._w_mis * misfires
            + self._w_sent * sentiment
        )

    def evaluate(self, action: Dict[str, Any], result: Any) -> str:
        """Return an emotion label for a completed trade."""
        delta = float(action.get("conviction_delta", 0.0) or 0.0)
        regret = float(action.get("regret", 0.0) or 0.0)
        misfires = float(action.get("misfires", 0.0) or 0.0)
        sentiment = self.update_sentiment()
        score = self.score(delta, regret, misfires, sentiment)
        if score > 0.5:
            return "confident"
        if score < -0.5:
            return "anxious"
        return "neutral"

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        # The emotion agent itself does not propose trades
        return []
