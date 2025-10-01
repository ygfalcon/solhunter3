from __future__ import annotations

import asyncio
import heapq
import logging
import os
import time
from typing import Iterable, Optional

from .scoring import heuristic_score
from .types import ScoredToken, TokenCandidate

log = logging.getLogger(__name__)


class ScoringService:
    """Rank token candidates and feed top entries into evaluation."""

    def __init__(
        self,
        input_queue: "asyncio.Queue[list[TokenCandidate]]",
        output_queue: "asyncio.Queue[list[ScoredToken]]",
        portfolio,
        *,
        max_batch: Optional[int] = None,
        cooldown: float = 2.0,
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.portfolio = portfolio
        self.max_batch = max_batch
        self.cooldown = max(0.0, float(cooldown))
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._last_scores: dict[str, tuple[float, float]] = {}

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="scoring_service")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        cooldown = max(self.cooldown, float(os.getenv("SCORING_COOLDOWN", "0.5") or 0.5))
        while not self._stopped.is_set():
            try:
                batch = await self.input_queue.get()
                self.input_queue.task_done()
                scored = self._score_batch(batch)
                if scored:
                    await self.output_queue.put(scored)
                if cooldown:
                    await asyncio.sleep(cooldown)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - logging
                log.exception("ScoringService failure: %s", exc)

    def _score_batch(self, batch: Iterable[TokenCandidate]) -> list[ScoredToken]:
        heap: list[tuple[float, TokenCandidate]] = []
        now = time.time()
        for candidate in batch:
            profile = candidate.metadata.get("profile") if candidate.metadata else {}
            if not profile:
                profile = self._build_profile(candidate.token)
                candidate.metadata.setdefault("profile", profile)
            score = heuristic_score(candidate.token, self.portfolio, profile)
            prev = self._last_scores.get(candidate.token)
            if prev and (now - prev[1]) < 1.0 and abs(score - prev[0]) < 0.01:
                score = (score + prev[0]) / 2.0
            heapq.heappush(heap, (-score, candidate))
            self._last_scores[candidate.token] = (score, now)
        limit = self.max_batch or len(heap)
        top: list[ScoredToken] = []
        rank = 0
        while heap and len(top) < limit:
            neg_score, cand = heapq.heappop(heap)
            rank += 1
            top.append(
                ScoredToken(
                    token=cand.token,
                    score=-neg_score,
                    rank=rank,
                    candidate=cand,
                    profile={"source": cand.source, "discovered_at": cand.discovered_at},
                )
            )
        return top

    def _build_profile(self, token: str) -> dict:
        history = getattr(self.portfolio, "price_history", {}).get(token) or []
        window = history[-min(len(history), 10) :]
        trend = 0.0
        if len(window) >= 2:
            first = window[0] or 1.0
            last = window[-1]
            trend = (last - first) / (abs(first) or 1.0)
        volume_score = float(getattr(self.portfolio, "risk_metrics", {}).get(token, 0.0))
        return {
            "trend_score": trend * 5.0,
            "volume_score": volume_score,
        }
