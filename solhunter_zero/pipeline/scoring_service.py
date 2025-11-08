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
        output_queue: "asyncio.Queue[ScoredToken]",
        portfolio,
        *,
        max_batch: Optional[int] = None,
        cooldown: float = 2.0,
        workers: Optional[int] = None,
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.portfolio = portfolio
        self.max_batch = max_batch
        self.cooldown = max(0.0, float(cooldown))
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        self._last_scores: dict[str, tuple[float, float]] = {}
        env_workers: Optional[int] = None
        raw_env = os.getenv("SCORING_WORKERS")
        if raw_env:
            try:
                env_workers = int(raw_env)
            except ValueError:
                log.warning("Invalid SCORING_WORKERS=%r; ignoring", raw_env)

        chosen: Optional[int] = None
        if workers is not None and workers >= 1:
            chosen = int(workers)
        elif env_workers is not None and env_workers >= 1:
            chosen = int(env_workers)

        if chosen is None:
            # Fall back to a reasonable default (CPU count or 5) when no valid value is provided.
            fallback = os.cpu_count() or 5
            chosen = max(1, int(fallback))

        self._worker_count = chosen
        self._worker_tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        if self._task is not None and self._task.done():
            self._task = None
        if self._task is not None and not self._task.done():
            return
        self._stopped.clear()
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
        if self._worker_tasks:
            for task in self._worker_tasks:
                task.cancel()
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks.clear()

    async def _run(self) -> None:
        cooldown = max(self.cooldown, float(os.getenv("SCORING_COOLDOWN", "0.5") or 0.5))
        workers = [
            asyncio.create_task(self._worker_loop(idx, cooldown), name=f"scoring_worker:{idx}")
            for idx in range(self._worker_count)
        ]
        self._worker_tasks = workers
        try:
            await self._stopped.wait()
        except asyncio.CancelledError:
            pass
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self._worker_tasks.clear()

    async def _worker_loop(self, idx: int, cooldown: float) -> None:
        name = f"scoring-{idx}"
        while not self._stopped.is_set():
            batch: list[TokenCandidate] | None = None
            try:
                batch = await self.input_queue.get()
            except asyncio.CancelledError:
                break
            try:
                scored = self._score_batch(batch)
                if scored:
                    for scored_token in scored:
                        await self.output_queue.put(scored_token)
                if cooldown:
                    await asyncio.sleep(cooldown)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pragma: no cover - logging
                log.exception("ScoringService %s failure: %s", name, exc)
            finally:
                if batch is not None:
                    self.input_queue.task_done()

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
