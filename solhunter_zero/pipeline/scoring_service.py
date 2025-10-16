from __future__ import annotations

import asyncio
import heapq
import logging
import math
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
        cooldown: float = 0.0,
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
        self._fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        env_workers: Optional[int] = None
        raw_env = os.getenv("SCORING_WORKERS")
        if raw_env:
            try:
                env_workers = int(raw_env)
            except ValueError:
                log.warning("Invalid SCORING_WORKERS=%r; ignoring", raw_env)
        chosen: Optional[int] = None
        if env_workers is not None:
            chosen = env_workers
        elif workers is not None:
            chosen = workers
        else:
            if self._fast_mode:
                chosen = 2
            else:
                cpu_default = os.cpu_count() or 4
                chosen = min(max(4, cpu_default), 16)
        if chosen is None or chosen <= 0:
            chosen = 1
        self._worker_limit = max(1, min(int(chosen), 32))
        self._worker_tasks: list[asyncio.Task] = []
        raw_timeout = os.getenv("SCORING_OUTPUT_PUT_TIMEOUT")
        timeout_value = 0.0
        if raw_timeout:
            try:
                timeout_value = float(raw_timeout)
            except ValueError:
                log.warning("Invalid SCORING_OUTPUT_PUT_TIMEOUT=%r; defaulting to 0", raw_timeout)
                timeout_value = 0.0
        self._output_put_timeout = max(0.0, timeout_value)

    async def start(self) -> None:
        if self._task is None:
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
        log.info("ScoringService stopped")

    async def _run(self) -> None:
        cooldown = max(0.0, self.cooldown)
        raw_env = os.getenv("SCORING_COOLDOWN")
        if raw_env is not None:
            try:
                cooldown = max(0.0, float(raw_env))
            except ValueError:
                log.warning("Invalid SCORING_COOLDOWN=%r; using %.3fs", raw_env, cooldown)
        if self._fast_mode:
            cooldown = min(cooldown, 0.25)
        log.info("ScoringService starting with workers=%d cooldown=%.3fs", self._worker_limit, cooldown)
        workers = [
            asyncio.create_task(self._worker_loop(idx, cooldown), name=f"scoring_worker:{idx}")
            for idx in range(self._worker_limit)
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
                return
            try:
                scored = self._score_batch(batch)
                if scored:
                    for scored_token in scored:
                        await self._put_scored_token(scored_token)
                if cooldown:
                    await asyncio.sleep(cooldown)
            except asyncio.CancelledError:
                return
            except Exception:  # pragma: no cover - logging
                log.exception("ScoringService worker %s encountered an error", name)
            finally:
                if batch is not None:
                    self.input_queue.task_done()

    async def _put_scored_token(self, scored_token: ScoredToken) -> None:
        timeout = self._output_put_timeout
        if timeout <= 0:
            await self.output_queue.put(scored_token)
            return
        for attempt in range(2):
            try:
                await asyncio.wait_for(self.output_queue.put(scored_token), timeout=timeout)
                return
            except asyncio.TimeoutError:
                if attempt == 0:
                    continue
                log.warning(
                    "Dropping scored token %s after output queue timeout (%.2fs)",
                    scored_token.token,
                    timeout,
                )
                return

    def _score_batch(self, batch: Iterable[TokenCandidate]) -> list[ScoredToken]:
        if not isinstance(batch, list):
            batch = list(batch)
        batch_size = len(batch)
        now = time.time()
        best_by_token: dict[str, tuple[float, float, TokenCandidate]] = {}
        for candidate in batch:
            token = getattr(candidate, "token", None)
            if not isinstance(token, str) or not token:
                continue
            metadata = getattr(candidate, "metadata", None)
            if metadata is None:
                candidate.metadata = metadata = {}
            profile = metadata.get("profile") if isinstance(metadata, dict) else {}
            if not profile:
                profile = self._build_profile(token)
                if isinstance(metadata, dict):
                    metadata.setdefault("profile", profile)
            try:
                score = float(heuristic_score(token, self.portfolio, profile))
            except Exception as exc:  # pragma: no cover - defensive
                log.warning("Failed to score token %s: %s", token, exc)
                score = 0.0
            discovered_at = getattr(candidate, "discovered_at", None)
            try:
                discovered_key = float(discovered_at)
            except (TypeError, ValueError):
                discovered_key = math.inf
            entry = best_by_token.get(token)
            if entry is None or score > entry[0] or (
                math.isclose(score, entry[0]) and discovered_key < entry[1]
            ):
                best_by_token[token] = (score, discovered_key, candidate)
        heap: list[tuple[float, float, str, TokenCandidate]] = []
        for token, (score, discovered_key, candidate) in best_by_token.items():
            prev = self._last_scores.get(token)
            if prev and (now - prev[1]) < 1.0 and abs(score - prev[0]) < 0.02:
                # Gentle smoothing to dampen tiny oscillations without lag.
                score = 0.5 * score + 0.5 * prev[0]
            self._last_scores[token] = (score, now)
            heapq.heappush(heap, (-score, discovered_key, token, candidate))
        limit_value: Optional[int] = self.max_batch
        if limit_value is None:
            env_limit_raw = os.getenv("PIPELINE_TOKEN_LIMIT")
            if env_limit_raw:
                try:
                    limit_value = int(env_limit_raw)
                except ValueError:
                    log.warning(
                        "Invalid PIPELINE_TOKEN_LIMIT=%r; ignoring", env_limit_raw
                    )
        if limit_value is not None and limit_value <= 0:
            limit_value = None
        effective_limit = limit_value if limit_value is not None else len(heap)
        if limit_value is not None and batch_size > 3 * limit_value:
            log.debug(
                "ScoringService oversized input batch: size=%d limit=%d", batch_size, limit_value
            )
        top: list[ScoredToken] = []
        rank = 0
        heap_count = len(heap)
        select_count = min(effective_limit, heap_count)
        if select_count:
            log.debug(
                "ScoringService selecting top %d from batch size %d", select_count, batch_size
            )
        while heap and len(top) < effective_limit:
            neg_score, discovered_key, token, cand = heapq.heappop(heap)
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
        history_source = getattr(self.portfolio, "price_history", None)
        history = []
        if isinstance(history_source, dict):
            history = history_source.get(token) or []
        if not history:
            return {"trend_score": 0.0, "volume_score": 0.0}
        window = list(history[-min(len(history), 10) :])
        trend = 0.0
        if len(window) >= 2:
            try:
                first = float(window[0])
                last = float(window[-1])
            except (TypeError, ValueError):
                first = last = 0.0
            denom = abs(first)
            if denom < 1e-6:
                denom = 1.0
            trend = (last - first) / denom
        trend_score = max(-10.0, min(10.0, trend * 5.0))
        risk_metrics = getattr(self.portfolio, "risk_metrics", None)
        volume_score = 0.0
        if isinstance(risk_metrics, dict):
            raw_volume = risk_metrics.get(token, 0.0)
            try:
                volume_score = float(raw_volume)
            except (TypeError, ValueError):
                volume_score = 0.0
        return {
            "trend_score": trend_score,
            "volume_score": volume_score,
        }
