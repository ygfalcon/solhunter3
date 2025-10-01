from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Dict, List, Optional

from .types import ActionBundle, EvaluationResult, ScoredToken

log = logging.getLogger(__name__)


class EvaluationService:
    """Run agent evaluations in parallel with memoisation."""

    def __init__(
        self,
        input_queue: "asyncio.Queue[list[ScoredToken]]",
        output_queue: "asyncio.Queue[list[ActionBundle]]",
        agent_manager,
        portfolio,
        *,
        default_workers: Optional[int] = None,
        cache_ttl: float = 10.0,
        on_result = None,
        should_skip = None,
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        self.cache_ttl = max(0.0, cache_ttl)
        self._cache: Dict[str, tuple[float, EvaluationResult]] = {}
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._worker_limit = default_workers or (os.cpu_count() or 4)
        self._on_result = on_result
        self._should_skip = should_skip

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="evaluation_service")

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
        sem = asyncio.Semaphore(max(1, self._worker_limit))
        while not self._stopped.is_set():
            try:
                scored_batch = await self.input_queue.get()
                self.input_queue.task_done()
                if self._should_skip:
                    scored_batch = [st for st in scored_batch if not self._should_skip(st.token)]
                    if not scored_batch:
                        log.debug("EvaluationService skipped batch (all tokens cached)")
                        continue
                tasks = [
                    asyncio.create_task(self._evaluate_token(st, sem), name=f"eval:{st.token}")
                    for st in scored_batch
                ]
                bundles: List[ActionBundle] = []
                for task in asyncio.as_completed(tasks):
                    try:
                        result = await task
                    except Exception as exc:
                        log.exception("Evaluation task failed: %s", exc)
                        continue
                    if result and result.actions:
                        if self._on_result:
                            try:
                                await self._on_result(result)
                            except Exception:  # pragma: no cover - defensive
                                log.exception("Result callback failed")
                        bundles.append(
                            ActionBundle(
                                token=result.token,
                                actions=result.actions,
                                created_at=time.time(),
                                metadata={"latency": result.latency, "cached": result.cached},
                            )
                        )
                    elif result and self._on_result:
                        try:
                            await self._on_result(result)
                        except Exception:
                            log.exception("Result callback failed")
                if bundles:
                    await self.output_queue.put(bundles)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("EvaluationService failure: %s", exc)

    async def _evaluate_token(self, scored: ScoredToken, sem: asyncio.Semaphore) -> Optional[EvaluationResult]:
        async with sem:
            now = time.time()
            cached = self._cache.get(scored.token)
            if cached and (now - cached[0]) < self.cache_ttl:
                return EvaluationResult(
                    token=scored.token,
                    actions=list(cached[1].actions),
                    latency=0.0,
                    cached=True,
                    metadata=dict(cached[1].metadata),
                )
            start = time.perf_counter()
            errors: List[str] = []
            actions: List[Dict] = []
            try:
                ctx = await self.agent_manager.evaluate_with_swarm(scored.token, self.portfolio)
                actions = list(ctx.actions)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                errors.append(str(exc))
                log.exception("Evaluation failed for %s", scored.token)
            latency = time.perf_counter() - start
            result = EvaluationResult(
                token=scored.token,
                actions=actions,
                latency=latency,
                cached=False,
                errors=errors,
                metadata={"score": scored.score, "rank": scored.rank},
            )
            if self.cache_ttl and not errors:
                self._cache[scored.token] = (time.time(), result)
            return result
