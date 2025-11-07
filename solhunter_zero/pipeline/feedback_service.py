from __future__ import annotations

import asyncio
import logging
import os
from typing import Callable, Optional

from .types import EvaluationResult, ExecutionReceipt

log = logging.getLogger(__name__)


class FeedbackService:
    """Consume receipts/results and update caches or telemetry."""

    def __init__(
        self,
        *,
        on_no_action: Optional[Callable[[EvaluationResult], None]] = None,
        on_execution: Optional[Callable[[ExecutionReceipt], None]] = None,
        workers: Optional[int] = None,
    ) -> None:
        self.on_no_action = on_no_action
        self.on_execution = on_execution
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()
        env_workers: Optional[int] = None
        raw_env = os.getenv("FEEDBACK_WORKERS")
        if raw_env:
            try:
                env_workers = int(raw_env)
            except ValueError:
                log.warning("Invalid FEEDBACK_WORKERS=%r; ignoring", raw_env)
        chosen = workers if workers is not None else env_workers
        if chosen is None or chosen <= 0:
            chosen = 5
        self._worker_limit = max(5, int(chosen))
        self._worker_tasks: list[asyncio.Task] = []

    async def put(self, item) -> None:
        await self._queue.put(item)

    async def start(self) -> None:
        if self._task is not None and self._task.done():
            self._task = None
        if self._task is not None and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(self._run(), name="feedback_service")

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
        workers = [
            asyncio.create_task(self._worker_loop(idx), name=f"feedback_worker:{idx}")
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

    async def _worker_loop(self, idx: int) -> None:
        name = f"feedback-{idx}"
        while not self._stopped.is_set():
            item = None
            try:
                item = await self._queue.get()
            except asyncio.CancelledError:
                break
            try:
                if isinstance(item, EvaluationResult):
                    await self._handle_evaluation(item)
                elif isinstance(item, ExecutionReceipt):
                    await self._handle_execution(item)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("FeedbackService %s failure: %s", name, exc)
            finally:
                if item is not None:
                    self._queue.task_done()

    async def _handle_evaluation(self, result: EvaluationResult) -> None:
        if result.actions:
            return
        if self.on_no_action:
            try:
                self.on_no_action(result)
            except Exception:
                log.exception("on_no_action callback failed")

    async def _handle_execution(self, receipt: ExecutionReceipt) -> None:
        if self.on_execution:
            try:
                self.on_execution(receipt)
            except Exception:
                log.exception("on_execution callback failed")
