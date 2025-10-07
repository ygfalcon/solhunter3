from __future__ import annotations

import asyncio
import logging
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
    ) -> None:
        self.on_no_action = on_no_action
        self.on_execution = on_execution
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: Optional[asyncio.Task] = None
        self._stopped = asyncio.Event()

    async def put(self, item) -> None:
        await self._queue.put(item)

    async def start(self) -> None:
        if self._task is None:
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

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                item = await self._queue.get()
                self._queue.task_done()
                if isinstance(item, EvaluationResult):
                    await self._handle_evaluation(item)
                elif isinstance(item, ExecutionReceipt):
                    await self._handle_execution(item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("FeedbackService failure: %s", exc)

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
