from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Awaitable, Callable, List, Optional

from ..event_bus import publish
from ..schemas import ActionExecuted
from .types import ActionBundle, ExecutionReceipt

log = logging.getLogger(__name__)


class ExecutionService:
    """Execute action bundles using agent manager's execution agent."""

    def __init__(
        self,
        input_queue: "asyncio.Queue[list[ActionBundle]]",
        agent_manager,
        *,
        lane_workers: int = 2,
        on_receipt: Optional[Callable[[ExecutionReceipt], Awaitable[None] | None]] = None,
    ) -> None:
        self.input_queue = input_queue
        self.agent_manager = agent_manager
        self.lane_workers = max(1, lane_workers)
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._lane_semaphore = asyncio.Semaphore(self.lane_workers)
        self._on_receipt = on_receipt

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="execution_service")

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
                bundles = await self.input_queue.get()
                self.input_queue.task_done()
                tasks = [
                    asyncio.create_task(self._execute_bundle(bundle), name=f"exec:{bundle.token}")
                    for bundle in bundles
                ]
                for task in asyncio.as_completed(tasks):
                    try:
                        receipt = await task
                        await self._notify_receipt(receipt)
                    except Exception as exc:
                        log.exception("Execution task failed: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("ExecutionService failure: %s", exc)

    async def _notify_receipt(self, receipt: ExecutionReceipt) -> None:
        if not self._on_receipt:
            return
        try:
            maybe = self._on_receipt(receipt)
            if inspect.isawaitable(maybe):
                await maybe  # pragma: no branch - cooperative with async callbacks
        except Exception:  # pragma: no cover - defensive logging
            log.exception("Receipt callback failed")

    async def _execute_bundle(self, bundle: ActionBundle) -> ExecutionReceipt:
        async with self._lane_semaphore:
            started = time.perf_counter()
            results: List = []
            errors: List[str] = []
            for action in bundle.actions:
                try:
                    result = await self.agent_manager.executor.execute(action)
                    results.append(result)
                    memory_agent = getattr(self.agent_manager, "memory_agent", None)
                    if memory_agent:
                        try:
                            await memory_agent.log(action)
                        except Exception:
                            log.exception("memory log failed for %s", bundle.token)
                    publish(
                        "action_executed",
                        ActionExecuted(action=action, result=result),
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    errors.append(str(exc))
                    log.exception("Execution error for %s", bundle.token)
            finished = time.perf_counter()
            return ExecutionReceipt(
                token=bundle.token,
                success=not errors,
                results=results,
                errors=errors,
                started_at=started,
                finished_at=finished,
            )
