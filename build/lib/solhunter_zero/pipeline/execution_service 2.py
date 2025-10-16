from __future__ import annotations

import asyncio
import inspect
import logging
import time
from typing import Any, Awaitable, Callable, List, Optional

from ..event_bus import publish
from ..schemas import ActionExecuted
from .types import ActionBundle, ExecutionReceipt

log = logging.getLogger(__name__)


class ExecutionService:
    """Execute action bundles using agent manager's execution agent."""

    def __init__(
        self,
        input_queue: "asyncio.Queue[ActionBundle]",
        agent_manager,
        *,
        lane_workers: int = 2,
        on_receipt: Optional[Callable[[ExecutionReceipt], Awaitable[None] | None]] = None,
    ) -> None:
        self.input_queue = input_queue
        self.agent_manager = agent_manager
        try:
            lane_count = int(lane_workers)
        except (TypeError, ValueError):
            lane_count = 5
        self.lane_workers = max(5, lane_count)
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._worker_tasks: list[asyncio.Task] = []
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
        workers = [
            asyncio.create_task(self._worker_loop(idx), name=f"execution_worker:{idx}")
            for idx in range(self.lane_workers)
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
        lane_name = f"lane-{idx}"
        while not self._stopped.is_set():
            bundle: ActionBundle | None = None
            try:
                bundle = await self.input_queue.get()
            except asyncio.CancelledError:
                break
            try:
                receipt = await self._execute_bundle(bundle, lane=lane_name)
                await self._notify_receipt(receipt)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("ExecutionService %s failure: %s", lane_name, exc)
            finally:
                if bundle is not None:
                    self.input_queue.task_done()

    async def _notify_receipt(self, receipt: ExecutionReceipt) -> None:
        if not self._on_receipt:
            return
        try:
            maybe = self._on_receipt(receipt)
            if inspect.isawaitable(maybe):
                await maybe  # pragma: no branch - cooperative with async callbacks
        except Exception:  # pragma: no cover - defensive logging
            log.exception("Receipt callback failed")

    async def _execute_bundle(self, bundle: ActionBundle, *, lane: str | None = None) -> ExecutionReceipt:
        started = time.perf_counter()
        memory_agent = getattr(self.agent_manager, "memory_agent", None)
        async def _process_action(action: dict) -> tuple[Any | None, str | None]:
            try:
                result = await self.agent_manager.executor.execute(action)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("Execution error for %s", bundle.token)
                return None, str(exc)
            if memory_agent:
                try:
                    await memory_agent.log(action)
                except Exception:
                    log.exception("memory log failed for %s", bundle.token)
            publish(
                "action_executed",
                ActionExecuted(action=action, result=result),
            )
            return result, None

        tasks = [_process_action(action) for action in bundle.actions]
        results: List[Any] = []
        errors: List[str] = []
        if tasks:
            processed = await asyncio.gather(*tasks, return_exceptions=True)
            for outcome in processed:
                if isinstance(outcome, Exception):
                    errors.append(str(outcome))
                    continue
                result, error = outcome
                if error:
                    errors.append(error)
                elif result is not None:
                    results.append(result)
        finished = time.perf_counter()
        return ExecutionReceipt(
            token=bundle.token,
            success=not errors,
            results=results,
            errors=errors,
            started_at=started,
            finished_at=finished,
            lane=lane,
        )
