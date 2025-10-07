from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

from .discovery_service import DiscoveryService
from .evaluation_service import EvaluationService
from .execution_service import ExecutionService
from .scoring_service import ScoringService
from .types import ActionBundle, EvaluationResult, ExecutionReceipt, ScoredToken, TokenCandidate
from .feedback_service import FeedbackService

log = logging.getLogger(__name__)


class PipelineCoordinator:
    """Coordinate staged discovery→scoring→evaluation→execution."""

    def __init__(
        self,
        agent_manager,
        portfolio,
        *,
        discovery_interval: float = 5.0,
        discovery_cache_ttl: float = 20.0,
        scoring_batch: Optional[int] = None,
        evaluation_cache_ttl: float = 10.0,
        evaluation_workers: Optional[int] = None,
        execution_lanes: Optional[int] = None,
        on_evaluation = None,
        on_execution = None,
    ) -> None:
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        self.discovery_interval = discovery_interval
        self.discovery_cache_ttl = discovery_cache_ttl
        self.scoring_batch = scoring_batch
        self.evaluation_cache_ttl = evaluation_cache_ttl
        self.evaluation_workers = evaluation_workers
        self.execution_lanes = execution_lanes
        self.on_evaluation = on_evaluation
        self.on_execution = on_execution

        self._discovery_queue: asyncio.Queue[list[TokenCandidate]] = asyncio.Queue(maxsize=4)
        self._scoring_queue: asyncio.Queue[list[ScoredToken]] = asyncio.Queue(maxsize=4)
        self._execution_queue: asyncio.Queue[list[ActionBundle]] = asyncio.Queue(maxsize=4)

        self._discovery_service = DiscoveryService(
            self._discovery_queue,
            interval=self.discovery_interval,
            cache_ttl=self.discovery_cache_ttl,
            limit=int(os.getenv("DISCOVERY_LIMIT", "0") or 0) or None,
        )
        self._scoring_service = ScoringService(
            self._discovery_queue,
            self._scoring_queue,
            portfolio,
            max_batch=self.scoring_batch or int(os.getenv("PIPELINE_TOKEN_LIMIT", "0") or 0) or None,
        )
        self._evaluation_service = EvaluationService(
            self._scoring_queue,
            self._execution_queue,
            agent_manager,
            portfolio,
            default_workers=self.evaluation_workers or int(os.getenv("EVALUATION_WORKERS", "0") or 0) or None,
            cache_ttl=self.evaluation_cache_ttl,
            on_result=self._on_evaluation_result,
            should_skip=self._should_skip_token,
        )
        self._execution_service = ExecutionService(
            self._execution_queue,
            agent_manager,
            lane_workers=self.execution_lanes or int(os.getenv("EXECUTION_LANE_WORKERS", "0") or 0) or 2,
            on_receipt=self._on_execution_receipt,
        )
        self._feedback_service = FeedbackService(
            on_no_action=self._register_no_action,
            on_execution=self._register_execution_feedback,
        )

        self._telemetry: List[Dict[str, Any]] = []
        self._telemetry_lock = asyncio.Lock()
        self._stopped = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._no_action_cache: Dict[str, float] = {}

    async def start(self) -> None:
        await self._discovery_service.start()
        await self._scoring_service.start()
        await self._evaluation_service.start()
        await self._execution_service.start()
        await self._feedback_service.start()
        log.info(
            "PipelineCoordinator: all services started (discovery→scoring→evaluation→execution)"
        )

    async def stop(self) -> None:
        self._stopped.set()
        await self._execution_service.stop()
        await self._evaluation_service.stop()
        await self._scoring_service.stop()
        await self._discovery_service.stop()
        await self._feedback_service.stop()
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()

    async def _on_evaluation_result(self, result: EvaluationResult) -> None:
        payload = {
            "token": result.token,
            "latency": result.latency,
            "cached": result.cached,
            "actions": len(result.actions),
            "errors": result.errors,
        }
        await self._record_telemetry("evaluation", payload)
        await self._feedback_service.put(result)
        if self.on_evaluation:
            try:
                await self.on_evaluation(result)
            except Exception:
                log.exception("External evaluation callback failed")

    async def _on_execution_receipt(self, receipt: ExecutionReceipt) -> None:
        payload = {
            "token": receipt.token,
            "latency": receipt.finished_at - receipt.started_at,
            "success": receipt.success,
            "errors": receipt.errors,
        }
        await self._record_telemetry("execution", payload)
        await self._feedback_service.put(receipt)
        if self.on_execution:
            try:
                await self.on_execution(receipt)
            except Exception:
                log.exception("External execution callback failed")

    async def _record_telemetry(self, stage: str, payload: Dict[str, Any]) -> None:
        async with self._telemetry_lock:
            self._telemetry.append(
                {
                    "timestamp": time.time(),
                    "stage": stage,
                    **payload,
                }
            )
            if len(self._telemetry) > 500:
                self._telemetry = self._telemetry[-500:]

    async def snapshot_telemetry(self) -> List[Dict[str, Any]]:
        async with self._telemetry_lock:
            return list(self._telemetry)

    def _register_no_action(self, result: EvaluationResult) -> None:
        ttl = float(os.getenv("NO_ACTION_CACHE_TTL", "10") or 10.0)
        if ttl <= 0:
            return
        self._no_action_cache[result.token] = time.time() + ttl

    def _register_execution_feedback(self, receipt: ExecutionReceipt) -> None:
        if not receipt.success:
            return
        if receipt.token in self._no_action_cache:
            self._no_action_cache.pop(receipt.token, None)

    def _should_skip_token(self, token: str) -> bool:
        expiry = self._no_action_cache.get(token)
        if not expiry:
            return False
        if expiry < time.time():
            self._no_action_cache.pop(token, None)
            return False
        return True
