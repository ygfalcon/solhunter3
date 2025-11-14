from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional, cast

from .discovery_service import DiscoveryService
from .evaluation_service import EvaluationService
from .execution_service import ExecutionService
from .scoring_service import ScoringService
from .portfolio_management_service import PortfolioManagementService
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
        evaluation_cache_ttl: Optional[float] = None,
        evaluation_workers: Optional[int] = None,
        execution_lanes: Optional[int] = None,
        on_evaluation = None,
        on_execution = None,
    ) -> None:
        self.agent_manager = agent_manager
        self.portfolio = portfolio

        fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
        self.fast_mode = fast_mode

        interval_override: Optional[float] = None
        raw_interval = os.getenv("DISCOVERY_INTERVAL")
        if raw_interval:
            try:
                interval_override = float(raw_interval)
            except ValueError:
                log.warning("Invalid DISCOVERY_INTERVAL=%r; ignoring", raw_interval)
        base_interval = float(discovery_interval or 0.0)
        if base_interval <= 0:
            base_interval = 5.0
        soft_floor = float(os.getenv("DISCOVERY_MIN_INTERVAL", "5") or 5.0)
        if fast_mode:
            soft_floor = min(soft_floor, 0.5)
            if interval_override is None:
                base_interval = min(base_interval, 1.0)
        if interval_override is not None:
            base_interval = interval_override
        self.discovery_interval = max(base_interval, soft_floor)

        self.discovery_cache_ttl = max(
            discovery_cache_ttl if discovery_cache_ttl and discovery_cache_ttl > 0 else self.discovery_interval,
            self.discovery_interval,
        )
        if fast_mode:
            self.discovery_cache_ttl = max(self.discovery_interval, min(self.discovery_cache_ttl, self.discovery_interval * 2))
        self.scoring_batch = scoring_batch

        eval_override: Optional[float] = None
        raw_eval_ttl = os.getenv("EVALUATION_CACHE_TTL")
        if raw_eval_ttl:
            try:
                eval_override = float(raw_eval_ttl)
            except ValueError:
                log.warning("Invalid EVALUATION_CACHE_TTL=%r; ignoring", raw_eval_ttl)
        base_eval_ttl: float
        if eval_override is not None:
            base_eval_ttl = eval_override
        elif evaluation_cache_ttl is not None:
            base_eval_ttl = float(evaluation_cache_ttl)
        else:
            base_eval_ttl = 30.0
        eval_floor = float(os.getenv("EVALUATION_MIN_CACHE_TTL", "15") or 15.0)
        self.evaluation_cache_ttl = max(base_eval_ttl, eval_floor, 0.0)
        if fast_mode:
            self.evaluation_cache_ttl = max(0.5, min(self.evaluation_cache_ttl, 8.0))
        self.evaluation_workers = evaluation_workers
        self.execution_lanes = execution_lanes
        self.on_evaluation = on_evaluation
        self.on_execution = on_execution

        self._discovery_queue = cast(
            asyncio.Queue[list[TokenCandidate]],
            self._build_queue(
                "DISCOVERY_QUEUE_SIZE",
                default=64,
                item_type=list[TokenCandidate],
            ),
        )
        self._scoring_queue = cast(
            asyncio.Queue[ScoredToken],
            self._build_queue(
                "SCORING_QUEUE_SIZE",
                default=128,
                item_type=ScoredToken,
            ),
        )
        self._execution_queue = cast(
            asyncio.Queue[ActionBundle],
            self._build_queue(
                "EXECUTION_QUEUE_SIZE",
                default=128,
                item_type=ActionBundle,
            ),
        )
        self._portfolio_service = PortfolioManagementService(portfolio)

        self._discovery_service = DiscoveryService(
            self._discovery_queue,
            interval=self.discovery_interval,
            cache_ttl=self.discovery_cache_ttl,
            limit=int(os.getenv("DISCOVERY_LIMIT", "0") or 0) or None,
            emit_batch_size=1 if fast_mode else None,
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

    @staticmethod
    def _build_queue(env_key: str, *, default: int, item_type: Any) -> asyncio.Queue:
        """Initialise an asyncio.Queue with optional env override."""

        raw_value = os.getenv(env_key)
        size = default
        _ = item_type  # appease static type analysers without affecting runtime
        if raw_value:
            try:
                size = int(raw_value)
            except ValueError:
                log.warning("Invalid %s=%r; defaulting to %d", env_key, raw_value, default)
                size = default
        if size <= 0:
            return asyncio.Queue()
        return asyncio.Queue(maxsize=max(1, size))

    async def start(self) -> None:
        await self._scoring_service.start()
        await self._evaluation_service.start()
        await self._execution_service.start()
        await self._feedback_service.start()
        await self._portfolio_service.start()
        await self._discovery_service.start()
        log.info(
            "PipelineCoordinator: all services started (discovery→scoring→evaluation→execution→portfolio)"
        )

    async def stop(self) -> None:
        self._stopped.set()
        await self._execution_service.stop()
        await self._evaluation_service.stop()
        await self._scoring_service.stop()
        await self._discovery_service.stop()
        await self._feedback_service.stop()
        await self._portfolio_service.stop()
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
            "metadata": result.metadata,
        }
        await self._record_telemetry("evaluation", payload)
        await self._feedback_service.put(result)
        await self._portfolio_service.put(result)
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
            "results": receipt.results,
        }
        await self._record_telemetry("execution", payload)
        await self._feedback_service.put(receipt)
        await self._portfolio_service.put(receipt)
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

    def queue_snapshot(self) -> Dict[str, int]:
        return {
            "discovery_queue": self._discovery_queue.qsize(),
            "scoring_queue": self._scoring_queue.qsize(),
            "execution_queue": self._execution_queue.qsize(),
            "no_action_cache": len(self._no_action_cache),
        }

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
