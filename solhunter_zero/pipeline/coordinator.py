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
        offline: bool = False,
        token_file: Optional[str] = None,
        discovery_empty_cache_ttl: Optional[float] = None,
        discovery_backoff_factor: Optional[float] = None,
        discovery_max_backoff: Optional[float] = None,
        discovery_limit: Optional[int] = None,
        discovery_startup_clones: Optional[int] = None,
        testnet: Optional[bool] = None,
    ) -> None:
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        self.testnet = bool(testnet) if testnet is not None else None

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
        if interval_override is not None:
            base_interval = interval_override
        self.discovery_interval = max(base_interval, soft_floor)

        self.discovery_cache_ttl = max(
            discovery_cache_ttl if discovery_cache_ttl and discovery_cache_ttl > 0 else self.discovery_interval,
            self.discovery_interval,
        )
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
        self.evaluation_workers = evaluation_workers
        self.execution_lanes = execution_lanes
        self.on_evaluation = on_evaluation
        self.on_execution = on_execution

        self._discovery_queue: asyncio.Queue[list[TokenCandidate]] = asyncio.Queue(maxsize=4)
        self._scoring_queue: asyncio.Queue[ScoredToken] = asyncio.Queue(maxsize=64)
        self._execution_queue: asyncio.Queue[ActionBundle] = asyncio.Queue(maxsize=64)
        self._portfolio_service = PortfolioManagementService(portfolio)

        if testnet is not None:
            executor = getattr(self.agent_manager, "executor", None)
            if executor is not None:
                try:
                    setattr(executor, "testnet", bool(testnet))
                except Exception:  # pragma: no cover - defensive logging
                    log.exception("Failed to propagate testnet flag to executor")

        discovery_kwargs: Dict[str, Any] = {}
        if discovery_empty_cache_ttl is not None:
            try:
                discovery_kwargs["empty_cache_ttl"] = float(discovery_empty_cache_ttl)
            except (TypeError, ValueError):
                log.warning(
                    "Invalid discovery_empty_cache_ttl=%r; ignoring",
                    discovery_empty_cache_ttl,
                )
        if discovery_backoff_factor is not None:
            try:
                discovery_kwargs["backoff_factor"] = float(discovery_backoff_factor)
            except (TypeError, ValueError):
                log.warning(
                    "Invalid discovery_backoff_factor=%r; ignoring",
                    discovery_backoff_factor,
                )
        if discovery_max_backoff is not None:
            try:
                discovery_kwargs["max_backoff"] = float(discovery_max_backoff)
            except (TypeError, ValueError):
                log.warning(
                    "Invalid discovery_max_backoff=%r; ignoring",
                    discovery_max_backoff,
                )
        if discovery_startup_clones is not None:
            try:
                discovery_kwargs["startup_clones"] = int(discovery_startup_clones)
            except (TypeError, ValueError):
                log.warning(
                    "Invalid discovery_startup_clones=%r; ignoring",
                    discovery_startup_clones,
                )

        limit_override: Optional[int] = None
        if discovery_limit is not None:
            try:
                limit_override = int(discovery_limit)
            except (TypeError, ValueError):
                log.warning("Invalid discovery_limit=%r; ignoring", discovery_limit)
            else:
                if limit_override <= 0:
                    limit_override = None
        else:
            raw_limit = os.getenv("DISCOVERY_LIMIT")
            if raw_limit:
                try:
                    parsed_limit = int(raw_limit)
                except ValueError:
                    log.warning("Invalid DISCOVERY_LIMIT=%r; ignoring", raw_limit)
                else:
                    if parsed_limit > 0:
                        limit_override = parsed_limit

        if limit_override is not None:
            discovery_kwargs["limit"] = limit_override

        self.offline = bool(offline)
        self.token_file = str(token_file) if token_file else None

        self._discovery_service = DiscoveryService(
            self._discovery_queue,
            interval=self.discovery_interval,
            cache_ttl=self.discovery_cache_ttl,
            offline=self.offline,
            token_file=self.token_file,
            **discovery_kwargs,
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
            testnet=self.testnet,
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

    def discovery_snapshot(self) -> Dict[str, Any]:
        """Expose the discovery service fetch/backoff state."""

        try:
            return self._discovery_service.snapshot()
        except Exception:
            log.exception("Failed to snapshot discovery service state")
            return {}

    async def start(self) -> None:
        services = [
            ("discovery", self._discovery_service),
            ("scoring", self._scoring_service),
            ("evaluation", self._evaluation_service),
            ("execution", self._execution_service),
            ("feedback", self._feedback_service),
            ("portfolio", self._portfolio_service),
        ]
        started: list[tuple[str, Any]] = []
        failed_service = "initial"
        try:
            for name, service in services:
                failed_service = name
                started.append((name, service))
                await service.start()
        except Exception:
            log.exception(
                "PipelineCoordinator: failed to start %s service; rolling back",
                failed_service,
            )
            for name, service in reversed(started):
                try:
                    await service.stop()
                except Exception:
                    log.exception(
                        "PipelineCoordinator: error stopping %s service during rollback",
                        name,
                    )
            raise
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

    def refresh_discovery(self) -> None:
        """Clear cached discovery state so new settings take effect promptly."""

        try:
            self._discovery_service.refresh()
        except Exception:
            log.exception("PipelineCoordinator: failed to refresh discovery service")

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
