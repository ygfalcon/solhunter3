from __future__ import annotations

"""Swarm orchestration pipeline.

This module normalises the discovery→evaluation→simulation→execution loop into
structured stages so we can reason about the swarm as a whole.  The pipeline
builds on ``AgentManager``/``AgentSwarm`` primitives but adds richer telemetry,
consistent action normalisation, and explicit asset feedback to the portfolio.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional
import asyncio
import logging
import math
import os
import time

from .agents.discovery import DiscoveryAgent
from .agent_manager import AgentManager, EvaluationContext
from .event_bus import publish
from .schemas import ActionExecuted, RuntimeLog
from .simulation import SimulationResult, run_simulations_async
from .portfolio import Portfolio
from .lru import TTLCache
from .execution import EventExecutor
from .prices import fetch_token_prices_async, get_cached_price, update_price_cache

log = logging.getLogger(__name__)


@dataclass
class SwarmAction:
    """Normalised agent proposal enriched with simulation metadata."""

    token: str
    side: str
    amount: float
    price: float
    agent: str
    expected_roi: float = 0.0
    success_prob: float = 0.0
    conviction_delta: float | None = None
    regret: float | None = None
    misfires: float | None = None
    bias: float | None = None
    weight: float = 1.0
    rl_action: list[float] | None = None
    venues: list[str] | None = None
    priority: int | None = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_raw(cls, payload: Dict[str, Any]) -> "SwarmAction | None":
        token = str(payload.get("token") or "").strip()
        side = str(payload.get("side") or "").strip().lower()
        if not token or side not in {"buy", "sell"}:
            return None
        try:
            amount = float(payload.get("amount", 0.0))
        except Exception:
            amount = 0.0
        try:
            price = float(payload.get("price", 0.0))
        except Exception:
            price = 0.0
        agent = str(payload.get("agent") or payload.get("source") or "").strip()
        expected_roi = float(payload.get("expected_roi") or payload.get("roi") or 0.0)
        success_prob = float(
            payload.get("success_prob")
            or payload.get("probability")
            or payload.get("confidence")
            or 0.0
        )
        weight = float(payload.get("weight", 1.0))
        venues = None
        if isinstance(payload.get("venues"), list):
            venues = [str(v) for v in payload["venues"]]
        priority = None
        if "priority" in payload:
            try:
                priority = int(payload.get("priority"))
            except Exception:
                priority = None
        fields = {
            "conviction_delta": payload.get("conviction_delta"),
            "regret": payload.get("regret"),
            "misfires": payload.get("misfires"),
            "bias": payload.get("bias"),
        }
        rl_action = payload.get("rl_action") if isinstance(payload.get("rl_action"), list) else None
        metadata = {}
        for key, value in payload.items():
            if key in {
                "token",
                "side",
                "amount",
                "price",
                "agent",
                "expected_roi",
                "roi",
                "success_prob",
                "probability",
                "confidence",
                "weight",
                "conviction_delta",
                "regret",
                "misfires",
                "bias",
                "venues",
                "priority",
                "rl_action",
            }:
                continue
            metadata[key] = value
        return cls(
            token=token,
            side=side,
            amount=amount,
            price=price,
            agent=agent or "unknown",
            expected_roi=expected_roi,
            success_prob=success_prob,
            conviction_delta=fields["conviction_delta"],
            regret=fields["regret"],
            misfires=fields["misfires"],
            bias=fields["bias"],
            weight=weight,
            rl_action=rl_action,
            venues=venues,
            priority=priority,
            metadata=metadata,
        )

    @property
    def notional(self) -> float:
        return max(0.0, self.amount) * max(0.0, self.price)

    @property
    def score(self) -> float:
        base = max(self.expected_roi, 0.0) * max(self.success_prob, 0.0)
        if self.bias is not None:
            base *= 1.0 + float(self.bias)
        if self.conviction_delta is not None:
            base *= 1.0 + float(self.conviction_delta)
        return base * max(self.weight, 0.0)

    def apply_simulation(self, sim: SimulationResult) -> None:
        if self.expected_roi <= 0:
            self.expected_roi = float(sim.expected_roi)
        if self.success_prob <= 0:
            self.success_prob = float(sim.success_prob)
        if sim.order_book_strength:
            self.metadata.setdefault("order_book_strength", sim.order_book_strength)
        if sim.tx_rate:
            self.metadata.setdefault("tx_rate", sim.tx_rate)
        if sim.slippage:
            self.metadata.setdefault("slippage", sim.slippage)

    def to_order(self) -> Dict[str, Any]:
        order = {
            "token": self.token,
            "side": self.side,
            "amount": float(self.amount),
            "price": float(self.price),
            "agent": self.agent,
            "expected_roi": float(self.expected_roi),
            "success_prob": float(self.success_prob),
            "weight": float(self.weight),
        }
        if self.venues:
            order["venues"] = list(self.venues)
        if self.priority is not None:
            order["priority"] = int(self.priority)
        order.update(self.metadata)
        return order


@dataclass
class DiscoveryStage:
    tokens: List[str] = field(default_factory=list)
    discovered: List[str] = field(default_factory=list)
    fallback_used: bool = False
    scores: Dict[str, float] = field(default_factory=dict)
    limit: int = 0


@dataclass
class EvaluationRecord:
    token: str
    score: float
    latency: float
    actions: List[SwarmAction]
    context: EvaluationContext | None
    errors: List[str] = field(default_factory=list)


@dataclass
class EvaluationStage:
    records: List[EvaluationRecord] = field(default_factory=list)
    latencies: List[float] = field(default_factory=list)
    budget_hit: bool = False


@dataclass
class SimulationStage:
    records: List[EvaluationRecord] = field(default_factory=list)
    rejected: Dict[str, int] = field(default_factory=dict)


@dataclass
class ExecutionRecord:
    token: str
    action: SwarmAction
    result: Dict[str, Any]


@dataclass
class ExecutionStage:
    executed: List[ExecutionRecord] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    lane_metrics: Dict[str, int] = field(default_factory=dict)
    skipped: Dict[str, int] = field(default_factory=dict)


@dataclass
class AssetStage:
    updates: List[Dict[str, Any]] = field(default_factory=list)


class ExecutionRequest:
    """Container passed to the dispatcher workers."""

    __slots__ = ("token", "action", "record", "token_result", "summary_errors", "raw_results")

    def __init__(
        self,
        token: str,
        action: Dict[str, Any],
        record: Dict[str, Any],
        token_result: Dict[str, Any],
        summary_errors: List[str],
        raw_results: List[Any],
    ) -> None:
        self.token = token
        self.action = action
        self.record = record
        self.token_result = token_result
        self.summary_errors = summary_errors
        self.raw_results = raw_results


class ExecutionDispatcher:
    """Dispatch execution requests into venue-specific lanes."""

    def __init__(self, agent_manager: AgentManager, *, lane_workers: int = 1) -> None:
        self.agent_manager = agent_manager
        self._lane_workers = max(1, int(lane_workers))
        self._queues: Dict[str, asyncio.Queue] = {}
        self._tasks: Dict[str, List[asyncio.Task]] = {}
        self._closing = False

    def lane_snapshot(self) -> Dict[str, int]:
        return {lane: queue.qsize() for lane, queue in self._queues.items()}

    def _ensure_lane(self, lane: str) -> asyncio.Queue:
        if lane not in self._queues:
            queue: asyncio.Queue = asyncio.Queue()
            self._queues[lane] = queue
            self._tasks[lane] = [
                asyncio.create_task(
                    self._lane_worker(lane, queue),
                    name=f"execution_lane:{lane}:{idx}",
                )
                for idx in range(self._lane_workers)
            ]
        return self._queues[lane]

    async def submit(self, lane: str, request: ExecutionRequest) -> asyncio.Future:
        if self._closing:
            raise RuntimeError("execution dispatcher already closing")
        queue = self._ensure_lane(lane)
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        await queue.put((request, fut))
        return fut

    async def _lane_worker(
        self, lane: str, queue: asyncio.Queue
    ) -> None:  # pragma: no cover - async loop
        while True:
            payload = await queue.get()
            if payload is None:
                queue.task_done()
                break
            request, future = payload
            try:
                record = await self._execute_request(request)
                if not future.done():
                    future.set_result(record)
            except Exception as exc:
                if not future.done():
                    future.set_exception(exc)
            finally:
                queue.task_done()

    async def _execute_request(self, request: ExecutionRequest) -> Dict[str, Any]:
        action = request.action
        token = request.token
        try:
            result = await self.agent_manager.executor.execute(action)
            request.record["result"] = _stringify(result)
            metrics = _extract_realized_metrics(action, result)
            if metrics:
                request.record.update(metrics)
                action.update(metrics)
            if getattr(self.agent_manager, "emotion_agent", None):
                try:
                    emotion = self.agent_manager.emotion_agent.evaluate(action, result)
                    action["emotion"] = emotion
                except Exception:
                    pass
            if getattr(self.agent_manager, "memory_agent", None):
                try:
                    await self.agent_manager.memory_agent.log(action)
                except Exception:
                    pass
            publish("action_executed", ActionExecuted(action=action, result=result))
            request.raw_results.append(result)
        except Exception as exc:
            err = f"execute:{exc}"
            request.record["result"] = f"error: {exc}"
            request.token_result.setdefault("errors", []).append(err)
            request.summary_errors.append(f"{token}: {err}")
        return request.record

    async def close(self) -> None:
        self._closing = True
        sentinels = []
        for lane, queue in self._queues.items():
            for _ in self._tasks.get(lane, []):
                sentinels.append(queue.put(None))
        if sentinels:
            await asyncio.gather(*sentinels)
        tasks = [t for lanes in self._tasks.values() for t in lanes]
        for task in tasks:
            try:
                await task
            except Exception:
                pass
        self._queues.clear()
        self._tasks.clear()


def _stringify(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_stringify(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _stringify(v) for k, v in value.items()}
    return str(value)


def _extract_realized_metrics(
    action: Dict[str, Any], result: Any
) -> Dict[str, float | None]:
    """Return realized execution metrics derived from ``result``."""

    def _as_float(val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    def _lookup(container: Dict[str, Any], keys: tuple[str, ...]) -> Optional[float]:
        for key in keys:
            if key in container:
                value = _as_float(container[key])
                if value is not None:
                    return value
        data = container.get("data")
        if isinstance(data, dict):
            return _lookup(data, keys)
        return None

    def _iter_fills(container: Dict[str, Any]) -> Iterable[tuple[float, float]]:
        for key in ("fills", "orders", "executions", "trades"):
            entries = container.get(key)
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                price = _lookup(entry, (
                    "price",
                    "avg_price",
                    "average_price",
                    "execution_price",
                    "executionPrice",
                    "rate",
                    "limit_price",
                ))
                amount = _lookup(entry, (
                    "filled_amount",
                    "filledAmount",
                    "amount",
                    "qty",
                    "quantity",
                    "size",
                ))
                if price is not None and amount is not None:
                    yield price, amount
        data = container.get("data")
        if isinstance(data, dict):
            yield from _iter_fills(data)

    def _realized_roi(
        realized_price: Optional[float], realized_amount: Optional[float]
    ) -> Optional[float]:
        entry_price = _as_float(action.get("entry_price"))
        if entry_price in (None, 0.0):
            return None
        if realized_price is None:
            return None
        if realized_amount is None or realized_amount <= 0:
            return None
        side = str(action.get("side") or "").strip().lower()
        if side == "sell":
            return (realized_price - entry_price) / entry_price
        if side == "buy":
            return (entry_price - realized_price) / entry_price
        return None

    metrics: Dict[str, float | None] = {}
    if not isinstance(result, dict):
        metrics["realized_roi"] = _realized_roi(None, None)
        return metrics

    realized_amount: Optional[float] = None
    realized_notional: Optional[float] = None
    realized_price: Optional[float] = None

    fills = list(_iter_fills(result))
    if fills:
        total_amount = sum(amount for _, amount in fills)
        total_notional = sum(price * amount for price, amount in fills)
        if total_amount > 0:
            realized_amount = total_amount
            realized_notional = total_notional
            realized_price = total_notional / total_amount

    amount = _lookup(
        result,
        (
            "filled_amount",
            "filledAmount",
            "executed_amount",
            "executedAmount",
            "amount",
            "trade_amount",
            "tradeAmount",
            "qty",
            "quantity",
            "size",
            "realized_amount",
        ),
    )
    price = _lookup(
        result,
        (
            "execution_price",
            "executionPrice",
            "avg_price",
            "average_price",
            "price",
            "fill_price",
            "realized_price",
        ),
    )
    notional = _lookup(
        result,
        (
            "notional",
            "trade_value",
            "tradeValue",
            "value",
            "gross_notional",
            "realized_notional",
        ),
    )

    if realized_amount is None and amount is not None:
        realized_amount = amount
    if realized_notional is None and notional is not None:
        realized_notional = notional
    if realized_price is None and price is not None:
        realized_price = price

    if (
        realized_amount is not None
        and realized_price is not None
        and realized_notional is None
    ):
        realized_notional = realized_amount * realized_price
    elif (
        realized_notional is not None
        and realized_amount is not None
        and realized_price is None
        and realized_amount
    ):
        realized_price = realized_notional / realized_amount
    elif (
        realized_notional is not None
        and realized_price is not None
        and realized_amount is None
        and realized_price
    ):
        realized_amount = realized_notional / realized_price

    if realized_amount is not None:
        metrics["realized_amount"] = realized_amount
    if realized_notional is not None:
        metrics["realized_notional"] = realized_notional
    if realized_price is not None:
        metrics["realized_price"] = realized_price

    metrics["realized_roi"] = _realized_roi(realized_price, realized_amount)

    return metrics


def _iter_container_keys(container: Any) -> Iterable[str]:
    if container is None:
        return []
    if isinstance(container, dict):
        return (str(k) for k in container.keys())
    if isinstance(container, (set, list, tuple)):
        return (str(v) for v in container)
    keys = getattr(container, "keys", None)
    if callable(keys):
        try:
            return (str(k) for k in keys())
        except TypeError:
            try:
                return (str(k) for k in container.keys())
            except Exception:
                return []
    return []


def _available_executor_names(agent_manager: AgentManager) -> set[str]:
    names: set[str] = set()
    sources = [
        getattr(agent_manager, "available_executors", None),
        getattr(agent_manager, "executors", None),
        getattr(agent_manager, "_event_executors", None),
    ]
    executor = getattr(agent_manager, "executor", None)
    if executor is not None:
        sources.extend(
            [
                getattr(executor, "available_executors", None),
                getattr(executor, "executors", None),
                getattr(executor, "_executors", None),
            ]
        )
    for source in sources:
        for key in _iter_container_keys(source):
            names.add(key)
    return names


def _resolve_lane(action: Dict[str, Any]) -> str:
    venues = action.get("venues")
    if isinstance(venues, list) and venues:
        lane = str(venues[0]).strip().lower()
        if lane:
            return lane
    venue = action.get("venue")
    if isinstance(venue, str) and venue.strip():
        return venue.strip().lower()
    return "default"


def _score_token(token: str, portfolio: Portfolio) -> float:
    score = 0.0
    balances = getattr(portfolio, "balances", {}) or {}
    if token in balances:
        score += 10.0
    history = getattr(portfolio, "price_history", {}).get(token) or []
    if history:
        window = history[-min(len(history), 10) :]
        if len(window) >= 2:
            first = window[0] or 1.0
            last = window[-1]
            delta = (last - first) / (abs(first) or 1.0)
            score += delta * 5.0
            mean = sum(window) / len(window)
            var = sum((x - mean) ** 2 for x in window) / len(window)
            if mean:
                score += math.sqrt(var) / (abs(mean) or 1.0)
    score += os.urandom(2)[0] / 65535.0
    return score


def _score_priority(score: float) -> float:
    """Map the discovery score to a bounded priority multiplier."""

    try:
        value = float(score)
    except Exception:
        return 0.5
    if not math.isfinite(value):
        return 0.5
    try:
        logistic = 1.0 / (1.0 + math.exp(-value))
    except OverflowError:
        logistic = 1.0 if value > 0 else 0.0
    return min(1.0, max(0.25, logistic))


async def _ensure_depth_executor(agent_manager: AgentManager, token: str) -> None:
    if not getattr(agent_manager, "depth_service", False):
        return
    executors = getattr(agent_manager, "_event_executors", {})
    if token in executors:
        return
    execer = EventExecutor(
        token,
        priority_rpc=getattr(agent_manager.executor, "priority_rpc", None),
    )
    executors[token] = execer
    agent_manager.executor.add_executor(token, execer)
    tasks = getattr(agent_manager, "_event_tasks", {})
    tasks[token] = asyncio.create_task(execer.run(), name=f"depth_event:{token}")


class SwarmPipeline:
    """High-level orchestrator for discovery→evaluation→execution loop."""

    _no_action_cache: TTLCache[str, float] = TTLCache(maxsize=256, ttl=60.0)

    def __init__(
        self,
        agent_manager: AgentManager,
        portfolio: Portfolio,
        *,
        memory: Any = None,
        state: Any = None,
        dry_run: bool = False,
        testnet: bool = False,
        offline: bool = False,
        token_file: str | None = None,
        discovery_method: str | None = None,
        stop_loss: float | None = None,
        take_profit: float | None = None,
        trailing_stop: float | None = None,
        max_drawdown: float = 1.0,
        volatility_factor: float = 1.0,
    ) -> None:
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        self.memory = memory
        self.state = state
        self.dry_run = bool(dry_run)
        self.testnet = bool(testnet)
        self.offline = bool(offline)
        self.token_file = token_file
        self.discovery_method = discovery_method
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.trailing_stop = trailing_stop
        self.max_drawdown = max_drawdown
        self.volatility_factor = volatility_factor
        self._skip_simulation = bool(getattr(agent_manager, "skip_simulation", False))

        self.fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
        self.time_budget = self._get_float_env("ITERATION_TIME_BUDGET", default=0.0)
        if not self.time_budget and self.fast_mode:
            self.time_budget = 30.0
        self.chunk_size = self._determine_chunk_size()
        self.lane_workers = max(1, int(os.getenv("EXECUTION_LANE_WORKERS", "2") or 2))
        self.token_timeout = self._get_float_env("TOKEN_EVAL_TIMEOUT", default=0.0)
        if not self.token_timeout and self.time_budget and self.fast_mode:
            self.token_timeout = max(5.0, self.time_budget / 8)
        self.no_action_ttl = self._get_float_env("NO_ACTION_CACHE_TTL", default=(10.0 if self.fast_mode else 0.0))
        if self.no_action_ttl:
            self._no_action_cache.ttl = float(self.no_action_ttl)
        cache_ttl_default = 60.0 if self.fast_mode else 45.0
        self.discovery_cache_ttl = self._get_float_env(
            "SWARM_DISCOVERY_CACHE_TTL", default=cache_ttl_default
        )
        cache_limit_env = os.getenv("SWARM_DISCOVERY_CACHE_LIMIT")
        try:
            cache_limit = int(cache_limit_env) if cache_limit_env else 0
        except Exception:
            cache_limit = 0
        if cache_limit <= 0:
            cache_limit = max(self.chunk_size * 8, 96)
        self.discovery_cache_limit = max(1, cache_limit)
        self._discovery_cache_tokens: list[str] = []
        self._discovery_cache_scores: dict[str, float] = {}
        self._discovery_cache_expiry: float = 0.0
        self._discovery_agent: DiscoveryAgent | None = None
        self.min_expected_roi = self._get_float_env("SWARM_MIN_EXPECTED_ROI", default=0.0)
        self.min_success_prob = self._get_float_env("SWARM_MIN_SUCCESS_PROB", default=0.0)
        self.min_score = self._get_float_env("SWARM_MIN_SCORE", default=0.0)
        self.max_actions_per_token = int(os.getenv("SWARM_MAX_ACTIONS", "0") or 0)
        self.simulation_batch = max(1, int(os.getenv("SWARM_SIMULATION_COUNT", "40") or 40))
        self.simulation_timeout = self._get_float_env("SWARM_SIMULATION_TIMEOUT", default=15.0)
        self._iteration_start: float | None = None

    @staticmethod
    def _get_float_env(name: str, *, default: float = 0.0) -> float:
        raw = os.getenv(name)
        if raw is None or raw == "":
            return float(default)
        try:
            return float(raw)
        except Exception:
            return float(default)

    def _determine_chunk_size(self) -> int:
        override = os.getenv("DISCOVERY_CHUNK_SIZE")
        if override:
            try:
                return max(1, int(override))
            except Exception:
                pass
        if self.fast_mode:
            cores = os.cpu_count() or 8
            return min(8, max(4, cores // 4))
        return 3

    def _remaining_budget(self) -> float | None:
        if not self.time_budget or self._iteration_start is None:
            return None
        elapsed = time.perf_counter() - self._iteration_start
        remaining = self.time_budget - elapsed
        return remaining if remaining > 0 else 0.0

    def _ensure_discovery_agent(self) -> DiscoveryAgent:
        if self._discovery_agent is None:
            self._discovery_agent = DiscoveryAgent()
        return self._discovery_agent

    def _update_discovery_cache(
        self,
        tokens: Iterable[str],
        *,
        scores: Optional[Dict[str, float]] = None,
        ttl: Optional[float] = None,
        refresh_ttl: bool = True,
        now: Optional[float] = None,
    ) -> None:
        if self.discovery_cache_limit <= 0:
            return

        seen: set[str] = set()
        ordered: list[str] = []
        for token in tokens:
            if not isinstance(token, str):
                continue
            candidate = token.strip()
            if not candidate or candidate in seen:
                continue
            ordered.append(candidate)
            seen.add(candidate)

        if not ordered:
            return

        for token in self._discovery_cache_tokens:
            if token not in seen:
                ordered.append(token)
                seen.add(token)
            if len(ordered) >= self.discovery_cache_limit:
                break

        if len(ordered) > self.discovery_cache_limit:
            ordered = ordered[: self.discovery_cache_limit]

        merged_scores = dict(self._discovery_cache_scores)
        if scores:
            for tok, value in scores.items():
                if isinstance(value, (int, float)) and math.isfinite(float(value)):
                    merged_scores[tok] = float(value)

        for tok in ordered:
            if tok not in merged_scores:
                merged_scores[tok] = _score_token(tok, self.portfolio)

        self._discovery_cache_tokens = ordered
        self._discovery_cache_scores = {tok: merged_scores[tok] for tok in ordered}

        if refresh_ttl:
            ttl_value = self.discovery_cache_ttl if ttl is None else float(ttl)
            if ttl_value and ttl_value > 0:
                ts = time.time() if now is None else float(now)
                self._discovery_cache_expiry = ts + ttl_value
            else:
                self._discovery_cache_expiry = 0.0

    async def _ensure_action_price(self, action: SwarmAction) -> None:
        """Ensure ``action`` carries a positive price using the shared cache."""

        if action.price and action.price > 0 and math.isfinite(action.price):
            return
        token = action.token
        if not token:
            return

        cached = get_cached_price(token)
        if isinstance(cached, (int, float)):
            cached_val = float(cached)
            if cached_val > 0 and math.isfinite(cached_val):
                action.price = cached_val
                return

        try:
            prices = await fetch_token_prices_async([token])
        except Exception as exc:  # pragma: no cover - network/IO failures
            log.debug("price fetch failed for %s: %s", token, exc)
            return

        price = prices.get(token)
        if not isinstance(price, (int, float)):
            return

        value = float(price)
        if value <= 0 or not math.isfinite(value):
            return

        action.price = value
        try:
            update_price_cache(token, value)
        except Exception:  # pragma: no cover - cache errors shouldn't break pipeline
            pass

    async def run(self) -> Dict[str, Any]:
        start_ts = time.perf_counter()
        self._iteration_start = start_ts
        publish("runtime.log", RuntimeLog(stage="loop", detail="begin"))

        try:
            discovery = await self._run_discovery()
            if not discovery.tokens:
                elapsed = time.perf_counter() - start_ts
                publish("runtime.log", RuntimeLog(stage="loop", detail=f"end:{elapsed:.3f}s"))
                return {
                    "elapsed_s": float(elapsed),
                    "discovered_count": len(discovery.discovered),
                    "tokens_discovered": discovery.discovered,
                    "tokens_used": [],
                    "picked_tokens": [],
                    "fallback_used": discovery.fallback_used,
                    "actions_executed": [],
                    "actions_count": 0,
                    "any_trade": False,
                    "errors": ["discovery:empty"],
                    "chunk_size": self.chunk_size,
                    "token_results": [],
                    "telemetry": {
                        "discovery": {
                            "discovered": len(discovery.discovered),
                            "scored": 0,
                            "limit": discovery.limit,
                        }
                    },
                }

            evaluation = await self._run_evaluation(discovery)
            if discovery.tokens and not evaluation.records and evaluation.budget_hit:
                simulation = SimulationStage(records=[])
                execution = ExecutionStage()
                assets = AssetStage()
            else:
                simulation = await self._run_simulation(evaluation)
                if self._remaining_budget() == 0.0:
                    execution = ExecutionStage()
                    assets = AssetStage()
                else:
                    execution = await self._run_execution(simulation)
                    assets = await self._run_asset_stage(execution)

            elapsed = time.perf_counter() - start_ts
            publish("runtime.log", RuntimeLog(stage="loop", detail=f"end:{elapsed:.3f}s"))

            executed_actions = [rec.result for rec in execution.executed]
            any_trade = any(
                rec.get("result")
                and not str(rec.get("result")).startswith("error")
                and rec.get("result") != "dry-run"
                for rec in executed_actions
            )

            telemetry = {
                "discovery": {
                    "discovered": len(discovery.discovered),
                    "scored": len(discovery.tokens),
                    "limit": discovery.limit,
                },
                "evaluation": {
                    "workers": self.chunk_size,
                    "latency_avg": (sum(evaluation.latencies) / len(evaluation.latencies))
                    if evaluation.latencies
                    else 0.0,
                    "latency_max": max(evaluation.latencies) if evaluation.latencies else 0.0,
                    "completed": len(evaluation.records),
                    "budget_hit": evaluation.budget_hit,
                    "token_timeout": self.token_timeout or None,
                },
                "execution": {
                    "lanes": execution.lane_metrics,
                    "submitted": len(execution.executed),
                    "lane_workers": self.lane_workers,
                    "errors": len(execution.errors),
                    "skipped": execution.skipped,
                },
                "pipeline": {
                    "queued": len(discovery.tokens),
                    "discovered": len(discovery.discovered),
                    "limit": discovery.limit,
                    "processed": len(simulation.records),
                    "budget": self.time_budget or None,
                },
            }

            summary_errors = list(execution.errors)
            for rec in simulation.records:
                for err in rec.errors:
                    if err not in summary_errors:
                        summary_errors.append(err)
            summary = {
                "elapsed_s": float(elapsed),
                "discovered_count": len(discovery.discovered),
                "tokens_discovered": discovery.discovered,
                "tokens_used": list(discovery.tokens),
                "picked_tokens": [rec.token for rec in simulation.records],
                "fallback_used": discovery.fallback_used,
                "actions_executed": executed_actions,
                "actions_count": len(executed_actions),
                "any_trade": any_trade,
                "errors": summary_errors,
                "chunk_size": self.chunk_size,
                "token_results": [
                    {
                        "token": rec.token,
                        "score": rec.score,
                        "actions": [
                            {
                                "token": act.token,
                                "side": act.side,
                                "amount": act.amount,
                                "price": act.price,
                                "agent": act.agent,
                                "score": act.score,
                                "expected_roi": act.expected_roi,
                                "success_prob": act.success_prob,
                                "evaluation_score": rec.score,
                            }
                            for act in rec.actions
                        ],
                        "errors": rec.errors,
                    }
                    for rec in simulation.records
                ],
                "telemetry": telemetry,
                "asset_updates": assets.updates,
            }
            return summary
        finally:
            self._iteration_start = None

    async def _run_discovery(self) -> DiscoveryStage:
        stage = DiscoveryStage()
        now = time.time()
        cached_tokens = list(self._discovery_cache_tokens)
        cache_hit = False
        cache_refreshed = False

        if (
            self.discovery_cache_ttl > 0
            and cached_tokens
            and now < self._discovery_cache_expiry
        ):
            stage.discovered = list(cached_tokens)
            cache_hit = True
            publish(
                "runtime.log",
                RuntimeLog(
                    stage="discovery",
                    detail=f"cache-hit:{len(stage.discovered)}",
                ),
            )
        else:
            tokens: list[str] = []
            try:
                disc = self._ensure_discovery_agent()
                tokens = await disc.discover_tokens(
                    offline=self.offline,
                    token_file=self.token_file,
                    method=self.discovery_method,
                )
            except Exception as exc:
                publish("runtime.log", RuntimeLog(stage="discovery", detail=f"error:{exc}"))
                log.exception("Discovery failed")
                tokens = []

            if tokens:
                stage.discovered = list(tokens)
                self._update_discovery_cache(tokens, refresh_ttl=True, now=now)
                cache_refreshed = True
                cached_tokens = list(self._discovery_cache_tokens)
            elif cached_tokens:
                stage.discovered = list(cached_tokens)
            else:
                stage.discovered = []

        if not stage.discovered and hasattr(self.state, "last_tokens"):
            try:
                last_tokens = list(getattr(self.state, "last_tokens", []) or [])
            except Exception:
                last_tokens = []
            if last_tokens:
                stage.discovered = [
                    str(tok) for tok in last_tokens if isinstance(tok, str) and tok
                ]

        if not stage.discovered:
            stage.tokens = [
                "So11111111111111111111111111111111111111112",
                "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
                "JUP4Fb2cqiRUcaTHdrPC8G4wEGGkZwyTDt1v",
            ]
            stage.fallback_used = True
            publish("runtime.log", RuntimeLog(stage="discovery", detail="fallback"))
        else:
            stage.tokens = list(stage.discovered)

        if cached_tokens and stage.tokens:
            for tok in cached_tokens:
                if tok not in stage.tokens:
                    stage.tokens.append(tok)
                    if 0 < self.discovery_cache_limit <= len(stage.tokens):
                        break

        if getattr(self.portfolio, "balances", None):
            for tok in getattr(self.portfolio, "balances").keys():
                if tok not in stage.tokens:
                    stage.tokens.append(tok)
                    if 0 < self.discovery_cache_limit <= len(stage.tokens):
                        break

        limit = max(self.chunk_size * 4, self.chunk_size)
        fast_limit = os.getenv("PIPELINE_TOKEN_LIMIT")
        if fast_limit:
            try:
                configured = max(1, int(fast_limit))
                limit = min(limit, configured)
            except Exception:
                pass
        elif self.fast_mode:
            default_cap = max(4, self.chunk_size * 2)
            limit = min(limit, default_cap)
        if self.time_budget:
            limit = min(limit, max(4, int(self.time_budget / 4)))
        if self.discovery_cache_limit:
            limit = min(limit, self.discovery_cache_limit)
        stage.limit = limit

        deduped: list[str] = []
        seen_tokens: set[str] = set()
        for tok in stage.tokens:
            if tok not in seen_tokens:
                deduped.append(tok)
                seen_tokens.add(tok)
        stage.tokens = deduped

        scores = {mint: _score_token(mint, self.portfolio) for mint in stage.tokens}
        stage.scores = scores
        ranked = sorted(stage.tokens, key=lambda t: scores.get(t, 0.0), reverse=True)
        stage.tokens = ranked[:limit]
        publish(
            "runtime.log",
            RuntimeLog(stage="pipeline", detail=f"queue:{len(stage.tokens)}/{len(scores)}"),
        )
        refresh_ttl = not cache_hit and not cache_refreshed
        ttl_override = None
        if refresh_ttl and not stage.discovered:
            ttl_override = min(5.0, self.discovery_cache_ttl or 5.0)
        self._update_discovery_cache(
            stage.tokens,
            scores=stage.scores,
            refresh_ttl=refresh_ttl,
            ttl=ttl_override,
        )
        if hasattr(self.state, "last_tokens"):
            try:
                self.state.last_tokens = list(stage.tokens)
            except Exception:
                self.state.last_tokens = list(stage.tokens)
        return stage

    async def _run_evaluation(self, discovery: DiscoveryStage) -> EvaluationStage:
        stage = EvaluationStage()
        if not discovery.tokens:
            return stage

        start_time = time.perf_counter()
        sem = asyncio.Semaphore(self.chunk_size)
        records: List[EvaluationRecord] = []
        errors: List[str] = []

        async def evaluate_token(token: str, score: float) -> None:
            nonlocal start_time
            remaining_before = self._remaining_budget()
            if remaining_before is not None and remaining_before <= 0:
                stage.budget_hit = True
                return
            async with sem:
                remaining_total = self._remaining_budget()
                if remaining_total is not None and remaining_total <= 0:
                    stage.budget_hit = True
                    return
                if self.time_budget and time.perf_counter() - start_time > self.time_budget:
                    stage.budget_hit = True
                    return
                cached_expiry = self._no_action_cache.get(token) if self.no_action_ttl else None
                if cached_expiry is not None:
                    publish(
                        "runtime.log",
                        RuntimeLog(stage="evaluation", detail=f"cached-skip:{token}"),
                    )
                    records.append(
                        EvaluationRecord(
                            token=token,
                            score=score,
                            latency=0.0,
                            actions=[],
                            context=None,
                            errors=["cached:no-action"],
                        )
                    )
                    return
                publish(
                    "runtime.log",
                    RuntimeLog(stage="evaluation", detail=f"start:{token} score={score:.3f}"),
                )
                t0 = time.perf_counter()
                ctx: EvaluationContext | None = None
                actions: List[SwarmAction] = []
                token_errors: List[str] = []
                try:
                    eval_coro = self.agent_manager.evaluate_with_swarm(token, self.portfolio)
                    eval_timeout = self.token_timeout
                    if remaining_total is not None:
                        budget_slice = max(0.1, remaining_total * 0.4)
                        if eval_timeout > 0:
                            eval_timeout = min(eval_timeout, budget_slice)
                        else:
                            eval_timeout = budget_slice
                    if eval_timeout and eval_timeout > 0:
                        ctx = await asyncio.wait_for(eval_coro, timeout=eval_timeout)
                    else:
                        ctx = await eval_coro
                    raw_actions = list(ctx.actions)
                    for raw in raw_actions:
                        if not isinstance(raw, dict):
                            continue
                        act = SwarmAction.from_raw(raw)
                        if act is not None:
                            actions.append(act)
                    for act in actions:
                        await self._ensure_action_price(act)
                    if not actions and self.no_action_ttl:
                        self._no_action_cache[token] = time.time() + self.no_action_ttl
                except asyncio.TimeoutError:
                    token_errors.append("evaluation:timeout")
                    publish(
                        "runtime.log",
                        RuntimeLog(stage="evaluation", detail=f"timeout:{token}"),
                    )
                except Exception as exc:
                    token_errors.append(f"evaluation:{exc}")
                    publish(
                        "runtime.log",
                        RuntimeLog(stage="evaluation", detail=f"error:{token}:{exc}"),
                    )
                    log.exception("Evaluation failed for %s", token)
                latency = time.perf_counter() - t0
                stage.latencies.append(latency)
                publish(
                    "runtime.log",
                    RuntimeLog(stage="evaluation", detail=f"done:{token} actions={len(actions)} latency={latency:.2f}s"),
                )
                records.append(
                    EvaluationRecord(
                        token=token,
                        score=score,
                        latency=latency,
                        actions=actions,
                        context=ctx,
                        errors=token_errors,
                    )
                )
                errors.extend(token_errors)

        await asyncio.gather(*(evaluate_token(tok, discovery.scores.get(tok, 0.0)) for tok in discovery.tokens))
        stage.records = records
        if errors:
            publish(
                "runtime.log",
                RuntimeLog(stage="evaluation", detail=f"errors:{len(errors)}"),
            )
        return stage

    async def _run_simulation(self, evaluation: EvaluationStage) -> SimulationStage:
        stage = SimulationStage(records=list(evaluation.records))
        if not evaluation.records:
            return stage

        min_roi = self.min_expected_roi
        min_prob = self.min_success_prob
        min_score = self.min_score
        max_actions = self.max_actions_per_token

        async def simulate(record: EvaluationRecord) -> None:
            actions = record.actions
            if not actions:
                return
            remaining_sim = self._remaining_budget()
            if remaining_sim is not None and remaining_sim <= 0:
                stage.rejected[record.token] = len(actions)
                record.actions = []
                return
            if self._skip_simulation:
                return
            need_sim = any(act.expected_roi <= 0 or act.success_prob <= 0 for act in actions)
            sims: List[SimulationResult] = []
            if need_sim:
                publish(
                    "runtime.log",
                    RuntimeLog(stage="simulation", detail=f"start:{record.token} actions={len(actions)}"),
                )
                try:
                    sim_timeout = self.simulation_timeout
                    if remaining_sim is not None:
                        budget_slice = max(0.1, remaining_sim * 0.6)
                        if sim_timeout > 0:
                            sim_timeout = min(sim_timeout, budget_slice)
                        else:
                            sim_timeout = budget_slice
                    if sim_timeout and sim_timeout > 0:
                        timeout_val = sim_timeout
                    else:
                        timeout_val = None
                    if timeout_val is not None:
                        sims = await asyncio.wait_for(
                            run_simulations_async(
                                record.token,
                                count=self.simulation_batch,
                            ),
                            timeout=timeout_val,
                        )
                    else:
                        sims = await run_simulations_async(
                            record.token,
                            count=self.simulation_batch,
                        )
                except asyncio.TimeoutError:
                    publish(
                        "runtime.log",
                        RuntimeLog(stage="simulation", detail=f"timeout:{record.token}"),
                    )
                except Exception as exc:
                    publish(
                        "runtime.log",
                        RuntimeLog(stage="simulation", detail=f"error:{record.token}:{exc}"),
                    )
                    log.exception("Simulation failed for %s", record.token)
            avg_roi = sum(sim.expected_roi for sim in sims) / len(sims) if sims else 0.0
            avg_prob = sum(sim.success_prob for sim in sims) / len(sims) if sims else 0.0
            for act in actions:
                for sim in sims:
                    act.apply_simulation(sim)
                if act.expected_roi <= 0 and avg_roi > 0:
                    act.expected_roi = avg_roi
                if act.success_prob <= 0 and avg_prob > 0:
                    act.success_prob = avg_prob
                act.metadata.setdefault("simulation_avg_roi", avg_roi)
                act.metadata.setdefault("simulation_avg_success", avg_prob)
                act.metadata.setdefault("evaluation_score", record.score)
            filtered: List[SwarmAction] = []
            for act in actions:
                if min_roi and act.expected_roi < min_roi:
                    continue
                if min_prob and act.success_prob < min_prob:
                    continue
                if min_score and act.score < min_score:
                    continue
                filtered.append(act)
            if max_actions and len(filtered) > max_actions:
                filtered.sort(key=lambda a: a.score, reverse=True)
                priority = _score_priority(record.score)
                allowed = int(math.ceil(max_actions * priority))
                allowed = max(1, min(len(filtered), allowed))
                rejected = len(filtered) - allowed
                if rejected > 0:
                    stage.rejected[record.token] = rejected
                    filtered = filtered[:allowed]
                else:
                    stage.rejected.pop(record.token, None)
            record.actions = filtered

        await asyncio.gather(*(simulate(rec) for rec in evaluation.records))
        return stage

    async def _run_execution(self, simulation: SimulationStage) -> ExecutionStage:
        stage = ExecutionStage()
        if not simulation.records:
            return stage

        dispatcher = ExecutionDispatcher(self.agent_manager, lane_workers=self.lane_workers)
        futures: List[asyncio.Future] = []
        raw_results_by_token: Dict[str, List[Any]] = {}

        for record in simulation.records:
            actions = record.actions
            token_payload = {
                "token": record.token,
                "actions": [],
                "errors": record.errors,
                "score": record.score,
            }
            raw_results_by_token[record.token] = []
            if not self.dry_run:
                await _ensure_depth_executor(self.agent_manager, record.token)
            for act in actions:
                order = act.to_order()
                side = str(order.get("side") or "").strip().lower()
                price_value_raw = order.get("price", 0.0)
                try:
                    price_value = float(price_value_raw)
                except Exception:
                    price_value = 0.0
                balances = getattr(self.portfolio, "balances", {}) or {}
                entry_price: float | None = None
                if (
                    self.portfolio
                    and side in {"buy", "sell"}
                ):
                    position = None
                    token_key = str(order.get("token") or act.token or "")
                    if token_key and isinstance(balances, dict):
                        position = balances.get(token_key)
                    if position is not None:
                        raw_entry = getattr(position, "entry_price", None)
                        if raw_entry is None and isinstance(position, dict):
                            raw_entry = position.get("entry_price")
                        try:
                            value = float(raw_entry)
                        except (TypeError, ValueError):
                            value = None
                        if value is not None and math.isfinite(value) and value > 0:
                            if side == "sell" or (side == "buy" and position is not None):
                                entry_price = value
                if entry_price is not None and "entry_price" not in order:
                    order["entry_price"] = entry_price
                risk = dict(order.get("risk", {}))
                if self.stop_loss is not None:
                    risk["stop_loss"] = float(self.stop_loss)
                if self.take_profit is not None:
                    risk["take_profit"] = float(self.take_profit)
                if self.trailing_stop is not None:
                    risk["trailing_stop"] = float(self.trailing_stop)
                if self.max_drawdown is not None:
                    risk.setdefault("max_drawdown", float(self.max_drawdown))
                if self.volatility_factor is not None:
                    risk.setdefault("volatility_factor", float(self.volatility_factor))
                current_drawdown_metric: float | None = None
                if (
                    self.portfolio is not None
                    and price_value > 0
                    and isinstance(balances, dict)
                ):
                    try:
                        snapshot: Dict[str, float] = {}
                        for tok, pos in balances.items():
                            base_price = getattr(pos, "entry_price", None)
                            if base_price is None and isinstance(pos, dict):
                                base_price = pos.get("entry_price")
                            try:
                                snapshot[tok] = float(base_price) if base_price is not None else 0.0
                            except (TypeError, ValueError):
                                snapshot[tok] = 0.0
                        snapshot[record.token] = price_value
                        self.portfolio.update_drawdown(snapshot)
                        current_drawdown_metric = self.portfolio.current_drawdown(snapshot)
                    except Exception:
                        current_drawdown_metric = None
                if current_drawdown_metric is not None:
                    try:
                        risk.setdefault("current_drawdown", float(current_drawdown_metric))
                    except (TypeError, ValueError):
                        pass
                if risk:
                    order["risk"] = risk
                else:
                    order.pop("risk", None)

                if price_value <= 0 or not math.isfinite(price_value):
                    if "missing_price" not in token_payload["errors"]:
                        token_payload["errors"].append("missing_price")
                    if "execution:missing_price" not in stage.errors:
                        stage.errors.append("execution:missing_price")
                    stage.skipped["missing_price"] = stage.skipped.get("missing_price", 0) + 1
                    log.warning(
                        "Skipping execution for %s due to missing price (value=%s)",
                        record.token,
                        order.get("price"),
                    )
                    continue

                lane = _resolve_lane(order)

                def _skip(reason: str, stage_reason: str, message: str) -> None:
                    if reason not in token_payload["errors"]:
                        token_payload["errors"].append(reason)
                    if stage_reason not in stage.errors:
                        stage.errors.append(stage_reason)
                    stage.skipped[reason] = stage.skipped.get(reason, 0) + 1
                    log.warning(message)

                requires_keypair = bool(
                    order.get("requires_keypair")
                    or order.get("require_keypair")
                    or order.get("needs_keypair")
                )
                if requires_keypair:
                    has_keypair = bool(order.get("keypair"))
                    if not has_keypair:
                        exec_keypair = getattr(self.agent_manager.executor, "keypair", None)
                        manager_keypair = getattr(self.agent_manager, "keypair", None)
                        keypair_path = getattr(self.agent_manager, "keypair_path", None)
                        has_keypair = bool(exec_keypair or manager_keypair or keypair_path)
                    if not has_keypair:
                        _skip(
                            "missing_keypair",
                            "execution:missing_keypair",
                            f"Skipping execution for {record.token} due to missing keypair",
                        )
                        continue

                lane_budget_spec = order.get("lane_budget")
                lane_budget_key: str | None = None
                required_budget: float | None = None
                if isinstance(lane_budget_spec, dict):
                    value = lane_budget_spec.get(lane)
                    if value is not None:
                        lane_budget_key = lane
                        try:
                            required_budget = float(value)
                        except Exception:
                            required_budget = None
                elif isinstance(lane_budget_spec, (int, float)):
                    required_budget = float(lane_budget_spec)
                    lane_budget_key = lane
                elif isinstance(lane_budget_spec, str) and lane_budget_spec.strip():
                    lane_budget_key = lane_budget_spec.strip()

                extra_budget = order.get("lane_budgets")
                if isinstance(extra_budget, dict):
                    value = extra_budget.get(lane)
                    if value is not None:
                        try:
                            required_budget = float(value)
                        except Exception:
                            required_budget = None
                        if lane_budget_key is None:
                            lane_budget_key = lane

                if lane_budget_key is None:
                    lane_budget_key = (
                        order.get("lane_budget_key")
                        or order.get("lane_budget_name")
                        or order.get("required_lane_budget")
                    )
                    if isinstance(lane_budget_key, str) and lane_budget_key.strip():
                        lane_budget_key = lane_budget_key.strip()
                    else:
                        lane_budget_key = None

                if required_budget is None:
                    raw_required = (
                        order.get("lane_budget_amount")
                        or order.get("lane_budget_required")
                        or order.get("required_lane_budget_amount")
                    )
                    if isinstance(raw_required, dict):
                        raw_required = raw_required.get(lane)
                    if raw_required is not None:
                        try:
                            required_budget = float(raw_required)
                        except Exception:
                            required_budget = None
                        if lane_budget_key is None:
                            lane_budget_key = lane

                if required_budget is not None and required_budget <= 0:
                    required_budget = None

                requires_lane_budget = bool(
                    order.get("requires_lane_budget")
                    or lane_budget_key is not None
                    or required_budget is not None
                )
                if requires_lane_budget:
                    budgets = getattr(self.agent_manager.executor, "lane_budgets", None)
                    available_budget = None
                    if budgets:
                        keys_to_try = []
                        if lane_budget_key:
                            keys_to_try.append(str(lane_budget_key))
                        keys_to_try.append(str(lane))
                        for key in keys_to_try:
                            if key in budgets:
                                available_budget = budgets.get(key)
                                break
                    missing_budget = False
                    if available_budget is None:
                        missing_budget = True
                    elif required_budget is not None:
                        try:
                            missing_budget = float(available_budget or 0.0) < float(required_budget)
                        except Exception:
                            missing_budget = not bool(available_budget)
                    else:
                        missing_budget = not bool(available_budget)
                    if missing_budget:
                        budget_name = lane_budget_key or lane
                        _skip(
                            "missing_lane_budget",
                            "execution:missing_lane_budget",
                            f"Skipping execution for {record.token} due to missing lane budget {budget_name}",
                        )
                        continue

                required_execs = order.get("requires_executor") or order.get("requires_executors")
                if isinstance(required_execs, str):
                    required_execs = [required_execs]
                elif isinstance(required_execs, Iterable):
                    required_execs = [str(item) for item in required_execs if item]
                else:
                    required_execs = []

                if required_execs:
                    available_execs = _available_executor_names(self.agent_manager)
                    missing = [name for name in required_execs if name not in available_execs]
                    if missing:
                        _skip(
                            "missing_executor",
                            "execution:missing_executor",
                            f"Skipping execution for {record.token} due to missing executors: {', '.join(missing)}",
                        )
                        continue

                token_payload["actions"].append(order)
                record_entry = {
                    "token": act.token,
                    "side": act.side,
                    "amount": act.amount,
                    "price": act.price,
                    "agent": act.agent,
                    "expected_roi": act.expected_roi,
                    "success_prob": act.success_prob,
                    "evaluation_score": record.score,
                }
                if "entry_price" in order:
                    record_entry["entry_price"] = order["entry_price"]
                if risk:
                    record_entry["risk"] = risk
                if self.dry_run:
                    record_entry["result"] = "dry-run"
                req = ExecutionRequest(
                    record.token,
                    order,
                    record_entry,
                    token_payload,
                    stage.errors,
                    raw_results_by_token[record.token],
                )
                fut = await dispatcher.submit(lane, req)
                futures.append(fut)
                stage.executed.append(
                    ExecutionRecord(
                        token=record.token,
                        action=act,
                        result=record_entry,
                    )
                )

        if futures:
            remaining_exec = self._remaining_budget()
            gather_coro = asyncio.gather(*futures, return_exceptions=True)
            try:
                if remaining_exec is not None:
                    timeout_val = max(0.1, remaining_exec)
                    results = await asyncio.wait_for(gather_coro, timeout=timeout_val)
                else:
                    results = await gather_coro
            except asyncio.TimeoutError:
                stage.errors.append("execution:timeout")
                for fut in futures:
                    fut.cancel()
                results = []
            for res in results:
                if isinstance(res, Exception):
                    stage.errors.append(str(res))
        stage.lane_metrics = dispatcher.lane_snapshot()
        await dispatcher.close()

        # Feed execution feedback back into swarms
        for record in simulation.records:
            ctx = record.context
            if ctx is None:
                continue
            swarm = self.agent_manager.consume_swarm(
                record.token,
                ctx.swarm if isinstance(ctx, EvaluationContext) else None,
            )
            if swarm is None:
                continue
            try:
                swarm.record_results(raw_results_by_token.get(record.token, []))
            except Exception:
                pass
        return stage

    async def _run_asset_stage(self, execution: ExecutionStage) -> AssetStage:
        stage = AssetStage()
        if self.dry_run or not execution.executed or not self.portfolio:
            return stage
        prices: Dict[str, float] = {}
        updates: List[Dict[str, Any]] = []
        for item in execution.executed:
            rec = item.result
            result = rec.get("result")
            if result and str(result).startswith("error"):
                continue
            token = rec.get("token")
            side = rec.get("side")
            amount = float(rec.get("amount", 0.0))
            price = float(rec.get("price", 0.0))
            if not token or not side:
                continue
            delta = amount if side == "buy" else -amount
            if delta == 0:
                continue
            try:
                self.portfolio.update(token, delta, price)
                prices[token] = price
                updates.append({"token": token, "delta": delta, "price": price})
            except Exception as exc:
                log.warning("portfolio update failed for %s: %s", token, exc)
        if prices:
            try:
                self.portfolio.record_prices(prices)
            except Exception:
                pass
            try:
                self.portfolio.update_risk_metrics()
            except Exception:
                pass
        stage.updates = updates
        return stage


__all__ = [
    "SwarmPipeline",
    "SwarmAction",
    "ExecutionDispatcher",
]
