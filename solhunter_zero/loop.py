from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from datetime import datetime
from typing import Any, Dict, Optional

from .event_bus import publish
from .schemas import Heartbeat, RuntimeLog, WeightsUpdated
from .agents.execution import ExecutionAgent
from .agent_manager import EvaluationContext
from .strategy_manager import StrategyManager
from .swarm_pipeline import SwarmPipeline
from .util import parse_bool_env

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Legacy compatibility wrappers
# ---------------------------------------------------------------------


class _LegacyExecutor:
    """Minimal executor that reuses :func:`place_order_async` for legacy runs.

    Depth service integration is intentionally disabled in legacy mode so newer
    components are not accidentally wired into older entrypoints.
    """

    priority_rpc: list[str] | None = None

    def __init__(self, *, dry_run: bool, testnet: bool) -> None:
        self.dry_run = dry_run
        self.testnet = testnet

    async def execute(self, action: Dict[str, Any]) -> Any:
        payload = dict(action)
        from . import main as main_module

        place_func = getattr(main_module, "place_order_async", place_order_async)
        return await place_func(
            payload.get("token"),
            payload.get("side"),
            payload.get("amount", 0.0),
            payload.get("price", 0.0),
            testnet=self.testnet,
            dry_run=self.dry_run,
            keypair=payload.get("keypair"),
            connectivity_test=payload.get("connectivity_test", False),
            base_url=payload.get("base_url"),
            venues=payload.get("venues"),
            max_retries=payload.get("max_retries", 1),
            timeout=payload.get("timeout"),
        )

    def add_executor(self, token: str, execer: Any) -> None:
        # depth-service is disabled in legacy mode; no-op
        return None


class _LegacyStrategyAdapter:
    """Expose ``StrategyManager`` via the AgentManager interface for the pipeline.

    Legacy runs operate without the depth service to match historical behaviour.
    """

    def __init__(self, strategy_manager: StrategyManager, *, dry_run: bool, testnet: bool) -> None:
        self.strategy_manager = strategy_manager
        self.executor = _LegacyExecutor(dry_run=dry_run, testnet=testnet)
        self.memory_agent = None
        self.emotion_agent = None
        self.depth_service = False
        self.skip_simulation = True

    async def evaluate_with_swarm(self, token: str, portfolio: Any) -> EvaluationContext:
        result = await self.strategy_manager.evaluate(token, portfolio)
        actions: list[Dict[str, Any]] = []
        for action in result or []:
            if not isinstance(action, dict):
                continue
            data = dict(action)
            data.setdefault("agent", "strategy")
            actions.append(data)
        return EvaluationContext(token=token, actions=actions, swarm=None, agents=[], weights={})

    def consume_swarm(self, token: str, default: Any = None) -> Any:
        return None


# ---------------------------------------------------------------------
# Public exceptions expected by callers
# ---------------------------------------------------------------------
class FirstTradeTimeoutError(TimeoutError):
    """Raised if no trade is placed within an expected bootstrapping window."""


# ---------------------------------------------------------------------
# RL bootstrap (lightweight stub that plays nice with the rest of the app)
# ---------------------------------------------------------------------
async def _init_rl_training(
    cfg: Dict[str, Any] | None,
    rl_daemon: bool | None = None,
    rl_interval: float = 3600.0,
) -> Optional[asyncio.Task]:
    """
    Start a tiny periodic task that *could* be swapped with a real RL loop.
    The daemon honours ``rl_auto_train`` from the loaded configuration and the
    ``RL_DAEMON`` environment variable.  When either toggle is enabled it
    publishes periodic weight updates and heartbeats so downstream listeners
    don't break.  Return the asyncio.Task so the runtime can cancel it on
    shutdown.
    """
    cfg = cfg or {}
    auto_flag = cfg.get("rl_auto_train")
    if auto_flag is None:
        auto_flag = cfg.get("use_rl_weights")
    enabled = bool(auto_flag)
    if rl_daemon is not None:
        enabled = enabled or bool(rl_daemon)
    enabled = enabled or parse_bool_env("RL_DAEMON", False)

    if not enabled:
        return None

    async def _loop() -> None:
        publish("runtime.log", RuntimeLog(stage="rl", detail="daemon-start"))
        try:
            while True:
                # In a real setup you'd compute new weights here.
                publish("heartbeat", Heartbeat(service="rl_daemon"))
                publish("weights_updated", WeightsUpdated(weights={}))
                publish("runtime.log", RuntimeLog(stage="rl", detail="tick"))
                await asyncio.sleep(max(5.0, float(rl_interval)))
        except asyncio.CancelledError:
            publish("runtime.log", RuntimeLog(stage="rl", detail="daemon-stop"))
            publish("heartbeat", Heartbeat(service="rl_daemon"))
            raise

    return asyncio.create_task(_loop(), name="rl_daemon")


# ---------------------------------------------------------------------
# Order helpers expected by main.py
# ---------------------------------------------------------------------
async def place_order_async(
    token_or_action: Any,
    side: str | None = None,
    amount: float | None = None,
    price: float | None = None,
    *,
    testnet: bool = False,
    dry_run: bool = False,
    keypair: Any | None = None,
    connectivity_test: bool = False,
    agent_manager: Any | None = None,
    base_url: str | None = None,
    venues: list[str] | None = None,
    max_retries: int | None = None,
    timeout: float | None = None,
    **extra: Any,
) -> Any:
    """Compatibility wrapper for legacy positional order entry and new dict payloads."""

    if isinstance(token_or_action, dict):
        payload = dict(token_or_action)
        token = payload.get("token")
        side = payload.get("side", side)
        amount = payload.get("amount", amount)
        price = payload.get("price", price)
        testnet = bool(payload.get("testnet", testnet))
        dry_run = bool(payload.get("dry_run", dry_run))
        keypair = payload.get("keypair", keypair)
        connectivity_test = bool(payload.get("connectivity_test", connectivity_test))
        base_url = payload.get("base_url", base_url)
        venues = payload.get("venues", venues)
        max_retries = payload.get("max_retries", max_retries)
        timeout = payload.get("timeout", timeout)
        rest = {
            k: v
            for k, v in payload.items()
            if k
            not in {
                "token",
                "side",
                "amount",
                "price",
                "testnet",
                "dry_run",
                "keypair",
                "connectivity_test",
                "base_url",
                "venues",
                "max_retries",
                "timeout",
            }
        }
        if rest:
            extra = {**rest, **extra}
    else:
        token = token_or_action

    if token is None or side is None:
        raise ValueError("place_order_async requires token and side")

    publish("runtime.log", RuntimeLog(stage="order", detail=f"submit:{side}"))

    action: Dict[str, Any] = {
        "token": token,
        "side": side,
        "amount": float(amount or 0.0),
        "price": float(price or 0.0),
        "testnet": bool(testnet),
        "dry_run": bool(dry_run),
        "keypair": keypair,
        "connectivity_test": bool(connectivity_test),
    }
    if base_url is not None:
        action["base_url"] = base_url
    if venues is not None:
        action["venues"] = venues
    if max_retries is not None:
        action["max_retries"] = max_retries
    if timeout is not None:
        action["timeout"] = timeout
    if extra:
        action.update(extra)

    execer: ExecutionAgent
    if agent_manager and getattr(agent_manager, "executor", None):
        execer = agent_manager.executor  # type: ignore[attr-defined]
    else:
        execer = ExecutionAgent(dry_run=dry_run, testnet=testnet, keypair=keypair)

    try:
        result = await execer.execute(action)
    except Exception as exc:
        log.exception("order execution failed: %s", exc)
        publish(
            "runtime.log",
            RuntimeLog(
                stage="order",
                detail=f"error:{type(exc).__name__}",
                level="ERROR",
            ),
        )
        raise
    publish("runtime.log", RuntimeLog(stage="order", detail="submitted"))
    return result


async def cancel_all_async(
    token: Optional[str] = None,
    *,
    agent_manager: Any | None = None,
) -> None:
    """
    Optional convenience. If ExecutionAgent exposes a cancel_all, call it.
    Otherwise, no-op.
    """
    execer: Optional[ExecutionAgent] = None
    if agent_manager and getattr(agent_manager, "executor", None):
        execer = agent_manager.executor  # type: ignore[attr-defined]
    if execer and hasattr(execer, "cancel_all"):
        try:
            if asyncio.iscoroutinefunction(execer.cancel_all):  # type: ignore[attr-defined]
                await execer.cancel_all(token)  # type: ignore[attr-defined]
            else:
                execer.cancel_all(token)  # type: ignore[attr-defined]
            publish("runtime.log", RuntimeLog(stage="order", detail=f"cancel_all:{token or '*'}"))
        except Exception as exc:
            log.warning("cancel_all failed: %s", exc)


# ---------------------------------------------------------------------
# One trading iteration (agent-first)
# ---------------------------------------------------------------------
async def run_iteration(
    memory: Any,
    portfolio: Any,
    state: Any,
    *,
    cfg: Dict[str, Any] | None = None,
    loop_delay: float | None = None,
    min_delay: float | None = None,
    max_delay: float | None = None,
    cpu_low_threshold: float | None = None,
    cpu_high_threshold: float | None = None,
    depth_freq_low: float | None = None,
    depth_freq_high: float | None = None,
    depth_rate_limit: float | None = None,
    iterations: int | None = None,
    testnet: bool = False,
    dry_run: bool = False,
    offline: bool = False,
    token_file: Optional[str] = None,
    discovery_method: str | None = None,  # kept for compatibility
    stop_loss: Optional[float] = None,
    take_profit: Optional[float] = None,
    trailing_stop: Optional[float] = None,
    max_drawdown: float = 1.0,
    volatility_factor: float = 1.0,
    arbitrage_threshold: float = 0.0,
    arbitrage_amount: float = 0.0,
    arbitrage_tokens: list[str] | None = None,
    strategy_manager: Any | None = None,   # unused, reserved
    agent_manager: Any | None = None,
    keypair: Any | None = None,
    market_ws_url: str | None = None,
    order_book_ws_url: str | None = None,
    rl_daemon: bool | None = None,
    rl_interval: float | None = None,
    proc_ref: Any | None = None,
    live_discovery: bool | None = None,
) -> Dict[str, Any]:
    """Coordinate discovery, evaluation, simulation, execution, and feedback."""

    if agent_manager is None:
        if strategy_manager is None:
            try:
                from . import main as main_module  # lazy import to honour monkeypatching

                strategy_cls = getattr(main_module, "StrategyManager", StrategyManager)
            except Exception:
                strategy_cls = StrategyManager
            strategy_manager = strategy_cls()
        agent_manager = _LegacyStrategyAdapter(
            strategy_manager,
            dry_run=dry_run,
            testnet=testnet,
        )

    pipeline = SwarmPipeline(
        agent_manager,
        portfolio,
        memory=memory,
        state=state,
        dry_run=dry_run,
        testnet=testnet,
        offline=offline,
        token_file=token_file,
        discovery_method=discovery_method,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trailing_stop=trailing_stop,
        max_drawdown=max_drawdown,
        volatility_factor=volatility_factor,
    )

    executor = getattr(agent_manager, "executor", None)
    original_dry_run = None
    if executor is not None and hasattr(executor, "dry_run"):
        try:
            original_dry_run = executor.dry_run
        except Exception:
            original_dry_run = None
        try:
            executor.dry_run = bool(getattr(executor, "dry_run", False) or dry_run)
        except Exception:
            pass

    try:
        result = await pipeline.run()
    finally:
        if original_dry_run is not None and executor is not None and hasattr(executor, "dry_run"):
            try:
                executor.dry_run = original_dry_run
            except Exception:
                pass
    result.setdefault("timestamp", datetime.utcnow().isoformat() + "Z")
    result.setdefault("timestamp_epoch", time.time())
    result.setdefault("dry_run", bool(dry_run))
    result.setdefault("testnet", bool(testnet))
    return result


def _result_has_trade(result: Any) -> bool:
    """Return ``True`` when the pipeline ``result`` contains a trade/fill."""

    if not isinstance(result, dict):
        return False

    for key in ("executions", "trades", "fills", "orders"):
        entries = result.get(key)
        if isinstance(entries, list) and len(entries) > 0:
            return True

    nested = result.get("data")
    if isinstance(nested, dict):
        return _result_has_trade(nested)

    return False


async def trading_loop(
    cfg: Dict[str, Any] | None,
    runtime_cfg: Any,
    memory: Any,
    portfolio: Any,
    state: Any,
    *,
    iterations: int | None = None,
    loop_delay: float = 30.0,
    min_delay: float = 5.0,
    max_delay: float = 120.0,
    **kwargs: Any,
) -> None:
    """Run repeated swarm iterations using the new pipeline."""

    backoff = 1.0
    count = 0

    deadline_env = os.getenv("FIRST_TRADE_DEADLINE_SEC", "0")
    try:
        first_trade_deadline = float(deadline_env) if deadline_env else 0.0
    except ValueError:
        first_trade_deadline = 0.0
    if first_trade_deadline < 0:
        first_trade_deadline = 0.0

    loop_start = time.time() if first_trade_deadline else 0.0
    first_trade_ts: float | None = None

    while iterations is None or count < iterations:
        start = time.perf_counter()
        try:
            result = await run_iteration(
                memory,
                portfolio,
                state,
                cfg=runtime_cfg or cfg,
                **kwargs,
            )
        except asyncio.CancelledError:
            publish("heartbeat", Heartbeat(service="trading_loop"))
            raise
        except Exception as exc:
            log.exception("iteration failed: %s", exc)
            publish(
                "runtime.log",
                RuntimeLog(
                    stage="loop",
                    detail=f"iteration-error:{type(exc).__name__}",
                    level="ERROR",
                ),
            )
            publish("heartbeat", Heartbeat(service="trading_loop"))
            jitter = random.uniform(0.5, 1.5)
            sleep_for = min(30.0, backoff) * jitter
            await asyncio.sleep(sleep_for)
            backoff = min(30.0, backoff * 2.0)
            continue

        backoff = 1.0
        elapsed = time.perf_counter() - start
        publish("heartbeat", Heartbeat(service="trading_loop"))
        publish(
            "system_metrics_combined",
            {"cpu": 0.0, "memory": 0.0, "iter_ms": elapsed * 1000.0},
        )

        if first_trade_deadline and first_trade_ts is None:
            if _result_has_trade(result):
                first_trade_ts = time.time()
            elif (time.time() - loop_start) >= first_trade_deadline:
                log.error(
                    "First trade not recorded within %.2f seconds", first_trade_deadline
                )
                publish(
                    "runtime.log",
                    RuntimeLog(
                        stage="loop",
                        detail="first-trade-timeout",
                        level="ERROR",
                    ),
                )
                raise FirstTradeTimeoutError("No trade within bootstrap deadline")

        count += 1
        if iterations is not None and count >= iterations:
            break
        delay = max(min_delay, min(max_delay, loop_delay - elapsed))
        await asyncio.sleep(max(0.0, delay))
