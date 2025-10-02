from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import threading
import time
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Optional

from ..agent_manager import AgentManager
from ..config import (
    apply_env_overrides,
    get_broker_urls,
    load_config,
    load_selected_config,
    set_env_from_config,
)
from ..event_bus import (
    publish,
    subscribe,
    unsubscribe,
    start_ws_server,
    stop_ws_server,
    verify_broker_connection,
)
from ..loop import FirstTradeTimeoutError, run_iteration, _init_rl_training
from ..pipeline import PipelineCoordinator
from ..main import perform_startup_async
from ..main_state import TradingState
from ..memory import Memory
from ..portfolio import Portfolio
from ..paths import ROOT
from ..redis_util import ensure_local_redis_if_needed
from ..ui import UIState, UIServer
from ..util import parse_bool_env


log = logging.getLogger(__name__)

_RL_HEALTH_PATH = ROOT / "rl_daemon.health.json"
_RL_HEALTH_INITIAL_INTERVAL = 5.0
_RL_HEALTH_MAX_INTERVAL = 60.0


def _probe_rl_daemon_health(timeout: float = 0.5) -> Dict[str, Any]:
    """Return information about an externally managed RL daemon.

    The probe inspects ``rl_daemon.health.json`` written by ``run_rl_daemon``
    and, when available, performs a lightweight HTTP request against the
    reported ``/health`` endpoint.  It returns a dictionary with the
    following keys:

    ``detected``
        Whether the health file was found.
    ``running``
        ``True`` when the health endpoint responds successfully.
    ``url``
        The health endpoint URL if one was recorded.
    ``error``
        A short description of the most recent error, if any.
    """

    result: Dict[str, Any] = {
        "detected": False,
        "running": False,
        "url": None,
        "error": None,
    }

    path = _RL_HEALTH_PATH
    if not path.exists():
        return result

    result["detected"] = True
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        result["error"] = f"invalid health file: {exc}"  # pragma: no cover - rare
        return result

    url = payload.get("url")
    if url:
        result["url"] = str(url)
    else:
        result["error"] = "missing url"
        return result

    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            status = getattr(response, "status", None)
            if status is None and hasattr(response, "getcode"):
                try:
                    status = response.getcode()
                except Exception:  # pragma: no cover - urllib variations
                    status = None
            if status is None or 200 <= int(status) < 400:
                result["running"] = True
    except Exception as exc:
        result["error"] = str(exc)

    return result


@dataclass
class RuntimeStatus:
    event_bus: bool = False
    trading_loop: bool = False
    depth_service: bool = False
    rl_daemon: bool = False
    heartbeat_ts: Optional[float] = None


class ActivityLog:
    """Thread-safe rolling buffer for runtime events."""

    def __init__(self, maxlen: int = 200) -> None:
        self._entries: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def add(self, stage: str, detail: str, *, ok: bool = True) -> None:
        entry = {
            "stage": stage,
            "detail": detail,
            "ok": bool(ok),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        with self._lock:
            self._entries.append(entry)

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._entries)


class TradingRuntime:
    """Coordinate event bus, UI, agents, and trading loop."""

    def __init__(
        self,
        *,
        config_path: Optional[str] = None,
        ui_host: str = "127.0.0.1",
        ui_port: int = 5000,
        loop_delay: Optional[float] = None,
        min_delay: Optional[float] = None,
        max_delay: Optional[float] = None,
    ) -> None:
        self.config_path = config_path
        self.ui_host = ui_host
        self.ui_port = int(ui_port)
        self.explicit_loop_delay = loop_delay
        self.explicit_min_delay = min_delay
        self.explicit_max_delay = max_delay

        self.cfg: Dict[str, Any] = {}
        self.runtime_cfg: Any = None
        self.depth_proc: Optional[Any] = None
        self.memory: Optional[Memory] = None
        self.portfolio: Optional[Portfolio] = None
        self.state = TradingState()
        self.agent_manager: Optional[AgentManager] = None
        self.rl_task: Optional[asyncio.Task] = None
        self.trading_task: Optional[asyncio.Task] = None
        self.bus_started = False
        self.ui_server: Optional[UIServer] = None
        self.ui_state = UIState()
        self.activity = ActivityLog()
        self.status = RuntimeStatus()
        self.stop_event = asyncio.Event()
        self._tasks: List[asyncio.Task] = []
        self._subscriptions: List[tuple[str, Any]] = []
        self._trades: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._ui_logs: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._last_iteration: Dict[str, Any] = {}
        self._iteration_lock = threading.Lock()
        self._iteration_count = 0
        self._recent_tokens: Deque[str] = deque()
        self._discovery_seen: set[str] = set()
        self._discovery_lock = threading.Lock()
        self._recent_tokens_limit = int(os.getenv("UI_DISCOVERY_LIMIT", "200") or 200)
        self._start_time: Optional[float] = None
        self._last_iteration_elapsed: Optional[float] = None
        self._last_iteration_errors: List[str] = []
        self._last_actions: List[Dict[str, Any]] = []
        history_limit = int(os.getenv("ITERATION_HISTORY_LIMIT", "120") or 120)
        self._iteration_history: Deque[Dict[str, Any]] = deque(maxlen=history_limit)
        self._history_lock = threading.Lock()
        pipeline_env = os.getenv("NEW_PIPELINE", "")
        fast_flag = os.getenv("FAST_PIPELINE_MODE", "")
        self._use_new_pipeline = (
            pipeline_env.lower() in {"1", "true", "yes", "on"}
            or fast_flag.lower() in {"1", "true", "yes", "on"}
        )
        self.pipeline: Optional[PipelineCoordinator] = None
        self._pending_tokens: Dict[str, Dict[str, Any]] = {}
        self._rl_status_info: Dict[str, Any] = {
            "enabled": False,
            "running": False,
            "source": None,
            "url": None,
            "error": None,
            "detected": False,
            "configured": False,
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_forever(self) -> None:
        """Run the trading runtime until interrupted."""

        try:
            asyncio.run(self._run())
        except KeyboardInterrupt:
            log.info("Trading runtime interrupted")

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:  # pragma: no cover - non-posix
                pass

        await self.start()
        await self.stop_event.wait()
        await self.stop()

    async def start(self) -> None:
        self.activity.add("runtime", "starting")
        self._start_time = time.time()
        await self._prepare_configuration()
        await self._start_event_bus()
        await self._start_ui()
        await self._start_agents()
        self._start_rl_status_watcher()
        await self._start_loop()
        self.activity.add("runtime", "started")

    async def stop(self) -> None:
        if self.stop_event.is_set() and not self._tasks:
            return
        self.stop_event.set()
        self.activity.add("runtime", "stopping")

        tasks_to_cancel = list(self._tasks)
        if self.rl_task is not None and self.rl_task not in tasks_to_cancel:
            tasks_to_cancel.append(self.rl_task)

        for task in tasks_to_cancel:
            task.cancel()
        for task in tasks_to_cancel:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._tasks = [t for t in self._tasks if t not in tasks_to_cancel]

        for topic, handler in list(self._subscriptions):
            with contextlib.suppress(Exception):
                unsubscribe(topic, handler)
        self._subscriptions.clear()

        if self.bus_started:
            with contextlib.suppress(Exception):
                await stop_ws_server()
            self.bus_started = False

        if self.ui_server:
            self.ui_server.stop()
            self.ui_server = None

        if self.rl_task is not None:
            if self.rl_task in tasks_to_cancel:
                self.rl_task = None
            else:
                self.rl_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self.rl_task
                self.rl_task = None

        if self.depth_proc is not None:
            with contextlib.suppress(Exception):
                if self.depth_proc.poll() is None:
                    self.depth_proc.terminate()
            self.depth_proc = None
        self.status.depth_service = False

        if self.pipeline is not None:
            await self.pipeline.stop()
            self.pipeline = None
        self.status.trading_loop = False

        self.activity.add("runtime", "stopped")

    # ------------------------------------------------------------------
    # Preparation helpers
    # ------------------------------------------------------------------

    async def _prepare_configuration(self) -> None:
        cfg_path = self.config_path
        if cfg_path is None:
            selected = load_selected_config()
            if selected:
                cfg_path = str(selected.get("__path__", "config.toml"))
        if cfg_path is not None:
            cfg_path = str(Path(cfg_path).expanduser().resolve())
            self.config_path = cfg_path

        cfg, runtime_cfg, depth_proc = await perform_startup_async(
            self.config_path, offline=False, dry_run=False
        )

        self.cfg = cfg
        self.runtime_cfg = runtime_cfg
        self.depth_proc = depth_proc
        self.status.depth_service = bool(
            depth_proc is not None and getattr(depth_proc, "poll", lambda: 1)() is None
        )

        if self.cfg.get("birdeye_api_key"):
            os.environ.setdefault("BIRDEYE_API_KEY", str(self.cfg["birdeye_api_key"]))
            try:
                from .. import scanner_common

                scanner_common.BIRDEYE_API_KEY = os.environ["BIRDEYE_API_KEY"]
                scanner_common.HEADERS["X-API-KEY"] = scanner_common.BIRDEYE_API_KEY
            except Exception:
                pass

        # Ensure environment reflects configuration for downstream modules
        set_env_from_config(cfg)
        for key in (
            "birdeye_api_key",
            "jupiter_ws_url",
            "orca_dex_url",
            "raydium_dex_url",
            "phoenix_dex_url",
            "meteora_dex_url",
        ):
            val = cfg.get(key)
            env_name = {
                "birdeye_api_key": "BIRDEYE_API_KEY",
                "jupiter_ws_url": "JUPITER_WS_URL",
                "orca_dex_url": "ORCA_DEX_URL",
                "raydium_dex_url": "RAYDIUM_DEX_URL",
                "phoenix_dex_url": "PHOENIX_DEX_URL",
                "meteora_dex_url": "METEORA_DEX_URL",
            }.get(key)
            if val and env_name and not os.getenv(env_name):
                os.environ[env_name] = str(val)
        if os.getenv("PYTORCH_ENABLE_MPS_FALLBACK") is None:
            os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"

    async def _start_event_bus(self) -> None:
        broker_urls = get_broker_urls(self.cfg) if self.cfg else []
        try:
            ensure_local_redis_if_needed(broker_urls)
        except Exception:
            log.exception("Failed to ensure Redis broker")

        ws_port = int(os.getenv("EVENT_BUS_WS_PORT", "8779") or 8779)
        event_bus_url = f"ws://127.0.0.1:{ws_port}"
        os.environ["EVENT_BUS_URL"] = event_bus_url
        os.environ.setdefault("BROKER_WS_URLS", event_bus_url)

        try:
            await start_ws_server("127.0.0.1", ws_port)
            self.bus_started = True
            self.activity.add("event_bus", f"listening on {event_bus_url}")
        except Exception as exc:
            self.activity.add("event_bus", f"failed: {exc}", ok=False)
            log.exception("Failed to start event bus websocket")

        ok = await verify_broker_connection(timeout=2.0)
        self.status.event_bus = ok
        if not ok:
            self.activity.add("broker", "verification failed", ok=False)
        else:
            self.activity.add("broker", "verified")

        self._subscribe_to_events()

    def _subscribe_to_events(self) -> None:
        async def _on_action(event: Any) -> None:
            payload = getattr(event, "action", None)
            result = getattr(event, "result", None)
            if payload is None:
                payload = getattr(event, "payload", {})
            entry = {
                "action": _serialize(payload),
                "result": _serialize(result),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            self._trades.append(entry)

        async def _on_log(event: Any) -> None:
            entry = {
                "topic": getattr(event, "topic", "log"),
                "payload": _serialize(getattr(event, "payload", event)),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            self._ui_logs.append(entry)

        self._subscriptions.append(("action_executed", _on_action))
        subscribe("action_executed", _on_action)
        self._subscriptions.append(("runtime.log", _on_log))
        subscribe("runtime.log", _on_log)

    async def _start_ui(self) -> None:
        self.ui_state.status_provider = self._collect_status
        self.ui_state.activity_provider = self.activity.snapshot
        self.ui_state.trades_provider = lambda: list(self._trades)
        self.ui_state.weights_provider = self._collect_weights
        self.ui_state.rl_status_provider = self._collect_rl_status
        self.ui_state.logs_provider = lambda: list(self._ui_logs)
        self.ui_state.summary_provider = self._collect_iteration
        self.ui_state.discovery_provider = self._collect_discovery
        self.ui_state.config_provider = self._collect_config
        self.ui_state.actions_provider = self._collect_actions
        self.ui_state.history_provider = self._collect_history

        self.ui_server = UIServer(self.ui_state, host=self.ui_host, port=self.ui_port)
        self.ui_server.start()
        self.activity.add("ui", f"http://{self.ui_host}:{self.ui_port}")

    async def _start_agents(self) -> None:
        memory_path = self.cfg.get("memory_path", "sqlite:///memory.db")
        portfolio_path = self.cfg.get("portfolio_path", "portfolio.json")

        self.memory = Memory(memory_path)
        self.memory.start_writer()
        self.portfolio = Portfolio(path=portfolio_path)

        manager = AgentManager.from_config(self.cfg)
        if manager is None:
            manager = AgentManager.from_default()
        if manager is None:
            raise RuntimeError("No trading agents available")
        self.agent_manager = manager

        fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
        fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
        if self._use_new_pipeline:
            self.pipeline = PipelineCoordinator(
                self.agent_manager,
                self.portfolio,
                discovery_interval=float(self.cfg.get("discovery_interval", 3.0) or 3.0),
                discovery_cache_ttl=float(os.getenv("DISCOVERY_CACHE_TTL", "20") or 20.0),
                scoring_batch=self._determine_scoring_batch(fast_mode),
                evaluation_cache_ttl=float(os.getenv("EVALUATION_CACHE_TTL", "10") or 10.0),
                evaluation_workers=self._determine_eval_workers(fast_mode),
                execution_lanes=self._determine_execution_lanes(fast_mode),
                on_evaluation=self._pipeline_on_evaluation,
                on_execution=self._pipeline_on_execution,
            )

        rl_enabled = bool(self.cfg.get("rl_auto_train", False)) or parse_bool_env(
            "RL_DAEMON", False
        )
        external_probe = _probe_rl_daemon_health()
        self._rl_status_info.update(
            {
                "enabled": bool(rl_enabled or external_probe.get("detected")),
                "configured": bool(rl_enabled),
                "running": False,
                "source": None,
                "url": external_probe.get("url"),
                "error": external_probe.get("error"),
                "detected": external_probe.get("detected"),
            }
        )
        rl_interval = float(self.cfg.get("rl_interval", 3600.0) or 3600.0)
        if fast_mode and self.cfg.get("rl_interval") is None:
            fast_interval = float(os.getenv("FAST_RL_INTERVAL", "60") or 60.0)
            rl_interval = min(rl_interval, fast_interval)
        self.rl_task = await _init_rl_training(
            self.cfg, rl_daemon=rl_enabled, rl_interval=rl_interval
        )
        if self.rl_task is not None:
            self._tasks.append(self.rl_task)
            running = not self.rl_task.done()
            self._rl_status_info.update(
                {
                    "enabled": True,
                    "running": running,
                    "source": "internal",
                    "url": None,
                    "error": None,
                }
            )
        elif external_probe.get("detected"):
            running = bool(external_probe.get("running"))
            self._rl_status_info.update(
                {
                    "running": running,
                    "source": "external",
                    "error": external_probe.get("error"),
                }
            )
        else:
            running = False
            self._rl_status_info.update(
                {
                    "running": False,
                    "source": None,
                }
            )
        self.status.rl_daemon = bool(self._rl_status_info.get("running"))

    def _start_rl_status_watcher(self) -> None:
        loop = asyncio.get_running_loop()
        task = loop.create_task(self._rl_status_watcher(), name="rl_status_watcher")
        self._tasks.append(task)

    async def _rl_status_watcher(self) -> None:
        delay = float(_RL_HEALTH_INITIAL_INTERVAL)
        failures = 0
        while not self.stop_event.is_set():
            info = _probe_rl_daemon_health()
            detected = bool(info.get("detected"))
            running = bool(info.get("running"))
            configured = bool(self._rl_status_info.get("configured"))
            internal_running = self.rl_task is not None and not self.rl_task.done()

            updates = {
                "detected": detected,
                "url": info.get("url"),
                "error": info.get("error"),
                "enabled": bool(configured or detected),
            }

            if internal_running:
                updates.update({
                    "running": True,
                    "source": "internal",
                })
                failures = 0
                delay = float(_RL_HEALTH_INITIAL_INTERVAL)
            elif detected:
                updates.update({
                    "running": running,
                    "source": "external",
                })
                failures = 0
                delay = float(_RL_HEALTH_INITIAL_INTERVAL)
            else:
                updates.update({
                    "running": False,
                    "source": None,
                })
                failures += 1
                backoff_factor = max(failures - 1, 0)
                delay = min(
                    float(_RL_HEALTH_INITIAL_INTERVAL) * (2 ** backoff_factor),
                    float(_RL_HEALTH_MAX_INTERVAL),
                )

            if not detected:
                updates.setdefault("url", None)

            self._rl_status_info.update(updates)
            self.status.rl_daemon = bool(self._rl_status_info.get("running"))

            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue
            else:
                break

    async def _start_loop(self) -> None:
        if self._use_new_pipeline and self.pipeline is not None:
            await self.pipeline.start()
            self.status.trading_loop = True
            poller = asyncio.create_task(self._pipeline_telemetry_poller(), name="pipeline_telemetry")
            self._tasks.append(poller)
        else:
            task = asyncio.create_task(self._trading_loop(), name="trading_loop")
            self.trading_task = task
            self._tasks.append(task)

    # ------------------------------------------------------------------
    # Trading loop
    # ------------------------------------------------------------------

    async def _trading_loop(self) -> None:
        assert self.agent_manager is not None
        assert self.memory is not None
        assert self.portfolio is not None

        fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}

        loop_delay = self._config_float("loop_delay", self.explicit_loop_delay, 30.0)
        min_delay = self._config_float("min_delay", self.explicit_min_delay, 5.0)
        max_delay = self._config_float("max_delay", self.explicit_max_delay, 120.0)
        if fast_mode:
            if self.explicit_loop_delay is None and self.cfg.get("loop_delay") is None:
                loop_delay = min(loop_delay, 5.0)
            if self.explicit_min_delay is None and self.cfg.get("min_delay") is None:
                min_delay = min(min_delay, 1.0)
            if self.explicit_max_delay is None and self.cfg.get("max_delay") is None:
                max_delay = min(max_delay, 15.0)
        discovery_method = self.cfg.get("discovery_method") or os.getenv(
            "DISCOVERY_METHOD", "websocket"
        )
        stop_loss = _maybe_float(self.cfg.get("stop_loss"))
        take_profit = _maybe_float(self.cfg.get("take_profit"))
        trailing_stop = _maybe_float(self.cfg.get("trailing_stop"))
        max_drawdown = _maybe_float(self.cfg.get("max_drawdown"), 1.0)
        volatility_factor = _maybe_float(self.cfg.get("volatility_factor"), 1.0)
        arbitrage_threshold = _maybe_float(self.cfg.get("arbitrage_threshold"), 0.0)
        arbitrage_amount = _maybe_float(self.cfg.get("arbitrage_amount"), 0.0)
        live_discovery = self.cfg.get("live_discovery")

        while not self.stop_event.is_set():
            start = time.perf_counter()
            try:
                summary = await run_iteration(
                    self.memory,
                    self.portfolio,
                    self.state,
                    cfg=self.runtime_cfg,
                    testnet=False,
                    dry_run=False,
                    offline=False,
                    token_file=None,
                    discovery_method=discovery_method,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop=trailing_stop,
                    max_drawdown=max_drawdown if max_drawdown is not None else 1.0,
                    volatility_factor=volatility_factor
                    if volatility_factor is not None
                    else 1.0,
                    arbitrage_threshold=arbitrage_threshold,
                    arbitrage_amount=arbitrage_amount,
                    strategy_manager=None,
                    agent_manager=self.agent_manager,
                )
                if not isinstance(summary, dict):
                    summary = {}
                await self._apply_iteration_summary(summary, stage="loop")
            except FirstTradeTimeoutError:
                self.activity.add("loop", "first trade timeout", ok=False)
                publish("runtime.log", {"stage": "loop", "detail": "timeout"})
            except Exception as exc:
                self.activity.add("loop", f"error: {exc}", ok=False)
                log.exception("Trading iteration failed")
                await asyncio.sleep(min_delay)
                continue

            elapsed = time.perf_counter() - start
            sleep_for = max(min_delay, min(max_delay, loop_delay - elapsed))
            await asyncio.sleep(max(0.1, sleep_for))

        self.status.trading_loop = False

    # ------------------------------------------------------------------
    # UI data helpers
    # ------------------------------------------------------------------

    def _collect_status(self) -> Dict[str, Any]:
        depth_ok = bool(self.depth_proc and self.depth_proc.poll() is None)
        rl_snapshot = self._collect_rl_status()
        rl_running = bool(rl_snapshot.get("running")) if rl_snapshot else False
        self.status.rl_daemon = rl_running
        status = {
            "event_bus": self.status.event_bus,
            "trading_loop": self.status.trading_loop,
            "depth_service": depth_ok,
            "rl_daemon": rl_running,
            "rl_daemon_status": rl_snapshot,
            "heartbeat": self.status.heartbeat_ts,
            "iterations_completed": self._iteration_count,
            "trade_count": len(self._trades),
            "activity_count": len(self.activity.snapshot()),
        }
        if hasattr(self.state, "last_tokens"):
            tokens = list(getattr(self.state, "last_tokens", []) or [])
            status["pipeline_tokens"] = tokens[:10]
            status["pipeline_size"] = len(tokens)
        iteration = self._collect_iteration()
        if iteration:
            status["last_iteration"] = {
                "timestamp": iteration.get("timestamp"),
                "actions": iteration.get("actions_count"),
                "discovered": iteration.get("discovered_count"),
                "fallback_used": iteration.get("fallback_used"),
                "elapsed_s": iteration.get("elapsed_s"),
            }
        discovery = self._collect_discovery()
        recent_tokens = discovery.get("recent", [])
        status["recent_tokens"] = recent_tokens[:10]
        return status

    def _collect_weights(self) -> Dict[str, Any]:
        if self.agent_manager is None:
            return {}
        return dict(self.agent_manager.weights)

    def _collect_rl_status(self) -> Dict[str, Any]:
        if self.rl_task is not None:
            running = not self.rl_task.done()
            self._rl_status_info.update(
                {
                    "enabled": True,
                    "running": running,
                    "source": "internal",
                    "configured": True,
                    "error": None,
                    "url": None,
                }
            )
        snapshot = dict(self._rl_status_info)
        snapshot.setdefault("enabled", False)
        snapshot.setdefault("running", False)
        snapshot.setdefault("configured", False)
        snapshot.setdefault("detected", False)
        return snapshot

    def _collect_iteration(self) -> Dict[str, Any]:
        with self._iteration_lock:
            data = dict(self._last_iteration)
            errors = list(self._last_iteration_errors)
            actions = list(self._last_actions)
            elapsed = self._last_iteration_elapsed
        if not data:
            return {}
        ts_epoch = data.get("timestamp_epoch")
        if ts_epoch is not None:
            try:
                age = max(0.0, time.time() - float(ts_epoch))
            except Exception:
                age = None
            else:
                data["age_seconds"] = age
        data.pop("timestamp_epoch", None)
        data.setdefault("iteration_count", self._iteration_count)
        data["errors"] = errors
        data["actions_executed"] = actions
        if elapsed is not None:
            data.setdefault("elapsed_s", elapsed)
        return data

    def _collect_discovery(self) -> Dict[str, Any]:
        with self._discovery_lock:
            recent = list(self._recent_tokens)
        with self._iteration_lock:
            latest = list(self._last_iteration.get("tokens_used", []) or [])
        return {
            "recent": recent[:50],
            "recent_count": len(recent),
            "latest_iteration_tokens": latest,
        }

    def _collect_config(self) -> Dict[str, Any]:
        cfg = self.cfg or {}
        sanitized: Dict[str, Any] = {}
        for key, value in cfg.items():
            lower = str(key).lower()
            if any(term in lower for term in ("key", "secret", "token", "pass", "auth")):
                continue
            sanitized[str(key)] = _serialize(value)
        env_summary: Dict[str, Optional[str]] = {}
        for env_key in (
            "SOLANA_RPC_URL",
            "SOLANA_WS_URL",
            "BIRDEYE_API_KEY",
            "EVENT_BUS_URL",
        ):
            val = os.getenv(env_key)
            if env_key == "BIRDEYE_API_KEY" and val:
                env_summary[env_key] = (val[:4] + "…" + val[-2:]) if len(val) > 6 else "***"
            else:
                env_summary[env_key] = val
        agents = (
            list(self.agent_manager.weights.keys())
            if self.agent_manager is not None
            else []
        )
        iteration = self._collect_iteration()
        return {
            "config_path": self.config_path,
            "agents": agents,
            "memory_path": cfg.get("memory_path"),
            "portfolio_path": cfg.get("portfolio_path"),
            "loop_delay": self._config_float("loop_delay", self.explicit_loop_delay, 30.0),
            "min_delay": self._config_float("min_delay", self.explicit_min_delay, 5.0),
            "max_delay": self._config_float("max_delay", self.explicit_max_delay, 120.0),
            "extras": _serialize(cfg.get("extras") or []),
            "env": env_summary,
            "iteration_count": self._iteration_count,
            "last_iteration_timestamp": iteration.get("timestamp"),
            "sanitized_config": sanitized,
        }

    def _record_discovery(self, tokens: Iterable[str]) -> None:
        with self._discovery_lock:
            for token in tokens:
                if token is None:
                    continue
                tok = str(token)
                if tok in self._discovery_seen:
                    continue
                self._recent_tokens.appendleft(tok)
                self._discovery_seen.add(tok)
            while len(self._recent_tokens) > self._recent_tokens_limit:
                removed = self._recent_tokens.pop()
                self._discovery_seen.discard(removed)

    def _collect_actions(self) -> List[Dict[str, Any]]:
        with self._iteration_lock:
            return list(self._last_actions)

    def _determine_scoring_batch(self, fast_mode: bool) -> Optional[int]:
        raw = int(os.getenv("PIPELINE_TOKEN_LIMIT", "0") or 0)
        if raw > 0:
            return raw
        if fast_mode:
            return 16
        return None

    def _determine_eval_workers(self, fast_mode: bool) -> Optional[int]:
        raw = int(os.getenv("EVALUATION_WORKERS", "0") or 0)
        if raw > 0:
            return raw
        return (os.cpu_count() or 8) if fast_mode else None

    def _determine_execution_lanes(self, fast_mode: bool) -> Optional[int]:
        raw = int(os.getenv("EXECUTION_LANE_WORKERS", "0") or 0)
        if raw > 0:
            return raw
        if fast_mode:
            return max(2, (os.cpu_count() or 8) // 2)
        return None

    async def _apply_iteration_summary(self, summary: Dict[str, Any], stage: str = "loop") -> None:
        if summary is None:
            summary = {}
        now_ts = summary.get("timestamp_epoch", time.time())
        self.status.trading_loop = True
        self.status.heartbeat_ts = now_ts
        if stage == "loop" or summary.get("committed"):
            self._iteration_count += 1

        actions = summary.get("actions_executed") or []
        with self._iteration_lock:
            self._last_iteration = dict(summary)
            self._last_iteration_errors = list(summary.get("errors", []))
            self._last_actions = list(actions)
            self._last_iteration_elapsed = summary.get("elapsed_s")
            snap = {
                "timestamp": summary.get("timestamp"),
                "elapsed_s": summary.get("elapsed_s"),
                "actions_count": summary.get("actions_count"),
                "discovered_count": summary.get("discovered_count"),
                "any_trade": summary.get("any_trade"),
                "telemetry": summary.get("telemetry", {}),
                "committed": summary.get("committed", False),
            }
            with self._history_lock:
                self._iteration_history.append(snap)

        tokens_for_discovery = (
            summary.get("tokens_used")
            or summary.get("tokens_discovered")
            or []
        )
        if tokens_for_discovery:
            self._record_discovery(tokens_for_discovery)

        detail = summary.get("detail")
        if not detail:
            count = summary.get("actions_count")
            if stage == "loop":
                detail = f"completed iteration (actions={count or 0})"
            else:
                detail = f"{stage} actions={count or 0}"
        self.activity.add(stage, detail)
        publish(
            "runtime.log",
            {
                "stage": stage,
                "detail": detail,
                "actions": summary.get("actions_count") or 0,
            },
        )

    async def _pipeline_on_evaluation(self, result) -> None:
        if result is None:
            return
        timestamp = datetime.utcnow().isoformat() + "Z"
        actions_summary = [
            {
                "token": action.get("token", result.token),
                "side": action.get("side"),
                "amount": action.get("amount"),
                "price": action.get("price"),
                "agent": action.get("agent"),
                "result": "pending",
            }
            for action in result.actions
        ]
        summary = {
            "timestamp": timestamp,
            "timestamp_epoch": time.time(),
            "elapsed_s": result.latency,
            "discovered_count": None,
            "tokens_discovered": [],
            "tokens_used": [result.token],
            "picked_tokens": [result.token],
            "fallback_used": False,
            "actions_executed": actions_summary,
            "actions_count": len(result.actions),
            "any_trade": bool(result.actions),
            "errors": result.errors,
            "token_results": [
                {
                    "token": result.token,
                    "actions": result.actions,
                    "errors": result.errors,
                    "score": result.metadata.get("score") if result.metadata else None,
                }
            ],
            "telemetry": {
                "evaluation": {
                    "latency": result.latency,
                    "cached": result.cached,
                    "errors": result.errors,
                }
            },
            "committed": False,
        }
        self._pending_tokens[result.token] = summary
        await self._apply_iteration_summary(summary, stage="pipeline")

    async def _pipeline_on_execution(self, receipt) -> None:
        if receipt is None:
            return
        summary = self._pending_tokens.get(receipt.token)
        timestamp = datetime.utcnow().isoformat() + "Z"
        actions = []
        if summary:
            actions = summary.get("actions_executed", [])
        if receipt.success and actions:
            for action, result in zip(actions, receipt.results):
                action["result"] = result
        elif actions:
            for action in actions:
                action["result"] = receipt.errors[0] if receipt.errors else "error"

        updated = summary or {
            "timestamp": timestamp,
            "timestamp_epoch": time.time(),
            "tokens_used": [receipt.token],
            "picked_tokens": [receipt.token],
            "telemetry": {},
        }
        updated["actions_executed"] = actions
        updated["actions_count"] = len(actions)
        updated["any_trade"] = receipt.success and bool(actions)
        updated.setdefault("errors", [])
        if receipt.errors:
            updated["errors"] = list(set(updated.get("errors", [])) | set(receipt.errors))
        updated["telemetry"] = updated.get("telemetry", {})
        updated.setdefault("telemetry", {}).setdefault("execution", {})
        updated["telemetry"]["execution"].update(
            {
                "latency": receipt.finished_at - receipt.started_at,
                "success": receipt.success,
                "errors": receipt.errors,
            }
        )
        updated["committed"] = receipt.success
        updated.setdefault("timestamp", timestamp)
        updated.setdefault("timestamp_epoch", time.time())
        await self._apply_iteration_summary(updated, stage="execution")
        self._pending_tokens.pop(receipt.token, None)

    async def _pipeline_telemetry_poller(self) -> None:
        index = 0
        while not self.stop_event.is_set():
            if not self.pipeline:
                break
            try:
                samples = await self.pipeline.snapshot_telemetry()
            except Exception:
                samples = []
            if samples and index < len(samples):
                for sample in samples[index:]:
                    entry = {
                        "topic": "pipeline",
                        "payload": sample,
                        "timestamp": datetime.utcfromtimestamp(sample.get("timestamp", time.time())).isoformat() + "Z",
                    }
                    self._ui_logs.append(entry)
                index = len(samples)
            await asyncio.sleep(1.0)
    def _collect_history(self) -> List[Dict[str, Any]]:
        with self._history_lock:
            return list(self._iteration_history)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _config_float(
        self, key: str, explicit: Optional[float], default: float
    ) -> float:
        if explicit is not None:
            return float(explicit)
        val = self.cfg.get(key)
        if val is None or val == "":
            return default
        try:
            return float(val)
        except Exception:
            return default


def _maybe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in (None, "", "null"):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _serialize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _serialize(v) for k, v in value.items()}
    if hasattr(value, "_asdict"):
        return {str(k): _serialize(v) for k, v in value._asdict().items()}
    if hasattr(value, "__dict__"):
        return {str(k): _serialize(v) for k, v in value.__dict__.items()}
    return str(value)
