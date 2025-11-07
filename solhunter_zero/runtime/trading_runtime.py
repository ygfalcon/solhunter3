from __future__ import annotations

import asyncio
import errno
import contextlib
import json
import logging
import os
import signal
import subprocess
import threading
import time
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Mapping, Optional

from ..agent_manager import AgentManager
from ..config import (
    CONFIG_DIR,
    apply_env_overrides,
    get_active_config_name,
    get_broker_urls,
    load_config,
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
from .. import discovery_state
from ..agents.discovery import resolve_discovery_method
from ..loop import FirstTradeTimeoutError, run_iteration, _init_rl_training
from ..pipeline import PipelineCoordinator
from ..main import perform_startup_async
from .. import metrics_aggregator
from ..main_state import TradingState
from ..memory import Memory
from ..portfolio import Portfolio
from ..paths import ROOT
from ..redis_util import ensure_local_redis_if_needed
from ..ui import UIState, UIServer, UIStartupError
from ..util import parse_bool_env


log = logging.getLogger(__name__)

DEPTH_PROCESS_SHUTDOWN_TIMEOUT = 5.0

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
        self._trades_lock = threading.Lock()
        self._ui_logs_lock = threading.Lock()
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
        self._offline_mode: bool = False
        self._dry_run_mode: bool = False
        self._testnet_mode: bool = False
        self._startup_modes: Optional[tuple[bool, bool, Optional[bool], bool]] = None
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
        self._metrics_started = False

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

        try:
            await self.start()
            await self.stop_event.wait()
        except Exception as exc:
            self.activity.add("runtime", f"failed: {exc}", ok=False)
            log.exception("Trading runtime failed during run")
            raise
        finally:
            await self.stop()

    async def start(self) -> None:
        self.activity.add("runtime", "starting")
        self._start_time = time.time()
        try:
            await self._prepare_configuration()
            self._ensure_metrics_aggregator_started()
            await self._start_event_bus()
            await self._start_ui()
            await self._start_agents()
            self._start_rl_status_watcher()
            await self._start_loop()
        except Exception as exc:
            self.activity.add("runtime", f"start failed: {exc}", ok=False)
            log.exception("TradingRuntime: start failed; attempting rollback")
            try:
                await self.stop()
            except Exception:
                log.exception("TradingRuntime: error during rollback after start failure")
            raise
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
            proc = self.depth_proc
            try:
                poll = getattr(proc, "poll", None)
                try:
                    running = poll() is None if callable(poll) else True
                except Exception:
                    running = True
                if running:
                    terminate = getattr(proc, "terminate", None)
                    if callable(terminate):
                        try:
                            terminate()
                        except Exception:
                            log.exception(
                                "Failed to terminate depth_service process gracefully"
                            )
                        else:
                            wait_fn = getattr(proc, "wait", None)
                            if callable(wait_fn):
                                try:
                                    wait_fn(timeout=DEPTH_PROCESS_SHUTDOWN_TIMEOUT)
                                except subprocess.TimeoutExpired:
                                    message = (
                                        "depth_service process did not exit within "
                                        f"{DEPTH_PROCESS_SHUTDOWN_TIMEOUT} seconds"
                                    )
                                    log.error(message)
                                    self.activity.add(
                                        "depth_service",
                                        message,
                                        ok=False,
                                    )
                                except Exception:
                                    log.exception(
                                        "Error while waiting for depth_service process to exit"
                                    )
            finally:
                self.depth_proc = None
        self.status.depth_service = False

        if self.pipeline is not None:
            await self.pipeline.stop()
            self.pipeline = None
        self.status.trading_loop = False

        if self.memory is not None:
            with contextlib.suppress(Exception):
                await self.memory.stop_writer()

        self.activity.add("runtime", "stopped")

    # ------------------------------------------------------------------
    # Preparation helpers
    # ------------------------------------------------------------------

    async def _prepare_configuration(self) -> None:
        cfg_path = self._resolve_config_path()
        if cfg_path is not None:
            self.config_path = cfg_path

        config_data, (
            offline_mode,
            dry_run_mode,
            live_discovery_override,
            testnet_mode,
        ) = self._determine_startup_modes(self.config_path)
        self._startup_modes = (
            bool(offline_mode),
            bool(dry_run_mode),
            live_discovery_override,
            bool(testnet_mode),
        )

        cfg, runtime_cfg, depth_proc = await perform_startup_async(
            self.config_path,
            offline=self._startup_modes[0],
            dry_run=self._startup_modes[1],
            testnet=self._startup_modes[3],
            preloaded_config=config_data,
        )

        self.cfg = cfg
        self.runtime_cfg = runtime_cfg
        self.depth_proc = depth_proc
        depth_running = False
        if depth_proc is not None:
            poll = getattr(depth_proc, "poll", lambda: None)
            try:
                if callable(poll):
                    depth_running = poll() is None
                else:
                    depth_running = True
            except Exception:
                depth_running = True

        self.status.depth_service = depth_running

        if self.cfg.get("birdeye_api_key"):
            os.environ.setdefault("BIRDEYE_API_KEY", str(self.cfg["birdeye_api_key"]))
            try:
                from .. import scanner_common

                scanner_common.BIRDEYE_API_KEY = os.environ["BIRDEYE_API_KEY"]
                scanner_common.HEADERS["X-API-KEY"] = scanner_common.BIRDEYE_API_KEY
            except Exception:
                pass

    def _resolve_config_path(self) -> Optional[str]:
        cfg_path = self.config_path
        if cfg_path is None:
            name = get_active_config_name()
            if name:
                candidate = Path(CONFIG_DIR) / name
                if candidate.exists():
                    cfg_path = str(candidate)
            if cfg_path is None:
                cfg_path = "config.toml"
        if cfg_path is not None:
            cfg_path = str(Path(cfg_path).expanduser().resolve())
        return cfg_path

    def _determine_startup_modes(
        self, cfg_path: Optional[str]
    ) -> tuple[Mapping[str, Any], tuple[bool, bool, Optional[bool], bool]]:
        config_data: Mapping[str, Any]
        if cfg_path is not None:
            config_data = apply_env_overrides(load_config(cfg_path))
        else:
            try:
                config_data = apply_env_overrides(load_config(None))
            except FileNotFoundError:
                config_data = {}
        return config_data, self._evaluate_mode_choices(config_data)

    def _evaluate_mode_choices(
        self, config: Mapping[str, Any]
    ) -> tuple[bool, bool, Optional[bool], bool]:
        offline_env_raw = os.getenv("SOLHUNTER_OFFLINE")
        dry_run_env_raw = os.getenv("DRY_RUN")
        live_discovery_env_raw = os.getenv("LIVE_DISCOVERY")

        offline_env = (
            parse_bool_env("SOLHUNTER_OFFLINE", False)
            if offline_env_raw is not None
            else None
        )
        dry_run_env = (
            parse_bool_env("DRY_RUN", False) if dry_run_env_raw is not None else None
        )
        live_discovery_env = (
            parse_bool_env("LIVE_DISCOVERY", False)
            if live_discovery_env_raw is not None
            else None
        )

        testnet_env: Optional[bool] = None
        if os.getenv("TESTNET") is not None:
            testnet_env = parse_bool_env("TESTNET", False)
        elif os.getenv("SOLHUNTER_TESTNET") is not None:
            testnet_env = parse_bool_env("SOLHUNTER_TESTNET", False)

        offline_cfg = _maybe_bool(config.get("offline"))
        dry_run_cfg = _maybe_bool(config.get("dry_run"))
        live_discovery_cfg = _maybe_bool(config.get("live_discovery"))
        testnet_cfg = _maybe_bool(config.get("testnet"))

        live_discovery_override: Optional[bool] = None
        if live_discovery_env is not None:
            live_discovery_override = bool(live_discovery_env)
        elif live_discovery_cfg is not None:
            live_discovery_override = bool(live_discovery_cfg)

        if offline_env is not None:
            offline_mode = bool(offline_env)
        elif offline_cfg is not None:
            offline_mode = bool(offline_cfg)
        elif live_discovery_override is not None:
            offline_mode = not live_discovery_override
        else:
            offline_mode = False

        if dry_run_env is not None:
            dry_run_mode = bool(dry_run_env)
        elif dry_run_cfg is not None:
            dry_run_mode = bool(dry_run_cfg)
        else:
            dry_run_mode = False

        if testnet_env is not None:
            testnet_mode = bool(testnet_env)
        elif testnet_cfg is not None:
            testnet_mode = bool(testnet_cfg)
        else:
            testnet_mode = False

        if live_discovery_override is not None:
            live_discovery_override = bool(live_discovery_override)

        return (
            bool(offline_mode),
            bool(dry_run_mode),
            live_discovery_override,
            bool(testnet_mode),
        )

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

    def _ensure_metrics_aggregator_started(self) -> None:
        if not self._metrics_started:
            metrics_aggregator.start()
            self._metrics_started = True

    async def _start_event_bus(self) -> None:
        broker_urls = get_broker_urls(self.cfg) if self.cfg else []
        try:
            ensure_local_redis_if_needed(broker_urls)
        except Exception as exc:
            message = f"failed to ensure local Redis broker: {exc}"
            self.activity.add("broker", message, ok=False)
            self.status.event_bus = False
            log.exception("Failed to ensure Redis broker")
            raise

        ws_port = int(os.getenv("EVENT_BUS_WS_PORT", "8779") or 8779)
        event_bus_url = f"ws://127.0.0.1:{ws_port}"

        local_ws_disabled = parse_bool_env("EVENT_BUS_DISABLE_LOCAL")
        start_attempts = 3
        start_delay = 0.25
        start_error: Optional[BaseException] = None

        port_range_env = os.getenv("EVENT_BUS_WS_PORT_RANGE", "").strip()
        fallback_ports: List[int] = []
        if port_range_env:
            range_spec = port_range_env.replace(",", "-")
            parts = [p.strip() for p in range_spec.split("-") if p.strip()]
            try:
                if len(parts) == 1:
                    start_port = end_port = int(parts[0])
                elif len(parts) >= 2:
                    start_port = int(parts[0])
                    end_port = int(parts[1])
                else:
                    raise ValueError("empty port range")
                if start_port > end_port:
                    start_port, end_port = end_port, start_port
                fallback_ports = [p for p in range(start_port, end_port + 1)]
            except ValueError:
                log.warning(
                    "Invalid EVENT_BUS_WS_PORT_RANGE %r; falling back to port 0",
                    port_range_env,
                )
                fallback_ports = []

        fallback_ports = [p for p in fallback_ports if p != ws_port]
        if not fallback_ports and ws_port != 0:
            fallback_ports = [0]

        attempt_sequence: List[tuple[str, int]] = [
            ("fixed", ws_port) for _ in range(start_attempts)
        ]
        attempt_sequence.extend(("fallback", port) for port in fallback_ports)

        total_attempts = len(attempt_sequence)
        server = None
        fallback_used = False

        for idx, (stage, port_candidate) in enumerate(attempt_sequence, start=1):
            try:
                server = await start_ws_server("127.0.0.1", port_candidate)
            except Exception as exc:
                start_error = exc
                log.exception(
                    "Failed to start event bus websocket on attempt %s (port %s)",
                    idx,
                    port_candidate,
                )
            else:
                if server is None and not local_ws_disabled:
                    start_error = RuntimeError(
                        "start_ws_server returned no server instance"
                    )
                else:
                    self.bus_started = bool(server)
                    if server is not None and not local_ws_disabled:
                        bound_port = port_candidate
                        try:
                            sockets = getattr(server, "sockets", None)
                            if sockets:
                                sock = sockets[0]
                                sockname = sock.getsockname()
                                if isinstance(sockname, (tuple, list)) and len(sockname) >= 2:
                                    bound_port = int(sockname[1])
                                elif isinstance(sockname, int):
                                    bound_port = sockname
                        except Exception:
                            bound_port = port_candidate
                        event_bus_url = f"ws://127.0.0.1:{bound_port}"
                        os.environ["EVENT_BUS_URL"] = event_bus_url
                        os.environ["BROKER_WS_URLS"] = event_bus_url
                    fallback_used = stage == "fallback"
                    detail = (
                        "local websocket disabled via EVENT_BUS_DISABLE_LOCAL"
                        if local_ws_disabled
                        else (
                            f"listening on {event_bus_url}"
                            if not fallback_used
                            else f"listening on {event_bus_url} (fallback)"
                        )
                    )
                    self.activity.add("event_bus", detail)
                    start_error = None
                    break

            if idx < total_attempts:
                await asyncio.sleep(start_delay)
                start_delay = min(start_delay * 2, 2.0)

        if start_error is not None:
            message = (
                "failed to start event bus websocket after "
                f"{total_attempts} attempts: {start_error}"
            )
            self.activity.add("event_bus", message, ok=False)
            self.status.event_bus = False
            raise RuntimeError(message) from start_error

        verify_attempts = 3
        verify_delay = 0.5
        verify_error: Optional[BaseException] = None
        broker_ok = False

        for attempt in range(1, verify_attempts + 1):
            try:
                broker_ok = await verify_broker_connection(timeout=2.0)
            except Exception as exc:
                verify_error = exc
                broker_ok = False
                log.exception(
                    "Broker verification failed on attempt %s", attempt
                )
            if broker_ok:
                break
            if attempt < verify_attempts:
                await asyncio.sleep(verify_delay)
                verify_delay = min(verify_delay * 2, 4.0)

        if not broker_ok:
            reason = "broker verification failed"
            if verify_error is not None:
                reason = f"{reason}: {verify_error}"
            message = f"{reason} after {verify_attempts} attempts"
            self.activity.add("broker", message, ok=False)
            self.status.event_bus = False
            raise RuntimeError(message) from verify_error

        self.status.event_bus = True
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
            self._append_trade(entry)

        async def _on_log(event: Any) -> None:
            entry = {
                "topic": getattr(event, "topic", "log"),
                "payload": _serialize(getattr(event, "payload", event)),
                "timestamp": datetime.utcnow().isoformat() + "Z",
            }
            self._append_ui_log(entry)

        self._subscriptions.append(("action_executed", _on_action))
        subscribe("action_executed", _on_action)
        self._subscriptions.append(("runtime.log", _on_log))
        subscribe("runtime.log", _on_log)

    async def _start_ui(self) -> None:
        self.ui_state.status_provider = self._collect_status
        self.ui_state.activity_provider = self.activity.snapshot
        self.ui_state.trades_provider = self._snapshot_trades
        self.ui_state.weights_provider = self._collect_weights
        self.ui_state.rl_status_provider = self._collect_rl_status
        self.ui_state.logs_provider = self._snapshot_ui_logs
        self.ui_state.summary_provider = self._collect_iteration
        self.ui_state.discovery_provider = self._collect_discovery
        self.ui_state.discovery_update_callback = self._on_discovery_method_update
        self.ui_state.config_provider = self._collect_config
        self.ui_state.actions_provider = self._collect_actions
        self.ui_state.history_provider = self._collect_history

        port_candidates: list[int] = []
        primary_port = int(self.ui_port)
        port_candidates.append(primary_port)

        range_spec: Optional[str] = None
        if self.cfg:
            cfg_range = self.cfg.get("ui_port_range")
            if cfg_range:
                range_spec = str(cfg_range)
        env_range = os.getenv("UI_PORT_RANGE", "").strip()
        if env_range:
            range_spec = env_range

        fallback_ports: list[int] = []
        if range_spec:
            range_expr = range_spec.replace(",", "-")
            parts = [p.strip() for p in range_expr.split("-") if p.strip()]
            try:
                if len(parts) == 1:
                    start_port = end_port = int(parts[0])
                elif len(parts) >= 2:
                    start_port = int(parts[0])
                    end_port = int(parts[1])
                else:
                    raise ValueError("empty port range")
                if start_port > end_port:
                    start_port, end_port = end_port, start_port
                fallback_ports = list(range(start_port, end_port + 1))
            except ValueError:
                log.warning(
                    "Invalid UI port range %r; falling back to port 0",
                    range_spec,
                )
                fallback_ports = []

        fallback_ports = [p for p in fallback_ports if p not in {primary_port, 0}]
        if primary_port != 0:
            fallback_ports.append(0)

        port_candidates.extend(p for p in fallback_ports if p not in port_candidates)

        last_error: Optional[UIStartupError] = None

        def _is_addr_in_use(error: UIStartupError) -> bool:
            errnos = {errno.EADDRINUSE}
            if hasattr(errno, "WSAEADDRINUSE"):
                errnos.add(errno.WSAEADDRINUSE)  # type: ignore[attr-defined]
            if error.errno in errnos:
                return True
            cause = error.__cause__
            if isinstance(cause, OSError) and getattr(cause, "errno", None) in errnos:
                return True
            return False

        for attempt, port_candidate in enumerate(port_candidates, start=1):
            self.ui_server = UIServer(
                self.ui_state,
                host=self.ui_host,
                port=port_candidate,
            )
            try:
                self.ui_server.start()
            except UIStartupError as exc:
                last_error = exc
                should_retry = _is_addr_in_use(exc) and attempt < len(port_candidates)
                if should_retry:
                    detail = (
                        f"failed to bind on {self.ui_host}:{port_candidate}; retrying"
                    )
                    self.activity.add("ui", detail, ok=False)
                    log.warning(
                        "TradingRuntime: UI port %s unavailable; retrying (attempt %s/%s)",
                        port_candidate,
                        attempt,
                        len(port_candidates),
                    )
                    self.ui_server = None
                    continue
                self.activity.add("ui", f"failed to start: {exc}", ok=False)
                log.exception(
                    "TradingRuntime: failed to start UI server on %s:%s",
                    self.ui_host,
                    port_candidate,
                )
                self.ui_server = None
                raise
            else:
                break
        else:
            self.ui_server = None
            if last_error is not None:
                raise last_error
            raise RuntimeError("UI server failed without an explicit error")

        self.ui_port = self.ui_server.port
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
        offline_mode, dry_run_mode, live_discovery_override, testnet_mode = (
            self._derive_offline_modes()
        )
        self._offline_mode = bool(offline_mode)
        self._dry_run_mode = bool(dry_run_mode)
        self._testnet_mode = bool(testnet_mode)

        if self._use_new_pipeline:
            log.info(
                "TradingRuntime: initialising staged pipeline (fast_mode=%s)",
                fast_mode,
            )
            discovery_interval_cfg = float(self.cfg.get("discovery_interval", 5.0) or 5.0)
            discovery_interval_cfg = max(discovery_interval_cfg, 5.0)

            cache_env = os.getenv("DISCOVERY_CACHE_TTL")
            if cache_env:
                try:
                    discovery_cache_ttl = float(cache_env)
                except ValueError:
                    log.warning("Invalid DISCOVERY_CACHE_TTL=%r; defaulting to 45s", cache_env)
                    discovery_cache_ttl = 45.0
            else:
                discovery_cache_ttl = float(self.cfg.get("discovery_cache_ttl", 45.0) or 45.0)

            discovery_cache_ttl = max(discovery_cache_ttl, discovery_interval_cfg)

            eval_cache_cfg = self.cfg.get("evaluation_cache_ttl")
            evaluation_cache_ttl = None
            if eval_cache_cfg is not None:
                try:
                    evaluation_cache_ttl = float(eval_cache_cfg)
                except (TypeError, ValueError):
                    log.warning(
                        "Invalid evaluation_cache_ttl=%r in config; falling back to defaults",
                        eval_cache_cfg,
                    )
                    evaluation_cache_ttl = None

            token_file = self._resolve_token_file()
            use_offline_discovery = bool(self._offline_mode)
            if live_discovery_override is True:
                use_offline_discovery = False
            elif live_discovery_override is False:
                use_offline_discovery = True
            discovery_limit_cfg = _maybe_int(
                self.cfg.get("discovery_limit", self.cfg.get("discovery_max_tokens"))
            )
            discovery_empty_cache = _maybe_float(
                self.cfg.get("discovery_empty_cache_ttl")
            )
            discovery_backoff = _maybe_float(self.cfg.get("discovery_backoff_factor"))
            discovery_max_backoff = _maybe_float(self.cfg.get("discovery_max_backoff"))
            discovery_startup_clones = _maybe_int(
                self.cfg.get("discovery_startup_clones")
            )

            live_discovery_active = (
                live_discovery_override
                if live_discovery_override is not None
                else not use_offline_discovery
            )
            log.info(
                "TradingRuntime: pipeline discovery offline=%s dry_run=%s live_discovery=%s token_file=%s",
                use_offline_discovery,
                self._dry_run_mode,
                live_discovery_active,
                token_file or "<static>",
            )

            executor = getattr(self.agent_manager, "executor", None)
            if executor is not None:
                try:
                    if hasattr(executor, "dry_run"):
                        setattr(executor, "dry_run", bool(self._dry_run_mode))
                    setattr(executor, "testnet", bool(self._testnet_mode))
                except Exception:  # pragma: no cover - defensive logging
                    log.exception("Failed to toggle executor modes")

            self.pipeline = PipelineCoordinator(
                self.agent_manager,
                self.portfolio,
                discovery_interval=discovery_interval_cfg,
                discovery_cache_ttl=discovery_cache_ttl,
                scoring_batch=self._determine_scoring_batch(fast_mode),
                evaluation_cache_ttl=evaluation_cache_ttl,
                evaluation_workers=self._determine_eval_workers(fast_mode),
                execution_lanes=self._determine_execution_lanes(fast_mode),
                on_evaluation=self._pipeline_on_evaluation,
                on_execution=self._pipeline_on_execution,
                offline=use_offline_discovery,
                token_file=token_file,
                testnet=self._testnet_mode,
                discovery_empty_cache_ttl=discovery_empty_cache,
                discovery_backoff_factor=discovery_backoff,
                discovery_max_backoff=discovery_max_backoff,
                discovery_limit=discovery_limit_cfg,
                discovery_startup_clones=discovery_startup_clones,
            )
            log.info(
                "TradingRuntime: pipeline created; evaluations will run through AgentManager swarm"
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
        try:
            if self._use_new_pipeline and self.pipeline is not None:
                await self.pipeline.start()
                self.status.trading_loop = True
                self.activity.add("loop", "started (pipeline)")
                poller = asyncio.create_task(
                    self._pipeline_telemetry_poller(), name="pipeline_telemetry"
                )
                self._tasks.append(poller)
            else:
                task = asyncio.create_task(self._trading_loop(), name="trading_loop")
                self.trading_task = task
                self.status.trading_loop = True
                self.activity.add("loop", "started")
                self._tasks.append(task)
        except Exception:
            self.status.trading_loop = False
            self.activity.add("loop", "failed to start", ok=False)
            raise

    # ------------------------------------------------------------------
    # Trading loop
    # ------------------------------------------------------------------

    async def _trading_loop(self) -> None:
        assert self.agent_manager is not None
        assert self.memory is not None
        assert self.portfolio is not None

        cancelled = False
        failure: Optional[BaseException] = None
        try:
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
            stop_loss = _maybe_float(self.cfg.get("stop_loss"))
            take_profit = _maybe_float(self.cfg.get("take_profit"))
            trailing_stop = _maybe_float(self.cfg.get("trailing_stop"))
            max_drawdown = _maybe_float(self.cfg.get("max_drawdown"), 1.0)
            volatility_factor = _maybe_float(self.cfg.get("volatility_factor"), 1.0)
            arbitrage_threshold = _maybe_float(self.cfg.get("arbitrage_threshold"), 0.0)
            arbitrage_amount = _maybe_float(self.cfg.get("arbitrage_amount"), 0.0)
            live_discovery = self.cfg.get("live_discovery")
            token_file = self._resolve_token_file() if self._offline_mode else None

            while not self.stop_event.is_set():
                start = time.perf_counter()
                discovery_method = self._current_discovery_method()
                try:
                    summary = await run_iteration(
                        self.memory,
                        self.portfolio,
                        self.state,
                        cfg=self.runtime_cfg,
                        testnet=self._testnet_mode,
                        dry_run=self._dry_run_mode,
                        offline=self._offline_mode,
                        token_file=token_file,
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
        except asyncio.CancelledError:
            cancelled = True
            raise
        except Exception as exc:
            failure = exc
            raise
        finally:
            self.status.trading_loop = False
            if failure is not None:
                self.activity.add("loop", f"stopped with error: {failure}", ok=False)
            else:
                ok = cancelled or self.stop_event.is_set()
                detail = "stopped" if ok else "stopped unexpectedly"
                self.activity.add("loop", detail, ok=ok)

    # ------------------------------------------------------------------
    # UI data helpers
    # ------------------------------------------------------------------

    def _collect_status(self) -> Dict[str, Any]:
        depth_ok = False
        if self.depth_proc is not None:
            poll = getattr(self.depth_proc, "poll", lambda: None)
            try:
                if callable(poll):
                    depth_ok = poll() is None
                else:
                    depth_ok = True
            except Exception:
                depth_ok = True
        rl_snapshot = self._collect_rl_status()
        rl_running = bool(rl_snapshot.get("running")) if rl_snapshot else False
        self.status.rl_daemon = rl_running
        with self._trades_lock:
            trade_count = len(self._trades)
        status = {
            "event_bus": self.status.event_bus,
            "trading_loop": self.status.trading_loop,
            "depth_service": depth_ok,
            "rl_daemon": rl_running,
            "rl_daemon_status": rl_snapshot,
            "heartbeat": self.status.heartbeat_ts,
            "iterations_completed": self._iteration_count,
            "trade_count": trade_count,
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
        snapshot: Dict[str, Any] = {
            "recent": recent[:50],
            "recent_count": len(recent),
            "latest_iteration_tokens": latest,
        }

        pipeline_snapshot: Dict[str, Any] = {}
        pipeline = self.pipeline if self._use_new_pipeline else None
        if pipeline is not None and hasattr(pipeline, "discovery_snapshot"):
            try:
                pipeline_snapshot = pipeline.discovery_snapshot()
            except Exception:
                log.debug("TradingRuntime: failed to snapshot discovery state", exc_info=True)
                pipeline_snapshot = {}

        if isinstance(pipeline_snapshot, dict) and pipeline_snapshot:
            backoff = _maybe_float(pipeline_snapshot.get("current_backoff")) or 0.0
            cooldown_until = _maybe_float(pipeline_snapshot.get("cooldown_until"))
            cooldown_remaining = _maybe_float(pipeline_snapshot.get("cooldown_remaining"))
            if cooldown_remaining is None and cooldown_until is not None:
                cooldown_remaining = max(0.0, cooldown_until - time.time())
            consecutive_empty = pipeline_snapshot.get("consecutive_empty")
            try:
                consecutive_empty_int = int(consecutive_empty)
            except (TypeError, ValueError):
                consecutive_empty_int = 0
            cooldown_active = bool(pipeline_snapshot.get("cooldown_active"))
            if cooldown_remaining is not None and cooldown_remaining <= 0:
                cooldown_remaining = 0.0
                if not cooldown_active:
                    cooldown_until = None
            expiry_iso: Optional[str] = None
            if cooldown_until:
                try:
                    expiry_iso = (
                        datetime.fromtimestamp(cooldown_until, timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except Exception:
                    expiry_iso = None
            last_fetch_ts = _maybe_float(pipeline_snapshot.get("last_fetch_ts"))
            last_fetch_iso: Optional[str] = None
            if last_fetch_ts:
                try:
                    last_fetch_iso = (
                        datetime.fromtimestamp(last_fetch_ts, timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except Exception:
                    last_fetch_iso = None
            last_fetch_count = pipeline_snapshot.get("last_fetch_count")
            try:
                last_fetch_count_int = int(last_fetch_count)
            except (TypeError, ValueError):
                last_fetch_count_int = None
            snapshot.update(
                {
                    "backoff_seconds": backoff,
                    "cooldown_until": cooldown_until,
                    "cooldown_remaining": cooldown_remaining,
                    "cooldown_expires_at": expiry_iso,
                    "cooldown_active": bool(cooldown_active and cooldown_remaining),
                    "consecutive_empty_fetches": consecutive_empty_int,
                    "last_fetch_ts": last_fetch_ts,
                    "last_fetch_at": last_fetch_iso,
                    "last_fetch_count": last_fetch_count_int,
                    "last_fetch_empty": bool(pipeline_snapshot.get("last_fetch_empty")),
                }
            )

        return snapshot

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

    def _on_discovery_method_update(self, method: str) -> None:
        canonical = resolve_discovery_method(method)
        if canonical is None:
            return
        previous = discovery_state.get_override()
        discovery_state.set_override(canonical)
        if previous == canonical:
            return
        pipeline = self.pipeline
        if pipeline is not None:
            refresher = getattr(pipeline, "refresh_discovery", None)
            if callable(refresher):
                try:
                    refresher()
                except Exception:  # pragma: no cover - defensive logging
                    log.exception("Failed to refresh discovery service after method update")
        detail = f"method updated to {canonical}"
        self.activity.add("discovery", detail)
        try:
            publish("runtime.log", {"stage": "discovery", "detail": detail})
        except Exception:  # pragma: no cover - logging only
            log.debug("Failed to publish discovery update", exc_info=True)

    def _current_discovery_method(self) -> str:
        return discovery_state.current_method(config=self.cfg)

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

    def _append_trade(self, entry: Dict[str, Any]) -> None:
        with self._trades_lock:
            self._trades.append(entry)

    def _snapshot_trades(self) -> List[Dict[str, Any]]:
        with self._trades_lock:
            return list(self._trades)

    def _append_ui_log(self, entry: Dict[str, Any]) -> None:
        with self._ui_logs_lock:
            self._ui_logs.append(entry)

    def _snapshot_ui_logs(self) -> List[Dict[str, Any]]:
        with self._ui_logs_lock:
            return list(self._ui_logs)

    def _emit_action_decisions(self, actions: Iterable[Dict[str, Any]]) -> int:
        count = 0
        for action in actions:
            payload = self._prepare_action_decision(action)
            if not payload:
                continue
            try:
                publish("action_decision", payload)
            except Exception:  # pragma: no cover - defensive logging
                log.exception("Failed to publish action_decision")
                continue
            count += 1
        return count

    def _prepare_action_decision(self, action: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(action, dict):
            return None
        token = action.get("token") or action.get("mint")
        if not token:
            return None
        side = str(action.get("side", "")).lower()
        if side not in {"buy", "sell"}:
            return None
        price = _maybe_float(action.get("price"))
        if price is None or price <= 0:
            price = _maybe_float(action.get("entry_price")) or _maybe_float(
                action.get("target_price")
            )
        if price is None or price <= 0:
            return None
        amount = _maybe_float(action.get("amount"))
        if amount is None or amount <= 0:
            amount = _maybe_float(action.get("size"))
        if amount is None or amount <= 0:
            amount = _maybe_float(action.get("quantity"))
        if (amount is None or amount <= 0) and price:
            notional = _maybe_float(action.get("notional_usd"))
            if notional is None or notional <= 0:
                notional = _maybe_float(action.get("notional"))
            if notional is not None and notional > 0:
                amount = notional / price
        if amount is None or amount <= 0:
            return None
        payload: Dict[str, Any] = {
            "token": str(token),
            "side": side,
            "size": float(amount),
            "price": float(price),
        }
        rationale: Dict[str, Any] = {}
        agent = action.get("agent")
        if agent:
            rationale["agent"] = str(agent)
        conv = _maybe_float(action.get("conviction_delta"))
        if conv is not None:
            rationale["conviction_delta"] = conv
        metadata = action.get("metadata")
        if isinstance(metadata, dict) and metadata:
            rationale["metadata"] = metadata
        if rationale:
            payload["rationale"] = rationale
        if self._dry_run_mode:
            payload["dry_run"] = True
        return payload

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
        for action in actions_summary:
            action["dry_run"] = bool(self._dry_run_mode)
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
        summary["dry_run"] = bool(self._dry_run_mode)
        if result.actions:
            self._pending_tokens[result.token] = summary
        else:
            # Avoid retaining tokens that produced no actions. Otherwise a
            # flood of "empty" evaluations would cause ``_pending_tokens`` to
            # grow without bound when there are no corresponding executions to
            # clear them.
            self._pending_tokens.pop(result.token, None)
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
        emitted = self._emit_action_decisions(actions)
        if emitted:
            updated["telemetry"]["execution"]["decisions_emitted"] = emitted
        updated["committed"] = receipt.success
        updated.setdefault("timestamp", timestamp)
        updated.setdefault("timestamp_epoch", time.time())
        updated["dry_run"] = bool(self._dry_run_mode)
        for action in actions:
            action.setdefault("dry_run", bool(self._dry_run_mode))
        await self._apply_iteration_summary(updated, stage="execution")
        self._pending_tokens.pop(receipt.token, None)

    async def _pipeline_telemetry_poller(self) -> None:
        index = 0
        pipeline_identity: Optional[int] = None
        while not self.stop_event.is_set():
            pipeline = self.pipeline
            if not pipeline:
                break
            current_identity = id(pipeline)
            if pipeline_identity != current_identity:
                pipeline_identity = current_identity
                index = 0
            try:
                samples = await pipeline.snapshot_telemetry()
            except Exception:
                samples = []
            if len(samples) < index:
                index = 0
            if samples and index < len(samples):
                for sample in samples[index:]:
                    entry = {
                        "topic": "pipeline",
                        "payload": sample,
                        "timestamp": datetime.utcfromtimestamp(
                            sample.get("timestamp", time.time())
                        ).isoformat()
                        + "Z",
                    }
                    self._append_ui_log(entry)
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

    def _derive_offline_modes(self) -> tuple[bool, bool, Optional[bool], bool]:
        if self._startup_modes is not None:
            offline_mode, dry_run_mode, live_discovery_override, testnet_mode = (
                self._startup_modes
            )
            return (
                bool(offline_mode),
                bool(dry_run_mode),
                live_discovery_override
                if live_discovery_override is None
                else bool(live_discovery_override),
                bool(testnet_mode),
            )

        return self._evaluate_mode_choices(self.cfg)

    def _resolve_token_file(self) -> Optional[str]:
        candidate = self.cfg.get("token_file")
        if not candidate:
            candidate = self.cfg.get("token_list")
        if not candidate:
            candidate = os.getenv("TOKEN_FILE") or os.getenv("TOKEN_LIST")
        if not candidate:
            return None
        try:
            path = Path(str(candidate)).expanduser()
        except Exception:
            return str(candidate)
        return str(path)


def _maybe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in (None, "", "null"):
        return default
    try:
        return float(value)
    except Exception:
        return default


def _maybe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return None


def _maybe_int(value: Any) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except Exception:
        return None


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
