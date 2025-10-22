from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import math
import os
import signal
import threading
import time
import urllib.request
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Deque, Dict, Iterable, List, Mapping, Optional, Tuple

ROOT = Path(__file__).resolve().parent.parent

from ..agent_manager import AgentManager
from ..config import get_broker_urls, load_selected_config, set_env_from_config
from ..event_bus import (
    publish,
    subscribe,
    unsubscribe,
    start_ws_server,
    stop_ws_server,
    verify_broker_connection,
    get_ws_address,
)
from ..agents.discovery import DEFAULT_DISCOVERY_METHOD, resolve_discovery_method
from ..loop import (
    FirstTradeTimeoutError,
    ResourceBudgetExceeded,
    run_iteration,
    _init_rl_training,
)
from .. import resource_monitor
from ..pipeline import PipelineCoordinator
from ..main import perform_startup_async
from ..main_state import TradingState
from ..memory import Memory
from ..portfolio import Portfolio
from ..exit_management import ExitManager
from ..schemas import RuntimeLog

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..golden_pipeline.flags import (
        resolve_depth_flag,
        resolve_momentum_flag,
    )
    from ..golden_pipeline.service import GoldenPipelineService
    from ..redis_util import ensure_local_redis_if_needed
    from ..ui import UIState, UIServer
    from ..util import parse_bool_env
    from .runtime_wiring import resolve_golden_enabled
    from .schema_adapters import read_golden, read_ohlcv
    from .tuning import analyse_evaluation


# Compute ROOT locally to avoid circular imports during module import time.
ROOT = Path(__file__).resolve().parent.parent


log = logging.getLogger(__name__)

_RL_HEALTH_PATH = ROOT / "rl_daemon.health.json"
_RL_HEALTH_INITIAL_INTERVAL = 5.0
_RL_HEALTH_MAX_INTERVAL = 60.0


def _int_env(name: str, default: int) -> int:
    try:
        raw = os.getenv(name, str(default))
        if raw is None or raw == "":
            return default
        return max(1, int(raw))
    except Exception:
        return default


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

    body: Optional[bytes] = None
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
            try:
                body = response.read()
            except Exception:  # pragma: no cover - optional body
                body = None
    except Exception as exc:
        result["error"] = str(exc)

    if body:
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            payload = None
        if isinstance(payload, dict):
            status = str(payload.get("status", "")).strip().lower()
            result["status"] = status or None
            heartbeat = payload.get("last_heartbeat")
            try:
                hb_ts = float(heartbeat) if heartbeat is not None else None
            except Exception:
                hb_ts = None
            else:
                if hb_ts is not None:
                    result["heartbeat_ts"] = hb_ts
                    result["heartbeat_age"] = max(0.0, time.time() - hb_ts)
            training = payload.get("last_training")
            try:
                result["last_training"] = float(training) if training is not None else None
            except Exception:
                result["last_training"] = None
            if payload.get("last_error"):
                result["last_error"] = payload.get("last_error")

    return result


_EXIT_PANEL_KEYS: tuple[str, ...] = (
    "hot_watch",
    "diagnostics",
    "queue",
    "closed",
    "missed_exits",
)


def _sanitize_exit_payload(data: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Return a defensive copy of *data* with required keys present."""

    sanitized: Dict[str, Any] = {}
    if isinstance(data, Mapping):
        for key, value in data.items():
            try:
                sanitized[key] = copy.deepcopy(value)
            except Exception:
                sanitized[key] = value
    for key in _EXIT_PANEL_KEYS:
        value = sanitized.get(key)
        if isinstance(value, list):
            try:
                sanitized[key] = copy.deepcopy(value)
            except Exception:
                sanitized[key] = list(value)
        elif value is None:
            sanitized[key] = []
        else:
            sanitized[key] = [value]
    for key in _EXIT_PANEL_KEYS:
        sanitized.setdefault(key, [])
    return sanitized


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

        self._ui_enabled = os.getenv("UI_ENABLED", "1").lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
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
        self._trades: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_TRADES_LIMIT", 200)
        )
        self._ui_logs: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_LOGS_LIMIT", 200)
        )
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
        self._swarm_lock = threading.Lock()
        self._discovery_candidates: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._token_facts: Dict[str, Dict[str, Any]] = {}
        self._market_ohlcv: Dict[str, Dict[str, Any]] = {}
        self._market_depth: Dict[str, Dict[str, Any]] = {}
        self._golden_snapshots: Dict[str, Dict[str, Any]] = {}
        self._latest_golden_hash: Dict[str, str] = {}
        self._agent_suggestions: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_SUGGESTIONS_LIMIT", 600)
        )
        self._agent_rejections: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_REJECTIONS_LIMIT", 600)
        )
        self._vote_decisions: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_VOTE_LIMIT", 400)
        )
        self._decision_counts: Dict[str, int] = {}
        self._decision_recent: Deque[tuple[str, float]] = deque()
        self._decision_first_seen: Dict[str, float] = {}
        fills_limit = _int_env("UI_FILLS_LIMIT", 400)
        self._virtual_fills: Deque[Dict[str, Any]] = deque(maxlen=fills_limit)
        self._live_fills: Deque[Dict[str, Any]] = deque(maxlen=fills_limit)
        self._paper_positions_cache: List[Dict[str, Any]] = []
        self._rl_weights_windows: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_RL_WEIGHTS_LIMIT", 240)
        )
        self._control_states: Dict[str, Dict[str, Any]] = {}
        pipeline_env = os.getenv("NEW_PIPELINE", "")
        fast_flag = os.getenv("FAST_PIPELINE_MODE", "")
        self._use_new_pipeline = (
            pipeline_env.lower() in {"1", "true", "yes", "on"}
            or fast_flag.lower() in {"1", "true", "yes", "on"}
        )
        self._use_golden_pipeline = False
        self._golden_service: Optional["GoldenPipelineService"] = None
        self.pipeline: Optional[PipelineCoordinator] = None
        self._pending_tokens: Dict[str, Dict[str, Any]] = {}
        self._last_eval_summary: Dict[str, Any] = {}
        self._last_execution_summary: Dict[str, Any] = {}
        self._rl_window_sec: float = 0.4
        self._rl_gate_active: bool = False
        self._rl_gate_reason: Optional[str] = "boot"
        self._last_heartbeat_perf: Optional[float] = None
        self._loop_delay: float = 30.0
        self._loop_min_delay: float = 5.0
        self._loop_max_delay: float = 120.0
        self._heartbeat_budget: float = 60.0
        self._rl_status_info: Dict[str, Any] = {
            "enabled": False,
            "running": False,
            "source": None,
            "url": None,
            "error": None,
            "detected": False,
            "configured": False,
            "health_ok": None,
            "heartbeat_age": None,
            "heartbeat_ts": None,
        }
        self._resource_snapshot: Dict[str, Any] = {}
        self._resource_alerts: Dict[str, Any] = {}
        self._exit_manager = ExitManager()
        self._exit_summary: Dict[str, Any] = _sanitize_exit_payload(None)
        self._exit_lock = threading.Lock()

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
        log.info("TradingRuntime: configuration prepared")
        self.activity.add("config", "ready")
        await self._start_event_bus()
        bus_ok = self.status.event_bus
        detail = "ready" if bus_ok else "degraded"
        self.activity.add("bus", detail, ok=bus_ok)
        log.log(logging.INFO if bus_ok else logging.WARNING, "TradingRuntime: event bus %s", detail)
        await self._start_ui()
        ui_detail = "enabled" if self.ui_server else "disabled"
        self.activity.add("ui", ui_detail, ok=bool(self.ui_server))
        log.info("TradingRuntime: UI %s", ui_detail)
        await self._start_agents()
        self.activity.add("agents", "ready")
        log.info("TradingRuntime: agents ready")
        self._start_rl_status_watcher()
        await self._start_loop()
        loop_state = "pipeline" if self.pipeline is not None else "loop"
        self.activity.add("loop", loop_state)
        log.info("TradingRuntime: trading loop started (%s)", loop_state)
        self.activity.add("runtime", "started")

    async def stop(self) -> None:
        if self.stop_event.is_set() and not self._tasks:
            return
        self.stop_event.set()
        self.activity.add("runtime", "stopping")

        if self._golden_service is not None:
            with contextlib.suppress(Exception):
                await self._golden_service.stop()
            self._golden_service = None

        tasks_to_cancel = list(self._tasks)
        if self.rl_task is not None and self.rl_task not in tasks_to_cancel:
            tasks_to_cancel.append(self.rl_task)

        for task in tasks_to_cancel:
            task.cancel()
        for task in tasks_to_cancel:
            with contextlib.suppress(asyncio.CancelledError):
                await task

        self._tasks = [
            t for t in self._tasks if not t.cancelled() and not t.done()
        ]
        if not self._tasks:
            self._tasks = []
        if self.trading_task is not None:
            if self.trading_task in tasks_to_cancel or self.trading_task.done():
                self.trading_task = None

        for topic, handler in list(self._subscriptions):
            with contextlib.suppress(Exception):
                unsubscribe(topic, handler)
        self._subscriptions.clear()

        if self.bus_started:
            with contextlib.suppress(Exception):
                await stop_ws_server()
            self.bus_started = False
            self.status.event_bus = False

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
        self.status.heartbeat_ts = None
        self._last_heartbeat_perf = None
        self.status.rl_daemon = False

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
        self.ui_state.golden_depth_enabled = resolve_depth_flag(self.cfg)
        self.ui_state.golden_momentum_enabled = resolve_momentum_flag(self.cfg)
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

        self._use_golden_pipeline = bool(resolve_golden_enabled(cfg))

    async def _start_event_bus(self) -> None:
        broker_urls = get_broker_urls(self.cfg) if self.cfg else []
        try:
            ensure_local_redis_if_needed(broker_urls)
        except Exception:
            log.exception("Failed to ensure Redis broker")

        ws_port = int(os.getenv("EVENT_BUS_WS_PORT", "8779") or 8779)
        event_bus_url = f"ws://127.0.0.1:{ws_port}"
        os.environ["EVENT_BUS_URL"] = event_bus_url
        os.environ["BROKER_WS_URLS"] = event_bus_url

        try:
            await start_ws_server("127.0.0.1", ws_port)
            self.bus_started = True
            listen_host, listen_port = get_ws_address()
            event_bus_url = f"ws://{listen_host}:{listen_port}"
            os.environ["EVENT_BUS_URL"] = event_bus_url
            os.environ["BROKER_WS_URLS"] = event_bus_url
            self.activity.add("event_bus", f"listening on {event_bus_url}")
        except Exception as exc:
            self.activity.add("event_bus", f"failed: {exc}", ok=False)
            log.warning(
                "Failed to start event bus websocket; running in degraded mode",
                exc_info=True,
            )

        ok = await verify_broker_connection(timeout=2.0)
        self.status.event_bus = bool(ok and self.bus_started)
        if not ok or not self.bus_started:
            self.activity.add("broker", "verification failed", ok=False)
            log.warning("Event bus verification failed; continuing without bus")
        else:
            self.activity.add("broker", "verified")

        try:
            resource_monitor.start_monitor()
        except Exception:  # pragma: no cover - best effort
            log.debug("Resource monitor start failed", exc_info=True)

        self._subscribe_to_events()

    def _subscribe_to_events(self) -> None:
        def _normalize_event(event: Any) -> Dict[str, Any]:
            payload = getattr(event, "payload", event)
            data = _serialize(payload)
            if isinstance(data, dict):
                return {str(k): v for k, v in data.items()}
            return {}

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

        async def _on_discovery_candidate(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload:
                return
            payload.setdefault("mint", payload.get("token") or payload.get("address"))
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._discovery_candidates.appendleft(payload)

        async def _on_token_snapshot(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._token_facts[str(mint)] = payload

        async def _on_market_ohlcv(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            read_ohlcv(payload, reader="runtime")
            with self._swarm_lock:
                self._market_ohlcv[str(mint)] = payload

        async def _on_market_depth(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._market_depth[str(mint)] = payload

        async def _on_golden_snapshot(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            read_golden(payload, reader="runtime")
            hash_value = payload.get("hash")
            with self._swarm_lock:
                self._golden_snapshots[str(mint)] = payload
                if hash_value:
                    self._latest_golden_hash[str(mint)] = str(hash_value)

        async def _on_suggestion(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._agent_suggestions.appendleft(payload)

        async def _on_suggestion_rejected(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._agent_rejections.appendleft(payload)

        async def _on_vote_decision(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            client_id = payload.get("clientOrderId") or payload.get("client_order_id")
            if client_id:
                with self._swarm_lock:
                    now_ts = time.time()
                    client_id_str = str(client_id)
                    self._decision_recent.append((client_id_str, now_ts))
                    cutoff = now_ts - 300.0
                    while self._decision_recent and self._decision_recent[0][1] < cutoff:
                        old_id, _old_ts = self._decision_recent.popleft()
                        current = self._decision_counts.get(old_id, 0)
                        if current <= 1:
                            self._decision_counts.pop(old_id, None)
                            self._decision_first_seen.pop(old_id, None)
                        else:
                            self._decision_counts[old_id] = current - 1
                    count = self._decision_counts.get(client_id_str, 0) + 1
                    self._decision_counts[client_id_str] = count
                    self._decision_first_seen.setdefault(client_id_str, now_ts)
                    payload["_duplicate_count"] = count
                    payload["_idempotent"] = count <= 1
                    payload["_first_seen"] = self._decision_first_seen.get(client_id_str)
                    self._vote_decisions.appendleft(payload)
                    cap = _int_env("VOTE_IDEMPOTENCY_CAP", 20000)
                    excess = len(self._decision_counts) - cap
                    if excess > 0:
                        oldest = sorted(
                            self._decision_first_seen.items(), key=lambda kv: kv[1]
                        )[:excess]
                        for key, _ts in oldest:
                            self._decision_counts.pop(key, None)
                            self._decision_first_seen.pop(key, None)
            else:
                with self._swarm_lock:
                    self._vote_decisions.appendleft(payload)

        async def _on_virtual_fill(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload.get("mint"):
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._virtual_fills.appendleft(payload)

        async def _on_live_fill(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload.get("mint"):
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._live_fills.appendleft(payload)

        async def _on_rl_weights(event: Any) -> None:
            payload = _normalize_event(event)
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._rl_weights_windows.appendleft(payload)
                vote_window = payload.get("vote_window_ms")
                if vote_window is not None:
                    try:
                        window_ms = float(vote_window)
                    except Exception:
                        window_ms = None
                    if window_ms:
                        self._rl_window_sec = max(window_ms / 1000.0, 0.1)
                        self._rl_status_info["vote_window_ms"] = window_ms
            if not self._rl_gate_active:
                reason = self._rl_gate_reason or "unknown"
                message = "Skipping RL weights while gate is closed (reason=%s)"
                if reason in {"stale_heartbeat", "rl_health"}:
                    log.info("stale RL ignored (reason=%s)", reason)
                else:
                    log.debug(message, reason)
                return
            if self.pipeline is not None:
                try:
                    self.pipeline.set_rl_weights(payload)
                except Exception:
                    log.exception("Failed to forward RL weights to pipeline")

        async def _on_resource_update(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload:
                return
            payload["_received"] = time.time()
            self._resource_snapshot = payload

        async def _on_resource_alert(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload:
                return
            payload["_received"] = time.time()
            resource_name = str(payload.get("resource") or payload.get("name") or "resource")
            self._resource_alerts[resource_name] = payload

        self._subscriptions.append(("action_executed", _on_action))
        subscribe("action_executed", _on_action)
        self._subscriptions.append(("runtime.log", _on_log))
        subscribe("runtime.log", _on_log)
        for topic, handler in (
            ("x:discovery.candidates", _on_discovery_candidate),
            ("x:token.snap", _on_token_snapshot),
            ("x:market.ohlcv.5m", _on_market_ohlcv),
            ("x:market.depth", _on_market_depth),
            ("x:mint.golden", _on_golden_snapshot),
            ("x:trade.suggested", _on_suggestion),
            ("x:trade.rejected", _on_suggestion_rejected),
            ("x:vote.decisions", _on_vote_decision),
            ("x:virt.fills", _on_virtual_fill),
            ("x:live.fills", _on_live_fill),
            ("rl:weights.applied", _on_rl_weights),
            ("resource_update", _on_resource_update),
            ("resource_alert", _on_resource_alert),
        ):
            self._subscriptions.append((topic, handler))
            subscribe(topic, handler)

    async def _start_ui(self) -> None:
        self.ui_state.status_provider = self._collect_status
        self.ui_state.activity_provider = self.activity.snapshot
        self.ui_state.trades_provider = lambda: list(self._trades)
        self.ui_state.weights_provider = self._collect_weights
        self.ui_state.rl_status_provider = self._collect_rl_status
        self.ui_state.logs_provider = lambda: list(self._ui_logs)
        self.ui_state.summary_provider = self._collect_summary
        self.ui_state.discovery_provider = self._collect_discovery
        self.ui_state.config_provider = self._collect_config
        self.ui_state.actions_provider = self._collect_actions
        self.ui_state.history_provider = self._collect_history
        self.ui_state.discovery_console_provider = self._collect_discovery_console
        self.ui_state.token_facts_provider = self._collect_token_facts_panel
        self.ui_state.market_state_provider = self._collect_market_panel
        self.ui_state.golden_snapshot_provider = self._collect_golden_panel
        self.ui_state.suggestions_provider = self._collect_suggestions_panel
        self.ui_state.vote_windows_provider = self._collect_vote_panel
        self.ui_state.shadow_provider = self._collect_shadow_panel
        self.ui_state.rl_provider = self._collect_rl_panel
        self.ui_state.settings_provider = self._collect_settings_panel
        self.ui_state.exit_provider = self._collect_exit_panel
        self.ui_state.health_provider = self._collect_health_metrics

        if not self._ui_enabled:
            self.ui_server = None
            log.info("TradingRuntime: UI disabled via UI_ENABLED")
            return

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
        self._rl_window_sec = float(getattr(self.agent_manager, "_rl_window_sec", 0.4) or 0.4)
        self.agent_manager.set_rl_disabled(True, reason="boot")

        fast_mode = os.getenv("FAST_PIPELINE_MODE", "").lower() in {"1", "true", "yes", "on"}
        if self._use_new_pipeline:
            log.info(
                "TradingRuntime: initialising staged pipeline (fast_mode=%s)",
                fast_mode,
            )
            discovery_interval_cfg = float(self.cfg.get("discovery_interval", 5.0) or 5.0)
            if fast_mode:
                fast_interval = float(os.getenv("FAST_DISCOVERY_INTERVAL", "0.75") or 0.75)
                discovery_interval_cfg = min(discovery_interval_cfg, fast_interval)
                min_interval = float(os.getenv("FAST_DISCOVERY_MIN_INTERVAL", "0.25") or 0.25)
            else:
                min_interval = 5.0
            discovery_interval_cfg = max(discovery_interval_cfg, min_interval)

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

            scoring_batch = self._determine_scoring_batch(fast_mode)
            evaluation_workers = self._determine_eval_workers(fast_mode)
            execution_lanes = self._determine_execution_lanes(fast_mode)
            # FAST_PIPELINE_MODE clamps discovery cadence and worker counts for CI/load shedding.
            self.pipeline = PipelineCoordinator(
                self.agent_manager,
                self.portfolio,
                discovery_interval=discovery_interval_cfg,
                discovery_cache_ttl=discovery_cache_ttl,
                scoring_batch=scoring_batch,
                evaluation_cache_ttl=evaluation_cache_ttl,
                evaluation_workers=evaluation_workers,
                execution_lanes=execution_lanes,
                on_evaluation=self._pipeline_on_evaluation,
                on_execution=self._pipeline_on_execution,
            )
            log.info(
                "TradingRuntime: pipeline created; evaluations will run through AgentManager swarm"
            )
            if fast_mode:
                publish(
                    "runtime.log",
                    {
                        "stage": "pipeline",
                        "detail": "FAST mode clamp",
                        "discovery_interval_s": discovery_interval_cfg,
                        "evaluation_workers": evaluation_workers,
                        "execution_lanes": execution_lanes,
                        "scoring_batch": scoring_batch,
                    },
                )

        if self._use_golden_pipeline and self.agent_manager is not None and self.portfolio is not None:
            try:
                from ..golden_pipeline.service import GoldenPipelineService

                self._golden_service = GoldenPipelineService(
                    agent_manager=self.agent_manager,
                    portfolio=self.portfolio,
                    config=self.cfg,
                )
                await self._golden_service.start()
                self.activity.add("golden_pipeline", "enabled")
                log.info("TradingRuntime: Golden pipeline service started")
            except Exception:
                log.exception("Failed to start Golden pipeline service")
                self.activity.add("golden_pipeline", "failed", ok=False)

        self._set_rl_gate_active(False, reason="boot")

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
            self._register_task(self.rl_task)
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
        self._register_task(task)

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
                "status": info.get("status"),
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

            hb_ts = info.get("heartbeat_ts")
            if hb_ts is not None:
                try:
                    updates["heartbeat_ts"] = float(hb_ts)
                except Exception:
                    pass
            hb_age = info.get("heartbeat_age")
            if hb_age is not None:
                try:
                    updates["heartbeat_age"] = float(hb_age)
                except Exception:
                    pass
            status_text = str(info.get("status") or "").lower()
            window = max(self._rl_window_sec * 2.0, 0.1)
            health_ok = bool(updates.get("running"))
            if status_text and status_text not in {"running", "ok"}:
                health_ok = False
            try:
                if float(updates.get("heartbeat_age", 0.0)) > window:
                    health_ok = False
            except Exception:
                pass
            updates["health_ok"] = health_ok

            internal_age = self._internal_heartbeat_age()
            if internal_age is not None:
                existing_age = updates.get("heartbeat_age")
                try:
                    existing_val = (
                        None if existing_age is None else float(existing_age)
                    )
                except Exception:
                    existing_val = None
                if existing_val is None or internal_age > existing_val:
                    updates["heartbeat_age"] = internal_age

            self._rl_status_info.update(updates)
            self.status.rl_daemon = bool(self._rl_status_info.get("running"))
            self._update_rl_gate()

            jitter = 1.0 + (0.30 * (os.urandom(1)[0] / 255.0 - 0.5))
            sleep_for = max(0.2, min(float(_RL_HEALTH_MAX_INTERVAL), delay * jitter))
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=sleep_for)
            except asyncio.TimeoutError:
                continue
            else:
                break

    def _rl_gate_allows(self) -> bool:
        if not self._rl_status_info.get("running"):
            return False
        age = self._effective_heartbeat_age()
        if age is None:
            return False
        window = max(self._rl_window_sec * 2.0, 0.1)
        return age <= window

    def _internal_heartbeat_age(self) -> Optional[float]:
        epoch_age: Optional[float] = None
        if self.status.heartbeat_ts is not None:
            try:
                epoch_age = max(0.0, time.time() - float(self.status.heartbeat_ts))
            except Exception:
                epoch_age = None
        perf_age: Optional[float] = None
        if self._last_heartbeat_perf is not None:
            try:
                perf_age = max(0.0, time.perf_counter() - self._last_heartbeat_perf)
            except Exception:
                perf_age = None
        candidates = [value for value in (epoch_age, perf_age) if value is not None]
        if not candidates:
            return None
        return max(candidates)

    def _effective_heartbeat_age(self) -> Optional[float]:
        internal_age = self._internal_heartbeat_age()
        external_raw = self._rl_status_info.get("heartbeat_age")
        external_age: Optional[float]
        try:
            external_age = None if external_raw is None else max(0.0, float(external_raw))
        except Exception:
            external_age = None
        candidates = [value for value in (internal_age, external_age) if value is not None]
        if not candidates:
            return None
        return max(candidates)

    def _set_rl_gate_active(self, active: bool, *, reason: Optional[str] = None) -> None:
        reason_text = None if active else (reason or self._rl_gate_reason or "unhealthy")
        if (
            self._rl_gate_active == active
            and (active or self._rl_gate_reason == reason_text)
        ):
            return

        self._rl_gate_active = active
        self._rl_gate_reason = None if active else reason_text
        gate_state = "open" if active else "blocked"
        self._rl_status_info["gate"] = gate_state
        self._rl_status_info["gate_reason"] = self._rl_gate_reason

        if self.pipeline is not None:
            self.pipeline.set_rl_enabled(active)
        if self.agent_manager is not None:
            self.agent_manager.set_rl_disabled(not active, reason=reason_text)

        if active:
            log.info("RL weights gate opened")
        else:
            log.warning("RL weights gate closed (%s)", reason_text or "unknown")

    def _update_rl_gate(self) -> None:
        if not self._rl_status_info.get("enabled"):
            self._set_rl_gate_active(False, reason="disabled")
            return
        if not self._rl_status_info.get("running"):
            self._set_rl_gate_active(False, reason="rl_unhealthy")
            return
        if self._rl_status_info.get("health_ok") is False:
            self._set_rl_gate_active(False, reason="rl_health")
            return
        window = max(self._rl_window_sec * 2.0, 0.1)
        hb_age = self._effective_heartbeat_age()
        if hb_age is None:
            self._set_rl_gate_active(False, reason="no_heartbeat")
            return
        if hb_age > window:
            self._set_rl_gate_active(False, reason="stale_heartbeat")
            return
        self._set_rl_gate_active(True)

    async def _start_loop(self) -> None:
        if self._use_new_pipeline and self.pipeline is not None:
            await self.pipeline.start()
            self.status.trading_loop = True
            poller = asyncio.create_task(self._pipeline_telemetry_poller(), name="pipeline_telemetry")
            self._register_task(poller)
        else:
            task = asyncio.create_task(self._trading_loop(), name="trading_loop")
            self.trading_task = task
            self._register_task(task)

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
        self._loop_delay = loop_delay
        self._loop_min_delay = min_delay
        self._loop_max_delay = max_delay
        self._heartbeat_budget = max(loop_delay * 2.0, 30.0)
        discovery_method = resolve_discovery_method(self.cfg.get("discovery_method"))
        if discovery_method is None:
            discovery_method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        if discovery_method is None:
            discovery_method = DEFAULT_DISCOVERY_METHOD
        stop_loss = _maybe_float(self.cfg.get("stop_loss"))
        take_profit = _maybe_float(self.cfg.get("take_profit"))
        trailing_stop = _maybe_float(self.cfg.get("trailing_stop"))
        max_drawdown = _maybe_float(self.cfg.get("max_drawdown"), 1.0)
        volatility_factor = _maybe_float(self.cfg.get("volatility_factor"), 1.0)
        arbitrage_threshold = _maybe_float(self.cfg.get("arbitrage_threshold"), 0.0)
        arbitrage_amount = _maybe_float(self.cfg.get("arbitrage_amount"), 0.0)
        live_discovery = self.cfg.get("live_discovery")

        throttle_logged = False
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
            exit_candidates = resource_monitor.active_budget("exit")
            throttle_candidates = resource_monitor.active_budget("throttle")
            exit_policy = exit_candidates[0] if exit_candidates else None
            throttle_policy = throttle_candidates[0] if throttle_candidates else None
            if exit_policy:
                resource = str(exit_policy.get("resource") or "resource")
                self.activity.add("loop", f"resource exit {resource}", ok=False)
                publish(
                    "runtime.log",
                    RuntimeLog(stage="loop", detail=f"resource-exit:{resource}", level="ERROR"),
                )
                log.error(
                    "Resource budget triggered shutdown action for %s: %s",
                    resource,
                    exit_policy,
                )
                self.stop_event.set()
                break
            throttled = bool(throttle_policy)
            if throttled:
                sleep_for = max(sleep_for, max_delay)
                if not throttle_logged:
                    publish(
                        "runtime.log",
                        RuntimeLog(stage="loop", detail="resource-throttle", level="WARN"),
                    )
                    log.warning(
                        "Resource budget throttle active; sleeping for %.2fs",
                        sleep_for,
                    )
                    throttle_logged = True
            else:
                throttle_logged = False
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
        activity_entries = self.activity.snapshot()
        status = {
            "event_bus": self.status.event_bus,
            "trading_loop": self.status.trading_loop,
            "depth_service": depth_ok,
            "rl_daemon": rl_running,
            "rl_daemon_status": rl_snapshot,
            "heartbeat": self.status.heartbeat_ts,
            "iterations_completed": self._iteration_count,
            "trade_count": len(self._trades),
            "activity_count": len(activity_entries),
        }
        heartbeat_ts = self.status.heartbeat_ts
        latency_ms: Optional[float] = None
        if self._last_heartbeat_perf is not None:
            try:
                latency_ms = max(
                    0.0, (time.perf_counter() - self._last_heartbeat_perf) * 1000.0
                )
            except Exception:
                latency_ms = None
        if latency_ms is None and heartbeat_ts is not None:
            try:
                latency_ms = max(0.0, time.time() - float(heartbeat_ts)) * 1000.0
            except Exception:
                latency_ms = None
        if latency_ms is not None:
            status["bus_latency_ms"] = latency_ms
        env_label = (
            os.getenv("SOLHUNTER_ENV")
            or os.getenv("DEPLOY_ENV")
            or os.getenv("RUNTIME_ENV")
            or os.getenv("APP_ENV")
            or os.getenv("ENVIRONMENT")
            or os.getenv("ENV")
        )
        if env_label:
            status["environment"] = str(env_label)
        pause_state = self._control_states.get("control:execution:paused", {})
        paper_state = self._control_states.get("control:execution:paper_only", {})
        rl_toggle = self._control_states.get("RL_WEIGHTS_DISABLED", {})
        status["paused"] = _control_active(pause_state)
        status["paper_mode"] = _control_active(paper_state)
        status["rl_mode"] = "shadow" if _control_active(rl_toggle) else "applied"
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
        status["rl_gate"] = rl_snapshot.get("gate")
        status["rl_gate_reason"] = rl_snapshot.get("gate_reason")
        return status

    def _collect_health_metrics(self) -> Dict[str, Any]:
        budgets = resource_monitor.get_budget_status()
        heartbeat_age = self._internal_heartbeat_age()
        heartbeat_threshold = max(self._loop_delay * 2.0, 30.0)
        heartbeat_ok = heartbeat_age is None or heartbeat_age <= heartbeat_threshold
        exit_active = any(
            info.get("active") and str(info.get("action")).lower() == "exit"
            for info in budgets.values()
        )
        throttle_active = any(
            info.get("active") and str(info.get("action")).lower() == "throttle"
            for info in budgets.values()
        )
        queue_stats: Dict[str, Any] = {}
        if self.pipeline is not None:
            try:
                queue_stats = self.pipeline.queue_snapshot()
            except Exception:
                queue_stats = {}
        try:
            from ..ui import get_ws_client_counts
        except Exception:  # pragma: no cover - optional during CI
            ws_clients: Dict[str, int] = {}
        else:
            try:
                ws_clients = get_ws_client_counts()
            except Exception:
                ws_clients = {}
        resource_snapshot = dict(self._resource_snapshot)
        resource_alerts = {name: dict(info) for name, info in self._resource_alerts.items()}
        ok = bool(self.status.event_bus) and heartbeat_ok and not exit_active
        payload: Dict[str, Any] = {
            "ok": ok,
            "event_bus": {"connected": bool(self.status.event_bus)},
            "heartbeat": {
                "age": heartbeat_age,
                "threshold": heartbeat_threshold,
                "ok": heartbeat_ok,
            },
            "queues": queue_stats,
            "loop": {
                "delay": self._loop_delay,
                "min_delay": self._loop_min_delay,
                "max_delay": self._loop_max_delay,
            },
            "resource": {
                "budgets": budgets,
                "alerts": resource_alerts,
                "latest": resource_snapshot,
                "exit_active": exit_active,
                "throttle_active": throttle_active,
            },
            "ui": {"ws_clients": ws_clients},
        }
        return payload

    def _collect_weights(self) -> Dict[str, Any]:
        if self.agent_manager is None:
            return {}
        snapshot = getattr(self.agent_manager, "weight_snapshot", None)
        if callable(snapshot):
            try:
                return snapshot()
            except Exception:
                pass
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
        snapshot.setdefault("gate", "open" if self._rl_gate_active else "blocked")
        snapshot.setdefault("gate_reason", self._rl_gate_reason)
        effective_age = self._effective_heartbeat_age()
        if effective_age is not None:
            snapshot["heartbeat_age"] = effective_age
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

    def _collect_summary(self) -> Dict[str, Any]:
        iteration = self._collect_iteration()
        evaluation = dict(self._last_eval_summary)
        execution = dict(self._last_execution_summary)
        queues: Dict[str, Any] = {}
        if self.pipeline is not None:
            try:
                queues = self.pipeline.queue_snapshot()
            except Exception:
                queues = {}
        tuning: List[Dict[str, Any]] = []
        if evaluation:
            meta = evaluation.get("metadata")
            if isinstance(meta, dict):
                try:
                    tuning = analyse_evaluation(meta)
                except Exception:
                    tuning = []
        return {
            "iteration": iteration,
            "evaluation": evaluation,
            "execution": execution,
            "queues": queues,
            "tuning": tuning,
        }

    def _collect_discovery_console(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            rows = list(self._discovery_candidates)
        seen: set[str] = set()
        candidates: List[Dict[str, Any]] = []
        for row in rows:
            mint_raw = row.get("mint") or row.get("token") or row.get("address")
            mint = str(mint_raw).strip() if mint_raw else ""
            if not mint or mint in seen:
                continue
            seen.add(mint)
            timestamp = _entry_timestamp(row, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and row.get("_received") is not None:
                try:
                    age = max(0.0, now - float(row["_received"]))
                except Exception:
                    age = None
            stale = age is not None and age > 120.0
            candidates.append(
                {
                    "mint": mint,
                    "score": _maybe_float(row.get("score")),
                    "source": row.get("source"),
                    "asof": row.get("asof"),
                    "age_seconds": age,
                    "age_label": _format_age(age),
                    "stale": stale,
                }
            )
        return {
            "candidates": candidates[:100],
            "stats": {"total": len(rows)},
        }

    def _collect_token_facts_panel(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            facts = dict(self._token_facts)
        ordered: Dict[str, Dict[str, Any]] = {}
        for mint in sorted(facts.keys()):
            payload = dict(facts[mint])
            timestamp = _entry_timestamp(payload, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            missing_meta = not payload.get("symbol") or not payload.get("name")
            stale = (age is not None and age > 300.0) or missing_meta
            ordered[mint] = {
                "symbol": payload.get("symbol"),
                "name": payload.get("name"),
                "decimals": payload.get("decimals"),
                "token_program": payload.get("token_program"),
                "flags": payload.get("flags") or [],
                "venues": payload.get("venues") or [],
                "asof": payload.get("asof"),
                "age_seconds": age,
                "age_label": _format_age(age),
                "stale": stale,
            }
        return {"tokens": ordered, "selected": None}

    def _collect_market_panel(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            ohlcv = dict(self._market_ohlcv)
            depth = dict(self._market_depth)
        markets: List[Dict[str, Any]] = []
        ohlcv_lags: List[float] = []
        depth_lags: List[float] = []
        for mint in sorted(set(list(ohlcv.keys()) + list(depth.keys()))):
            candle = dict(ohlcv.get(mint) or {})
            depth_entry = dict(depth.get(mint) or {})
            ts_close = _entry_timestamp(candle, "asof_close")
            ts_depth = _entry_timestamp(depth_entry, "asof")
            age_close = _age_seconds(ts_close, now)
            age_depth = _age_seconds(ts_depth, now)
            if age_close is None and candle.get("_received") is not None:
                try:
                    age_close = max(0.0, now - float(candle["_received"]))
                except Exception:
                    age_close = None
            if age_depth is None and depth_entry.get("_received") is not None:
                try:
                    age_depth = max(0.0, now - float(depth_entry["_received"]))
                except Exception:
                    age_depth = None
            depth_pct_raw = depth_entry.get("depth_pct") or depth_entry.get("depth") or {}
            depth_pct: Dict[str, Optional[float]] = {}
            if isinstance(depth_pct_raw, dict):
                for key, value in depth_pct_raw.items():
                    depth_pct[str(key).strip("% ")] = _maybe_float(value)
            depth_pct = {k: v for k, v in depth_pct.items() if v is not None}
            combined_age = None
            for value in (age_close, age_depth):
                if value is None:
                    continue
                if combined_age is None or value < combined_age:
                    combined_age = value
            stale = False
            if age_close is not None:
                ohlcv_lags.append(age_close * 1000.0)
            if age_depth is not None:
                depth_lags.append(age_depth * 1000.0)
            if age_close is not None and age_close > 120.0:
                stale = True
            if age_depth is not None and age_depth > 6.0:
                stale = True
            normalized_candle = read_ohlcv(candle, reader="runtime")
            close_value = normalized_candle.get("close")
            volume_value = normalized_candle.get("volume_usd")
            volume_base_value = normalized_candle.get("volume_base")
            buyers_value = normalized_candle.get("buyers")
            if buyers_value is None:
                buyers_value = _maybe_int(depth_entry.get("buyers"))
            sellers_value = normalized_candle.get("sellers")
            if sellers_value is None:
                sellers_value = _maybe_int(candle.get("seller_count"))
            if sellers_value is None:
                sellers_value = _maybe_int(candle.get("sellers"))
            if sellers_value is None:
                sellers_value = _maybe_int(depth_entry.get("sellers"))
            markets.append(
                {
                    "mint": mint,
                    "close": close_value,
                    "volume": volume_value,
                    "spread_bps": _maybe_float(depth_entry.get("spread_bps")),
                    "depth_pct": depth_pct,
                    "age_close": age_close,
                    "age_depth": age_depth,
                    "lag_close_ms": age_close * 1000.0 if age_close is not None else None,
                    "lag_depth_ms": age_depth * 1000.0 if age_depth is not None else None,
                    "stale": stale,
                    "updated_label": _format_age(combined_age),
                    "buyers": buyers_value,
                    "sellers": sellers_value,
                    "volume_base": volume_base_value,
                }
            )
        summary = {
            "ohlcv_ms": max(ohlcv_lags) if ohlcv_lags else None,
            "depth_ms": max(depth_lags) if depth_lags else None,
        }
        return {"markets": markets, "updated_at": None, "lag_ms": summary}

    def _collect_golden_panel(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            golden = dict(self._golden_snapshots)
            hash_map = dict(self._latest_golden_hash)
        snapshots: List[Dict[str, Any]] = []
        lag_samples: List[float] = []

        def _normalize_momentum_breakdown(value: Any) -> Any:
            if isinstance(value, bool) or value is None:
                return value
            if isinstance(value, Mapping):
                return {
                    str(key): _normalize_momentum_breakdown(val)
                    for key, val in value.items()
                }
            if isinstance(value, (list, tuple, set)):
                return [_normalize_momentum_breakdown(val) for val in value]
            numeric = _maybe_float(value)
            if numeric is not None:
                return numeric
            return value

        def _as_bool(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
            if isinstance(value, (int, float)):
                return bool(value)
            return False

        for mint in sorted(golden.keys()):
            payload = dict(golden[mint])
            normalized_golden = read_golden(payload, reader="runtime")
            timestamp = _entry_timestamp(payload, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            hash_value = payload.get("hash")
            hash_text = str(hash_value) if hash_value is not None else None
            if age is not None:
                lag_samples.append(age * 1000.0)
            coalesce = payload.get("coalesce_window_s")
            if coalesce is None:
                coalesce = payload.get("coalesce_s")
            if coalesce is None:
                window_ms = payload.get("coalesce_ms")
                if window_ms is not None:
                    try:
                        coalesce = float(window_ms) / 1000.0
                    except Exception:
                        coalesce = None
            if coalesce is None:
                try:
                    coalesce = float(payload.get("coalesce"))
                except Exception:
                    coalesce = None
            stale_threshold = None
            if coalesce is not None:
                try:
                    stale_threshold = max(0.0, float(coalesce) * 2.0 + 5.0)
                except Exception:
                    stale_threshold = None
                else:
                    stale_threshold = min(600.0, max(5.0, stale_threshold))
            stale_flag = False
            if age is not None:
                if stale_threshold is not None:
                    stale_flag = age > stale_threshold
                else:
                    stale_flag = age > 60.0
            momentum_score = _maybe_float(payload.get("momentum_score"))
            pump_intensity = _maybe_float(payload.get("pump_intensity"))
            pump_score = _maybe_float(payload.get("pump_score"))
            if pump_intensity is None and pump_score is not None:
                pump_intensity = pump_score
            social_score = _maybe_float(payload.get("social_score"))
            social_sentiment = _maybe_float(payload.get("social_sentiment"))
            tweets_per_min = _maybe_float(payload.get("tweets_per_min"))
            buyers_last_hour = _maybe_int(payload.get("buyers_last_hour"))
            momentum_latency = _maybe_float(payload.get("momentum_latency_ms"))
            raw_sources = payload.get("momentum_sources")
            if isinstance(raw_sources, (list, tuple, set)):
                candidate_sources = list(raw_sources)
            elif isinstance(raw_sources, str):
                candidate_sources = [raw_sources]
            else:
                candidate_sources = []
            momentum_sources: List[str] = []
            seen_sources: set[str] = set()
            for source in candidate_sources:
                if source is None:
                    continue
                source_text = str(source).strip()
                if not source_text or source_text in seen_sources:
                    continue
                seen_sources.add(source_text)
                momentum_sources.append(source_text)
            raw_breakdown = payload.get("momentum_breakdown")
            momentum_breakdown: Dict[str, Any] = {}
            if isinstance(raw_breakdown, Mapping):
                momentum_breakdown = {
                    str(key): _normalize_momentum_breakdown(value)
                    for key, value in raw_breakdown.items()
                }
            momentum_stale = _as_bool(payload.get("momentum_stale"))
            momentum_partial = _as_bool(payload.get("momentum_partial"))
            snapshots.append(
                {
                    "mint": mint,
                    "hash": hash_text,
                    "hash_short": _short_hash(hash_text),
                    "px": normalized_golden.get("mid_usd"),
                    "liq": normalized_golden.get("depth_1pct_usd"),
                    "age_seconds": age,
                    "age_label": _format_age(age),
                    "stale": stale_flag,
                    "coalesce_window_s": coalesce,
                    "lag_ms": age * 1000.0 if age is not None else None,
                    "stale_threshold_s": stale_threshold,
                    "momentum_score": momentum_score,
                    "momentum_stale": momentum_stale,
                    "momentum_partial": momentum_partial,
                    "momentum_sources": momentum_sources,
                    "momentum_breakdown": momentum_breakdown,
                    "pump_intensity": pump_intensity,
                    "pump_score": pump_score,
                    "social_score": social_score,
                    "social_sentiment": social_sentiment,
                    "tweets_per_min": tweets_per_min,
                    "buyers_last_hour": buyers_last_hour,
                    "momentum_latency_ms": momentum_latency,
                }
            )
        return {
            "snapshots": snapshots,
            "hash_map": hash_map,
            "lag_ms": max(lag_samples) if lag_samples else None,
        }

    def _collect_suggestions_panel(self) -> Dict[str, Any]:
        now = time.time()
        window = 300.0
        with self._swarm_lock:
            suggestions = list(self._agent_suggestions)
            golden_hashes = dict(self._latest_golden_hash)
            decisions = list(self._vote_decisions)
            rejections = list(self._agent_rejections)
        items: List[Dict[str, Any]] = []
        recent_count = 0
        for payload in suggestions:
            mint = payload.get("mint")
            if not mint:
                continue
            timestamp = _entry_timestamp(payload, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            ttl = _maybe_float(payload.get("ttl_sec"))
            remaining = None
            if ttl is not None and age is not None:
                remaining = max(0.0, ttl - age)
            stale = False
            if ttl is not None and age is not None:
                stale = age > ttl
            elif age is not None and age > window:
                stale = True
            inputs_hash = payload.get("inputs_hash")
            golden_hash = golden_hashes.get(str(mint))
            mismatch = bool(inputs_hash and golden_hash and str(inputs_hash) != golden_hash)
            gating = payload.get("gating") or {}
            breakeven = _maybe_float(gating.get("breakeven_bps"))
            edge_buffer = _maybe_float(gating.get("edge_buffer_bps"))
            expected_edge = _maybe_float(gating.get("expected_edge_bps"))
            raw_edge_pass = gating.get("edge_pass")
            edge_pass = raw_edge_pass if isinstance(raw_edge_pass, bool) else None
            items.append(
                {
                    "agent": payload.get("agent"),
                    "mint": mint,
                    "side": (payload.get("side") or "").lower() or None,
                    "notional_usd": _maybe_float(payload.get("notional_usd")),
                    "edge": _maybe_float(payload.get("edge")),
                    "breakeven_bps": breakeven,
                    "edge_buffer_bps": edge_buffer,
                    "expected_edge_bps": expected_edge,
                    "edge_pass": edge_pass,
                    "risk": payload.get("risk") or {},
                    "max_slippage_bps": _maybe_float(payload.get("max_slippage_bps")),
                    "inputs_hash": inputs_hash,
                    "inputs_hash_short": _short_hash(inputs_hash),
                    "golden_hash": golden_hash,
                    "golden_hash_short": _short_hash(golden_hash),
                    "ttl_label": _format_ttl(remaining, ttl),
                    "ttl_seconds": ttl,
                    "age_label": _format_age(age),
                    "stale": stale,
                    "must": bool(payload.get("must")),
                    "age_seconds": age,
                    "hash_mismatch": mismatch,
                    "gating": gating,
                }
            )
            if age is not None and age <= window:
                recent_count += 1
        rejection_items: List[Dict[str, Any]] = []
        recent_rejections = 0
        for payload in rejections:
            mint = payload.get("mint")
            if not mint:
                continue
            ts = _entry_timestamp(payload, "detected_at")
            if ts is None:
                ts = _entry_timestamp(payload, "asof")
            age = _age_seconds(ts, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            if age is not None and age <= window:
                recent_rejections += 1
            rejection_items.append(
                {
                    "agent": payload.get("agent"),
                    "source_agent": payload.get("source_agent"),
                    "mint": mint,
                    "side": (payload.get("side") or "").lower() or None,
                    "notional_usd": _maybe_float(payload.get("notional_usd")),
                    "requested_notional_usd": _maybe_float(
                        payload.get("requested_notional_usd")
                    ),
                    "reason": payload.get("reason"),
                    "reason_code": payload.get("reason_code"),
                    "guard": payload.get("guard"),
                    "scope": payload.get("scope"),
                    "cap_usd": _maybe_float(payload.get("cap_usd")),
                    "available_notional_usd": _maybe_float(
                        payload.get("available_notional_usd")
                    ),
                    "max_slippage_bps": _maybe_float(payload.get("max_slippage_bps")),
                    "guard_slippage_bps": _maybe_float(
                        payload.get("guard_slippage_bps")
                    ),
                    "snapshot_hash": payload.get("snapshot_hash"),
                    "snapshot_hash_short": _short_hash(payload.get("snapshot_hash")),
                    "age_label": _format_age(age),
                    "age_seconds": age,
                    "detected_at": payload.get("detected_at"),
                    "gating": payload.get("gating") or {},
                    "depth_points": payload.get("depth_points") or [],
                }
            )
        latest_age = None
        for item in items:
            age = item.get("age_seconds")
            if age is None:
                continue
            if latest_age is None or age < latest_age:
                latest_age = age
        recent_decisions = 0
        for decision in decisions:
            ts = _entry_timestamp(decision, "ts")
            age = _age_seconds(ts, now)
            if age is None and decision.get("_received") is not None:
                try:
                    age = max(0.0, now - float(decision["_received"]))
                except Exception:
                    age = None
            if age is not None and age <= window:
                recent_decisions += 1
        acceptance = 0.0
        if recent_count:
            acceptance = min(1.0, recent_decisions / max(recent_count, 1))
        metrics = {
            "count": len(items),
            "rate_per_min": recent_count / max(window / 60.0, 1e-6),
            "acceptance_rate": acceptance,
            "updated_label": _format_age(latest_age),
            "stale": latest_age is not None and latest_age > window,
            "golden_tracked": len(golden_hashes),
            "rejections": len(rejection_items),
            "rejections_rate_per_min": recent_rejections / max(window / 60.0, 1e-6),
        }
        return {
            "suggestions": items[:200],
            "rejections": rejection_items[:200],
            "metrics": metrics,
        }

    def _collect_vote_panel(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            suggestions = list(self._agent_suggestions)
            decisions = list(self._vote_decisions)
        window_ms = os.getenv("VOTE_WINDOW_MS", "15000")
        try:
            window_duration = float(window_ms) / 1000.0
        except Exception:
            window_duration = 15.0
        windows_map: Dict[tuple[str, str, Any], Dict[str, Any]] = {}
        for suggestion in suggestions:
            mint = suggestion.get("mint")
            if not mint:
                continue
            side = (suggestion.get("side") or "").lower()
            key = (str(mint), side, suggestion.get("inputs_hash"))
            bucket = windows_map.setdefault(
                key,
                {
                    "mint": str(mint),
                    "side": side or "buy",
                    "quorum": 0,
                    "scores": [],
                    "first_ts": None,
                    "hash": suggestion.get("inputs_hash"),
                    "decision": None,
                },
            )
            bucket["quorum"] += 1
            score = _maybe_float(suggestion.get("edge"))
            if score is not None:
                bucket["scores"].append(score)
            ts = _entry_timestamp(suggestion, "asof")
            if ts is None and suggestion.get("_received") is not None:
                try:
                    ts = datetime.fromtimestamp(float(suggestion["_received"]), tz=timezone.utc)
                except Exception:
                    ts = None
            if ts is not None:
                current = bucket.get("first_ts")
                if current is None or ts < current:
                    bucket["first_ts"] = ts
        for decision in decisions:
            mint = decision.get("mint")
            if not mint:
                continue
            side = (decision.get("side") or "").lower()
            snapshot_hash = decision.get("snapshot_hash")
            key = (str(mint), side, snapshot_hash)
            if key in windows_map:
                windows_map[key]["decision"] = decision
        windows: List[Dict[str, Any]] = []
        for data in windows_map.values():
            first_ts = data.get("first_ts")
            age = _age_seconds(first_ts, now)
            if age is None:
                age = 0.0
            countdown = max(0.0, window_duration - age)
            decision = data.get("decision")
            expired = age > window_duration and not decision
            if data["scores"]:
                score_value = sum(data["scores"]) / max(len(data["scores"]), 1)
            elif decision is not None:
                score_value = _maybe_float(decision.get("score"))
            else:
                score_value = None
            idempotent = True
            if decision is not None:
                idempotent = bool(decision.get("_idempotent", False))
            windows.append(
                {
                    "mint": data["mint"],
                    "side": data["side"],
                    "quorum": data["quorum"],
                    "score": score_value,
                    "countdown": countdown,
                    "countdown_label": _format_countdown(countdown),
                    "expired": expired,
                    "idempotent": idempotent,
                    "idempotency_label": "unique ✔" if idempotent else "dup ⛔",
                }
            )
        windows.sort(key=lambda item: item.get("countdown", 0.0))

        tape: List[Dict[str, Any]] = []
        for decision in decisions:
            ts = _entry_timestamp(decision, "ts")
            age = _age_seconds(ts, now)
            if age is None and decision.get("_received") is not None:
                try:
                    age = max(0.0, now - float(decision["_received"]))
                except Exception:
                    age = None
            client_id = decision.get("clientOrderId") or decision.get("client_order_id")
            seen_ts = decision.get("_first_seen")
            seen_age = _age_seconds(
                datetime.fromtimestamp(seen_ts, tz=timezone.utc) if seen_ts else None,
                now,
            )
            idempotent = bool(decision.get("_idempotent", False))
            tape.append(
                {
                    "mint": decision.get("mint"),
                    "side": (decision.get("side") or "").lower() or None,
                    "client_order_id": client_id,
                    "notional_usd": _maybe_float(decision.get("notional_usd")),
                    "score": _maybe_float(decision.get("score")),
                    "age_label": _format_age(age),
                    "duplicate": bool(decision.get("_duplicate_count", 0) > 1),
                    "idempotent": idempotent,
                    "idempotency_label": "unique ✔" if idempotent else "dup ⛔",
                    "first_seen_label": _format_age(seen_age),
                }
            )
        return {"windows": windows[:60], "decisions": tape[:60]}

    def _refresh_exit_summary(self, summary: Optional[Mapping[str, Any]] = None) -> None:
        data: Optional[Mapping[str, Any]] = None
        if summary and isinstance(summary, Mapping):
            for key in (
                "exit_summary",
                "exit_panel",
                "exit_state",
                "exit_manager",
                "exit_manager_summary",
            ):
                candidate = summary.get(key)
                if isinstance(candidate, Mapping):
                    data = candidate
                    break
        if data is None and self._exit_manager is not None:
            try:
                data = self._exit_manager.summary()
            except Exception:  # pragma: no cover - defensive
                log.debug("Exit manager summary failed", exc_info=True)
                data = None
        sanitized = _sanitize_exit_payload(data)
        with self._exit_lock:
            self._exit_summary = sanitized

    def _collect_exit_panel(self) -> Dict[str, Any]:
        self._refresh_exit_summary()
        with self._exit_lock:
            return copy.deepcopy(self._exit_summary)

    def _collect_shadow_panel(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            fills = list(self._virtual_fills)
            golden_hashes = dict(self._latest_golden_hash)
            golden_prices = {}
            for mint, payload in self._golden_snapshots.items():
                normalized = read_golden(payload, reader="runtime.shadow")
                golden_prices[mint] = normalized.get("mid_usd")
        items: List[Dict[str, Any]] = []
        for payload in fills:
            mint = payload.get("mint")
            if not mint:
                continue
            ts = _entry_timestamp(payload, "ts")
            age = _age_seconds(ts, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            snapshot_hash = payload.get("snapshot_hash")
            mismatch = bool(
                snapshot_hash
                and golden_hashes.get(str(mint))
                and str(snapshot_hash) != golden_hashes[str(mint)]
            )
            items.append(
                {
                    "order_id": payload.get("order_id"),
                    "mint": mint,
                    "side": (payload.get("side") or "").lower() or None,
                    "qty_base": _maybe_float(payload.get("qty_base")),
                    "price_usd": _maybe_float(payload.get("price_usd")),
                    "fees_usd": _maybe_float(payload.get("fees_usd")),
                    "slippage_bps": _maybe_float(payload.get("slippage_bps")),
                    "snapshot_hash": snapshot_hash,
                    "snapshot_hash_short": _short_hash(snapshot_hash),
                    "hash_mismatch": mismatch,
                    "age_label": _format_age(age),
                    "stale": age is not None and age > 900.0,
                }
            )
        positions: List[Dict[str, Any]] = []
        if self.portfolio is not None:
            balances = getattr(self.portfolio, "balances", {})
            if isinstance(balances, dict):
                iterator = balances.items()
            else:
                iterator = []
            for mint, pos in iterator:
                try:
                    qty = float(pos.amount)
                except Exception:
                    qty = 0.0
                try:
                    avg_cost = float(pos.entry_price)
                except Exception:
                    avg_cost = 0.0
                side = "long" if qty >= 0 else "short"
                current_px = golden_prices.get(mint)
                unrealized = None
                if current_px is not None:
                    unrealized = (current_px - avg_cost) * qty
                total_pnl = unrealized
                positions.append(
                    {
                        "mint": mint,
                        "side": side,
                        "qty_base": qty,
                        "avg_cost": avg_cost,
                        "realized_usd": None,
                        "unrealized_usd": unrealized,
                        "total_pnl_usd": total_pnl,
                    }
                )
        return {
            "virtual_fills": items[:200],
            "paper_positions": positions,
            "live_fills": [],
        }

    def _collect_rl_panel(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            weights = list(self._rl_weights_windows)
            decisions = list(self._vote_decisions)
            suggestions = list(self._agent_suggestions)
        entries: List[Dict[str, Any]] = []
        for payload in weights:
            mint = payload.get("mint")
            if not mint:
                continue
            ts = _entry_timestamp(payload, "asof")
            if ts is None:
                ts = _entry_timestamp(payload, "timestamp")
            age = _age_seconds(ts, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            multipliers = (
                payload.get("multipliers")
                or payload.get("weights")
                or payload.get("agents")
            )
            if isinstance(multipliers, dict):
                parts: List[str] = []
                for key, value in list(multipliers.items())[:3]:
                    val = _maybe_float(value)
                    if val is None:
                        continue
                    parts.append(f"{key}:{val:.2f}")
                summary = ", ".join(parts)
                if len(multipliers) > 3:
                    summary = summary + " …" if summary else "…"
                multiplier_map = {str(k): _maybe_float(v) for k, v in multipliers.items()}
            else:
                summary_val = _maybe_float(payload.get("multiplier"))
                summary = f"{summary_val:.2f}" if summary_val is not None else "n/a"
                multiplier_map = {"value": summary_val} if summary_val is not None else {}
            entries.append(
                {
                    "mint": mint,
                    "window_hash": payload.get("window_hash") or payload.get("hash"),
                    "window_hash_short": _short_hash(
                        payload.get("window_hash") or payload.get("hash")
                    ),
                    "multiplier": summary,
                    "age_label": _format_age(age),
                    "stale": age is not None and age > 600.0,
                    "age_seconds": age,
                    "multipliers": multiplier_map,
                }
            )
        uplift = _compute_rl_uplift(decisions, suggestions, now)
        return {"weights": entries[:120], "uplift": uplift}

    def _collect_settings_panel(self) -> Dict[str, Any]:
        now = time.time()
        definitions = [
            ("Global Pause", "/api/control/pause", "control:execution:paused"),
            ("Paper-only", "/api/control/paper", "control:execution:paper_only"),
            ("Family Budget", "/api/control/budget/{family}", "control:budget"),
            ("Spread Gate", "/api/control/spread", "MAX_SPREAD_BPS"),
            ("Depth Gate", "/api/control/depth", "MIN_DEPTH1PCT_USD"),
            ("RL Toggle", "/api/control/rl", "RL_WEIGHTS_DISABLED"),
            ("Blacklist", "/api/control/blacklist", "control:blacklist"),
        ]
        controls: List[Dict[str, Any]] = []
        for label, endpoint, key in definitions:
            state_entry = self._control_states.get(key, {})
            value = state_entry.get("value")
            state = state_entry.get("state")
            if state is None:
                if value in ("1", 1, True, "true", "on"):
                    state = "active"
                elif value is not None:
                    state = str(value)
                else:
                    state = "unknown"
            ttl_val = state_entry.get("ttl")
            ttl_label = _format_ttl(ttl_val, None)
            updated = state_entry.get("updated_at")
            age = None
            if isinstance(updated, (int, float)):
                try:
                    age = max(0.0, now - float(updated))
                except Exception:
                    age = None
            controls.append(
                {
                    "label": label,
                    "endpoint": endpoint,
                    "state": state,
                    "ttl_label": ttl_label,
                    "age_label": _format_age(age),
                }
            )
        overrides = {
            "MAX_SPREAD_BPS": os.getenv("MAX_SPREAD_BPS"),
            "MIN_DEPTH1PCT_USD": os.getenv("MIN_DEPTH1PCT_USD"),
            "RL_WEIGHTS_DISABLED": os.getenv("RL_WEIGHTS_DISABLED"),
        }
        return {"controls": controls, "overrides": overrides, "staleness": {}}

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
        sensitive_suffixes = ("_KEY", "_SECRET", "_TOKEN")
        for key, value in os.environ.items():
            if key in env_summary:
                continue
            upper_key = key.upper()
            if any(upper_key.endswith(suffix) for suffix in sensitive_suffixes):
                env_summary[key] = "***" if value else None
        if self.agent_manager is not None:
            weights_snapshot = self._collect_weights()
            if isinstance(weights_snapshot, dict):
                agent_map = weights_snapshot.get("agents", {})
                if isinstance(agent_map, dict):
                    agents = sorted(agent_map.keys())
                else:
                    agents = []
            else:
                agents = []
            if not agents:
                agents = [agent.name for agent in getattr(self.agent_manager, "agents", [])]
        else:
            agents = []
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
                token_str = str(token).strip()
                if not token_str or token_str in self._discovery_seen:
                    continue
                self._recent_tokens.appendleft(token_str)
                self._discovery_seen.add(token_str)
            while len(self._recent_tokens) > self._recent_tokens_limit:
                removed = self._recent_tokens.pop()
                self._discovery_seen.discard(removed)

    def _collect_actions(self) -> List[Dict[str, Any]]:
        with self._iteration_lock:
            return list(self._last_actions)

    def _emit_action_decisions(self, actions: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
        count = 0
        failures = 0
        for action in actions:
            payload = self._prepare_action_decision(action)
            if not payload:
                continue
            try:
                publish("action_decision", payload)
            except Exception:  # pragma: no cover - defensive logging
                failures += 1
                log.exception("Failed to publish action_decision")
                continue
            count += 1
        return count, failures

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
        self._last_heartbeat_perf = time.perf_counter()
        self._update_rl_gate()
        if stage == "loop" or summary.get("committed"):
            self._iteration_count += 1

        actions = summary.get("actions_executed") or []
        telemetry = summary.get("telemetry")
        if telemetry and stage == "loop":
            evaluation_meta = telemetry.get("evaluation")
            if isinstance(evaluation_meta, dict):
                cached = evaluation_meta.get("cached")
                if cached and not summary.get("actions_count"):
                    evaluation_meta.pop("latency", None)
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
        self._refresh_exit_summary(summary)

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
        log.info(
            "Evaluation tick token=%s actions=%d cached=%s latency=%.2fs errors=%s",
            result.token,
            len(result.actions),
            result.cached,
            result.latency,
            result.errors or None,
        )
        self._pending_tokens[result.token] = summary
        self._last_eval_summary = {
            "token": result.token,
            "actions": len(result.actions),
            "latency": result.latency,
            "cached": result.cached,
            "errors": list(result.errors),
            "metadata": _serialize(result.metadata),
            "timestamp": timestamp,
        }
        await self._apply_iteration_summary(summary, stage="pipeline")

    async def _pipeline_on_execution(self, receipt) -> None:
        if receipt is None:
            return
        summary = self._pending_tokens.get(receipt.token)
        timestamp = datetime.utcnow().isoformat() + "Z"
        actions = []
        real_trades = 0
        simulated_trades = 0
        if summary:
            actions = summary.get("actions_executed", [])
        if receipt.success and actions:
            for action, result in zip(actions, receipt.results):
                action["result"] = result
                sim_flag = False
                if isinstance(result, dict):
                    sim_flag = bool(result.get("simulated"))
                    action["simulated"] = sim_flag
                    mode = result.get("mode")
                    if mode is not None:
                        action["execution_mode"] = mode
                    endpoint = result.get("endpoint")
                    if endpoint is not None:
                        action["execution_endpoint"] = endpoint
                    signature = result.get("signature")
                    if signature is not None:
                        action["tx_signature"] = signature
                else:
                    action["simulated"] = None
                if sim_flag:
                    simulated_trades += 1
                else:
                    real_trades += 1
        elif actions:
            for action in actions:
                if receipt.errors:
                    action["result"] = str(receipt.errors[0])
                else:
                    action["result"] = action.get("result")
                action["simulated"] = None

        updated = summary or {
            "timestamp": timestamp,
            "timestamp_epoch": time.time(),
            "tokens_used": [receipt.token],
            "picked_tokens": [receipt.token],
            "telemetry": {},
        }
        updated["actions_executed"] = actions
        updated["actions_count"] = len(actions)
        updated["real_trades"] = real_trades
        updated["simulated_trades"] = simulated_trades
        updated["any_trade"] = receipt.success and real_trades > 0
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
                "real_trades": real_trades,
                "simulated_trades": simulated_trades,
            }
        )
        log.log(
            logging.INFO if receipt.success else logging.WARNING,
            "Execution tick token=%s success=%s latency=%.2fs actions=%d real=%d simulated=%d errors=%s",
            receipt.token,
            receipt.success,
            receipt.finished_at - receipt.started_at,
            len(actions),
            real_trades,
            simulated_trades,
            receipt.errors or None,
        )
        emitted, failed = self._emit_action_decisions(actions)
        updated["telemetry"]["execution"]["decisions_emitted"] = emitted
        updated["telemetry"]["execution"]["decisions_failed"] = failed
        updated["committed"] = receipt.success
        updated.setdefault("timestamp", timestamp)
        updated.setdefault("timestamp_epoch", time.time())
        await self._apply_iteration_summary(updated, stage="execution")
        self._pending_tokens.pop(receipt.token, None)
        self._last_execution_summary = {
            "token": receipt.token,
            "success": receipt.success,
            "errors": list(receipt.errors),
            "results": _serialize(receipt.results),
            "latency": updated.get("elapsed_s"),
            "timestamp": timestamp,
        }

    async def _pipeline_telemetry_poller(self) -> None:
        index = 0
        while not self.stop_event.is_set():
            if not self.pipeline:
                break
            try:
                samples = await self.pipeline.snapshot_telemetry()
            except Exception:
                samples = []
            try:
                queue_stats = self.pipeline.queue_snapshot()
            except Exception:
                queue_stats = {}
            if queue_stats:
                entry = {
                    "topic": "pipeline",
                    "payload": {
                        "timestamp": time.time(),
                        "stage": "queues",
                        **queue_stats,
                    },
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                }
                self._ui_logs.append(entry)
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
            if self.stop_event.is_set() or self.pipeline is None:
                break
    def _collect_history(self) -> List[Dict[str, Any]]:
        with self._history_lock:
            return list(self._iteration_history)

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _cleanup_task(self, task: asyncio.Task) -> None:
        self._tasks = [t for t in self._tasks if t is not task]

    def _register_task(self, task: Optional[asyncio.Task]) -> None:
        if task is None or task.done():
            return
        self._tasks = [t for t in self._tasks if not t.done()]
        if task not in self._tasks:
            self._tasks.append(task)
            task.add_done_callback(self._cleanup_task)

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
        result = float(value)
    except Exception:
        return default
    if not math.isfinite(result):
        return default
    return result


def _maybe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    numeric = _maybe_float(value)
    if numeric is None:
        return default
    try:
        result = int(numeric)
    except Exception:
        return default
    if result < 0:
        return default
    return result


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except Exception:
            return None
    return None


def _entry_timestamp(entry: Dict[str, Any], field: str) -> Optional[datetime]:
    ts = _parse_timestamp(entry.get(field))
    if ts is not None:
        return ts
    received = entry.get("_received")
    if received is not None:
        try:
            return datetime.fromtimestamp(float(received), tz=timezone.utc)
        except Exception:
            return None
    return None


def _age_seconds(timestamp: Optional[datetime], now: Optional[float] = None) -> Optional[float]:
    if timestamp is None:
        return None
    if now is None:
        now = time.time()
    try:
        return max(0.0, now - timestamp.timestamp())
    except Exception:
        return None


def _format_age(age: Optional[float]) -> str:
    if age is None:
        return "n/a"
    if age < 0.5:
        return "<1s ago"
    total = int(age)
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    parts: List[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if not parts or seconds:
        parts.append(f"{seconds}s")
    return " ".join(parts) + " ago"


def _format_countdown(seconds: float) -> str:
    if seconds <= 0:
        return "expired"
    total = int(round(seconds))
    minutes, secs = divmod(total, 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


def _short_hash(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value)
    if len(text) <= 10:
        return text
    return f"{text[:6]}…{text[-4:]}"


def _format_ttl(remaining: Optional[float], original: Optional[float]) -> str:
    if remaining is None:
        if original is None:
            return "n/a"
        try:
            base = float(original)
        except Exception:
            return "n/a"
        if base <= 0:
            return "expired"
        minutes, seconds = divmod(int(base), 60)
        if minutes:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"
    if remaining <= 0:
        return "expired"
    minutes, seconds = divmod(int(remaining), 60)
    if minutes:
        return f"{minutes}m {seconds}s left"
    return f"{seconds}s left"


def _compute_rl_uplift(
    decisions: List[Dict[str, Any]],
    suggestions: List[Dict[str, Any]],
    now: float,
) -> Dict[str, Any]:
    window = 300.0
    decision_scores: List[float] = []
    for decision in decisions:
        ts = _entry_timestamp(decision, "ts")
        age = _age_seconds(ts, now)
        if age is None and decision.get("_received") is not None:
            try:
                age = max(0.0, now - float(decision["_received"]))
            except Exception:
                age = None
        if age is not None and age <= window:
            score = _maybe_float(decision.get("score"))
            if score is not None:
                decision_scores.append(score)
    suggestion_scores: List[float] = []
    for suggestion in suggestions:
        ts = _entry_timestamp(suggestion, "asof")
        age = _age_seconds(ts, now)
        if age is None and suggestion.get("_received") is not None:
            try:
                age = max(0.0, now - float(suggestion["_received"]))
            except Exception:
                age = None
        if age is not None and age <= window:
            edge = _maybe_float(suggestion.get("edge"))
            if edge is not None:
                suggestion_scores.append(edge)
    rolling = 0.0
    rl_avg = sum(decision_scores) / len(decision_scores) if decision_scores else 0.0
    plain_avg = (
        sum(suggestion_scores) / len(suggestion_scores)
        if suggestion_scores
        else 0.0
    )
    if decision_scores and suggestion_scores:
        rolling = rl_avg - plain_avg
    last_delta = 0.0
    if decisions:
        last_decision = decisions[0]
        score_val = _maybe_float(last_decision.get("score"))
        if score_val is not None:
            related = [
                _maybe_float(suggestion.get("edge"))
                for suggestion in suggestions
                if suggestion.get("mint") == last_decision.get("mint")
                and (
                    last_decision.get("snapshot_hash") is None
                    or suggestion.get("inputs_hash")
                    == last_decision.get("snapshot_hash")
                )
            ]
            related = [value for value in related if value is not None]
            if related:
                last_delta = score_val - (sum(related) / len(related))
    uplift_pct = 0.0
    if plain_avg:
        uplift_pct = (rl_avg - plain_avg) / abs(plain_avg) * 100.0
    return {
        "rolling_5m": rolling,
        "last_decision_delta": last_delta,
        "score_rl": rl_avg,
        "score_plain": plain_avg,
        "uplift_pct": uplift_pct,
    }


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


def _control_active(entry: Dict[str, Any]) -> bool:
    if not isinstance(entry, dict) or not entry:
        return False
    state = entry.get("state")
    value = entry.get("value") if state is None else state
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"active", "on", "true", "yes", "enabled", "paused"}:
            return True
        if lowered in {"off", "false", "no", "disabled"}:
            return False
        return bool(lowered)
    if isinstance(value, (int, float)):
        return value != 0
    return bool(value)
