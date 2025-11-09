from __future__ import annotations

import argparse
import asyncio
import errno
import importlib
import inspect
import logging
import os
import signal
import socket
import sys
from collections.abc import Mapping, Sequence
from contextlib import closing, suppress
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable, Iterable, Optional

from werkzeug.serving import make_server

from ..util import install_uvloop, parse_bool_env
from ..agents.discovery import DEFAULT_DISCOVERY_METHOD, resolve_discovery_method
from .. import event_bus
from ..config import (
    initialize_event_bus,
    apply_env_overrides,
    load_config,
    load_selected_config,
)
from ..main import perform_startup_async
from ..main_state import TradingState
from ..memory import Memory
from ..portfolio import Portfolio
from ..strategy_manager import StrategyManager
from ..agent_manager import AgentManager
from ..loop import ResourceBudgetExceeded, trading_loop as _trading_loop
from .. import ui as _ui_module
from .runtime_wiring import RuntimeWiring, initialise_runtime_wiring, resolve_golden_enabled
from ..production import (
    Provider,
    assert_providers_ok,
    format_configured_providers,
    load_production_env,
    ConnectivityChecker,
)

if hasattr(_ui_module, "create_app"):
    _create_ui_app = _ui_module.create_app  # type: ignore[attr-defined]
else:  # pragma: no cover - fallback used only when UI module lacks factory
    def _create_ui_app(_state: Any | None = None) -> Any:
        try:
            from flask import Flask

            return Flask(__name__)
        except Exception:
            def _wsgi_app(environ, start_response):
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b"UI disabled"]

            return _wsgi_app

if hasattr(_ui_module, "start_websockets"):
    _start_ui_ws = getattr(_ui_module, "start_websockets")
else:  # pragma: no cover - websockets optional in lightweight UI builds
    def _start_ui_ws() -> dict[str, Any]:
        logging.getLogger(__name__).warning(
            "UI websockets not available; continuing with HTTP polling only"
        )
        return {}


install_uvloop()

log = logging.getLogger(__name__)


_UI_TOPIC_DEFAULTS: dict[str, str] = {
    "discovery": "x:discovery.candidates",
    "token_facts": "x:token.snap",
    "market_ohlcv": "x:market.ohlcv.5m",
    "market_depth": "x:market.depth",
    "golden": "x:mint.golden",
    "suggestions": "x:trade.suggested",
    "votes": "x:vote.decisions",
    "virtual_fills": "x:virt.fills",
    "live_fills": "x:live.fills",
}


class _UIPanelForwarder:
    """Subscribe to event-bus topics and push snapshots to the UI."""

    def __init__(
        self,
        *,
        push_event: Callable[[Any], bool],
        topic_map: dict[str, str] | None = None,
    ) -> None:
        self._push = push_event
        self._topic_map = dict(topic_map or _UI_TOPIC_DEFAULTS)
        self._subscriptions: list[Callable[[], None]] = []
        self._started = False
        self._closed = False

    def start(self) -> None:
        if self._started or self._closed:
            return
        self._started = True

        for panel, topic in self._topic_map.items():
            async def _handler(event: Any, _panel: str = panel, _topic: str = topic) -> None:
                message = {"panel": _panel, "topic": _topic, "data": event}
                try:
                    self._push(message)
                except Exception:
                    log.exception(
                        "Failed to push UI event", extra={"panel": _panel, "topic": _topic}
                    )

            unsub = event_bus.subscribe(topic, _handler)
            self._subscriptions.append(unsub)

    async def flush(self) -> None:
        """Emit an initial empty payload for each wired panel."""

        for panel, topic in self._topic_map.items():
            try:
                self._push({"panel": panel, "topic": topic, "data": None})
            except Exception:
                log.debug("UI flush failed for panel %s", panel, exc_info=True)
                continue

    def stop(self) -> None:
        if self._closed:
            return
        self._closed = True
        for unsub in self._subscriptions:
            try:
                unsub()
            except Exception:
                pass
        self._subscriptions.clear()


def _runtime_artifact_dir() -> Path:
    """Return the directory where runtime artifacts should be written."""

    root = Path(os.getenv("RUNTIME_ARTIFACT_ROOT", "artifacts"))
    run_id_raw = (
        os.getenv("RUNTIME_RUN_ID")
        or os.getenv("RUN_ID")
        or os.getenv("MODE")
        or "prelaunch"
    )
    run_id_norm = run_id_raw.replace("\\", "/").strip()
    parts = [part for part in run_id_norm.split("/") if part and part not in {".", ".."}]
    if not parts:
        parts = ["prelaunch"]
    artifact_dir = root.joinpath(*parts)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _publish_ui_url_to_redis(ui_url: str) -> None:
    """Publish the UI URL to Redis using durable and TTL keys."""

    redis_url = os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/1"
    if not redis_url:
        return
    try:
        import redis  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - redis optional in some builds
        log.debug("Redis client unavailable; skipping UI URL publish")
        return

    try:
        client = redis.Redis.from_url(redis_url, socket_timeout=1.0)  # type: ignore[attr-defined]
        client.set("solhunter:ui:url", ui_url)
        client.setex("solhunter:ui:url:latest", 600, ui_url)
    except Exception as exc:  # pragma: no cover - redis connectivity issues
            log.warning("Failed to publish UI URL to redis at %s: %s", redis_url, exc)


PRODUCTION_PROVIDERS: list[Provider] = [
    Provider("Solana", ("SOLANA_RPC_URL", "SOLANA_WS_URL")),
    Provider("Helius", ("HELIUS_API_KEY",)),
    Provider("Redis", ("REDIS_URL",), optional=True),
    Provider("UI", ("UI_WS_URL", "UI_HOST", "UI_PORT"), optional=True),
    Provider("Helius-DAS", ("DAS_BASE_URL",), optional=True),
]


_PROVIDER_OPTIONALITY: dict[str, bool] = {
    provider.name: provider.optional for provider in PRODUCTION_PROVIDERS
}

_PROBE_PROVIDER_MAP: dict[str, str] = {
    "solana-rpc": "Solana",
    "solana-ws": "Solana",
    "helius-rest": "Helius",
    "helius-das": "Helius-DAS",
    "redis": "Redis",
    "ui-ws": "UI",
    "ui-http": "UI",
    "ws-gateway": "UI",
}


def _probe_required(name: str) -> bool:
    provider = _PROBE_PROVIDER_MAP.get(name)
    if provider is None:
        return True
    return not _PROVIDER_OPTIONALITY.get(provider, False)


def load_production_environment(*, overwrite: bool = True) -> dict[str, str]:
    return load_production_env(overwrite=overwrite)


def _config_has_broker(cfg: Mapping[str, object] | None) -> bool:
    if not cfg:
        return False
    raw = cfg.get("broker_urls")
    if isinstance(raw, str):
        if raw.strip():
            return True
    elif isinstance(raw, Sequence):
        for item in raw:
            if str(item).strip():
                return True
    raw_single = cfg.get("broker_url")
    if isinstance(raw_single, str) and raw_single.strip():
        return True
    return False


def _config_has_event_bus(cfg: Mapping[str, object] | None) -> bool:
    if not cfg:
        return False
    raw = cfg.get("event_bus_url")
    if isinstance(raw, str) and raw.strip():
        return True
    return False


def _config_mode(cfg: Mapping[str, object] | None) -> str | None:
    if not cfg:
        return None
    raw = cfg.get("mode")
    if isinstance(raw, str):
        cleaned = raw.strip()
        if cleaned:
            return cleaned
    return None


def apply_production_defaults(cfg: Mapping[str, object] | None = None) -> dict[str, str]:
    applied: dict[str, str] = {}

    mode = _config_mode(cfg) or "live"
    for key in ("SOLHUNTER_MODE", "MODE"):
        if key not in os.environ:
            os.environ.setdefault(key, mode)
            applied[key] = os.environ[key]

    broker_env_configured = any(
        os.getenv(name) for name in ("BROKER_WS_URLS", "BROKER_URLS", "BROKER_URL")
    )
    if not broker_env_configured and _config_has_broker(cfg):
        broker_env_configured = True
    if not broker_env_configured and "BROKER_WS_URLS" not in os.environ:
        os.environ.setdefault("BROKER_WS_URLS", "ws://127.0.0.1:8769")
        applied["BROKER_WS_URLS"] = os.environ["BROKER_WS_URLS"]

    event_bus_configured = bool(os.getenv("EVENT_BUS_URL"))
    if not event_bus_configured and _config_has_event_bus(cfg):
        event_bus_configured = True
    if not event_bus_configured and "EVENT_BUS_URL" not in os.environ:
        os.environ.setdefault("EVENT_BUS_URL", "ws://127.0.0.1:8779")
        applied["EVENT_BUS_URL"] = os.environ["EVENT_BUS_URL"]

    return applied


def validate_production_keys() -> str:
    assert_providers_ok(PRODUCTION_PROVIDERS)
    message = format_configured_providers(PRODUCTION_PROVIDERS)
    log.info(message)
    return message


async def connectivity_check_async(
    checker: ConnectivityChecker | None = None,
) -> list[dict[str, object]]:
    checker = checker or ConnectivityChecker()

    results = await checker.check_all()
    formatted: list[dict[str, object]] = []
    fatal: tuple[str, str, str] | None = None
    event_bus_error: str | None = None
    for result in results:
        required = _probe_required(result.name)
        status = "OK" if result.ok else f"FAIL ({result.error or result.status})"
        log.info(
            "Connectivity %s → %s (%.2f ms)",
            result.name,
            status,
            result.latency_ms or -1.0,
        )
        formatted.append(
            {
                "name": result.name,
                "target": result.target,
                "ok": result.ok,
                "required": required,
                "latency_ms": result.latency_ms,
                "status": result.status,
                "status_code": result.status_code,
                "error": result.error,
            }
        )
        if required and not result.ok and fatal is None:
            reason = result.error or result.status or "unavailable"
            fatal = (result.name, reason, result.target)
        if (
            result.name == "ui-http"
            and not result.ok
            and result.error
            and "event bus" in result.error.lower()
        ):
            event_bus_error = (
                "Event bus unavailable: "
                f"{result.error} ({result.target})"
            )
    if event_bus_error:
        raise SystemExit(event_bus_error)
    if fatal:
        name, reason, target = fatal
        raise SystemExit(
            "Connectivity requirement failed: "
            f"{name} — {reason} ({target})"
        )
    return formatted


async def connectivity_soak_async(
    checker: ConnectivityChecker | None = None,
) -> dict[str, object]:
    duration = float(os.getenv("CONNECTIVITY_SOAK_DURATION", "180"))
    if duration <= 0:
        log.info("Connectivity soak disabled (duration <= 0)")
        return {"disabled": True, "duration": duration}

    checker = checker or ConnectivityChecker()
    output_path = Path("artifacts/prelaunch/connectivity_report.json")

    summary = await checker.run_soak(duration=duration, output_path=output_path)
    log.info(
        "Connectivity soak completed in %.1fs (reconnects=%d)",
        summary.duration,
        summary.reconnect_count,
    )
    return {
        "duration": summary.duration,
        "metrics": summary.metrics,
        "reconnect_count": summary.reconnect_count,
        "report": str(output_path),
    }


def connectivity_check(
    checker: ConnectivityChecker | None = None,
) -> list[dict[str, object]]:
    if checker is not None:
        return asyncio.run(connectivity_check_async(checker=checker))
    return asyncio.run(connectivity_check_async())


def connectivity_soak(
    checker: ConnectivityChecker | None = None,
) -> dict[str, object]:
    if checker is not None:
        return asyncio.run(connectivity_soak_async(checker=checker))
    return asyncio.run(connectivity_soak_async())


@dataclass
class RuntimeHandles:
    ui_app: Any | None = None
    ui_threads: dict[str, Any] | None = None
    ui_server: Any | None = None
    bus_started: bool = False
    tasks: list[asyncio.Task] = field(default_factory=list)
    ui_state: Any | None = None
    depth_proc: Any | None = None
    runtime_wiring: RuntimeWiring | None = None
    bus_subscriptions: list[Callable[[], None]] = field(default_factory=list)
    agent_runtime: Any | None = None
    trade_executor: Any | None = None
    ui_forwarder: Any | None = None


async def maybe_await(result: Any) -> Any:
    if inspect.isawaitable(result):
        return await result
    return result


class RuntimeOrchestrator:
    """Owns startup sequencing for UI → EventBus → Agents (+ RL).

    This runs only when NEW_RUNTIME=1 to preserve the current behavior by default.
    """

    def __init__(
        self,
        *,
        config_path: str | None = None,
        run_http: bool = True,
        skip_stages: Iterable[str] | None = None,
    ) -> None:
        self.config_path = config_path
        self.run_http = run_http
        self.handles = RuntimeHandles()
        self._closed = False
        self._golden_service: Any | None = None
        self._golden_enabled: bool = False
        self._ui_forwarder: _UIPanelForwarder | None = None
        self._stop_reason: str | None = None
        self._pending_stage_events: list[tuple[str, bool, str]] = []
        normalized_skips: set[str] = set()
        if skip_stages:
            for stage in skip_stages:
                if not stage:
                    continue
                normalized_skips.add(str(stage).replace("_", "-").strip())
        self._skip_stages = normalized_skips

    @property
    def stop_reason(self) -> str | None:
        return self._stop_reason

    def _register_task(self, task: asyncio.Task) -> None:
        self.handles.tasks.append(task)
        try:
            task.add_done_callback(self._on_task_done)
        except Exception:
            log.debug("Failed to register task callback", exc_info=True)

    async def _handle_resource_budget_exit(self, exc: ResourceBudgetExceeded) -> None:
        detail = str(exc)
        await self._publish_stage("runtime:resource_exit", False, detail)
        await self.stop_all()

    def _on_task_done(self, task: asyncio.Task) -> None:
        try:
            self.handles.tasks.remove(task)
        except ValueError:
            pass
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except Exception as err:  # pragma: no cover - defensive
            log.debug("Task exception retrieval failed: %s", err)
            return
        if exc is None:
            return
        loop: asyncio.AbstractEventLoop | None
        try:
            loop = task.get_loop()
        except Exception:
            loop = None
        if isinstance(exc, ResourceBudgetExceeded):
            message = str(exc)
            self._stop_reason = message
            log.error("Trading loop stopped due to resource budget: %s", message)
            if loop and not loop.is_closed():
                loop.create_task(self._handle_resource_budget_exit(exc))
            return
        self._stop_reason = str(exc)
        log.exception("Runtime task %s failed", task.get_name(), exc_info=exc)
        if loop and not loop.is_closed():
            loop.create_task(self.stop_all())

    def _emit_ui_ready(self, host: str, port: int) -> None:
        """Log and persist UI readiness details for downstream consumers."""

        url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
        scheme = os.getenv("UI_HTTP_SCHEME") or os.getenv("UI_SCHEME") or "http"
        ui_url = f"{scheme}://{url_host}:{port}"
        ws_urls: dict[str, str] = {}
        if hasattr(_ui_module, "get_ws_urls"):
            try:
                ws_urls = _ui_module.get_ws_urls()  # type: ignore[attr-defined]
            except Exception:
                ws_urls = {}
        rl_url = ws_urls.get("rl") or "-"
        events_url = ws_urls.get("events") or "-"
        logs_url = ws_urls.get("logs") or "-"
        readiness_line = (
            "UI_READY "
            f"url={ui_url} "
            f"rl_ws={rl_url} "
            f"events_ws={events_url} "
            f"logs_ws={logs_url}"
        )
        log.info(readiness_line)
        artifact_dir: Path | None = None
        try:
            artifact_dir = _runtime_artifact_dir()
        except Exception as exc:  # pragma: no cover - filesystem issues
            log.warning("Failed to prepare UI artifact directory: %s", exc)
        if artifact_dir is not None:
            try:
                (artifact_dir / "ui_url.txt").write_text(ui_url, encoding="utf-8")
            except Exception as exc:  # pragma: no cover - filesystem issues
                log.warning("Failed to write UI URL artifact: %s", exc)
        try:
            _publish_ui_url_to_redis(ui_url)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Error publishing UI URL to redis: %s", exc)

    def _is_stage_skipped(self, stage: str) -> bool:
        return stage in self._skip_stages

    def _emit_stage_event(self, stage: str, ok: bool, detail: str) -> None:
        try:
            event_bus.publish(
                "runtime.stage_changed",
                {"stage": stage, "ok": ok, "detail": detail},
            )
        except Exception:
            pass
        if os.getenv("ORCH_VERBOSE", "").lower() in {"1", "true", "yes"}:
            log.info("stage=%s ok=%s detail=%s", stage, ok, detail)

    def _flush_pending_stage_events(self) -> None:
        if not self.handles.bus_started or not self._pending_stage_events:
            return
        pending = list(self._pending_stage_events)
        self._pending_stage_events.clear()
        for stage, ok, detail in pending:
            self._emit_stage_event(stage, ok, detail)

    async def _publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        if self.handles.bus_started:
            self._emit_stage_event(stage, ok, detail)
        else:
            self._pending_stage_events.append((stage, ok, detail))
            if os.getenv("ORCH_VERBOSE", "").lower() in {"1", "true", "yes"}:
                log.info("stage=%s ok=%s detail=%s (queued)", stage, ok, detail)

    async def _ensure_ui_forwarder(self) -> None:
        if self._ui_forwarder is not None:
            return

        ui_state_obj: Any | None = None
        try:
            ui_state_mod = importlib.import_module("solhunter_zero.ui.state")
        except ModuleNotFoundError:
            ui_state_mod = None
        except Exception:
            log.debug("Failed to import solhunter_zero.ui.state", exc_info=True)
            ui_state_mod = None

        if ui_state_mod is not None:
            UIStateCls = getattr(ui_state_mod, "UIState", None)
            topic_map = getattr(ui_state_mod, "TOPIC_MAP", None)
            if callable(UIStateCls):
                try:
                    ui_state_obj = UIStateCls()
                except Exception:
                    log.exception("Failed to initialise streaming UI state")
                    ui_state_obj = None

            if ui_state_obj is not None:
                bus_instance: Any | None = None
                try:
                    bus_mod = importlib.import_module("solhunter_zero.bus")
                except ModuleNotFoundError:
                    bus_mod = None
                except Exception:
                    log.debug("Failed to import solhunter_zero.bus", exc_info=True)
                    bus_mod = None

                if bus_mod is not None:
                    bus_instance = getattr(bus_mod, "BUS", None)
                    if bus_instance is None:
                        EventBusCls = getattr(bus_mod, "EventBus", None)
                        if callable(EventBusCls):
                            try:
                                bus_instance = EventBusCls()
                            except Exception:
                                log.exception("Failed to instantiate EventBus for UI state")
                                bus_instance = None

                if bus_instance is not None and callable(getattr(bus_instance, "stream", None)):
                    topics = topic_map if isinstance(topic_map, dict) and topic_map else _UI_TOPIC_DEFAULTS
                    add_provider = getattr(ui_state_obj, "add_provider", None)
                    if callable(add_provider):
                        for panel, topic in topics.items():
                            try:
                                stream = bus_instance.stream(topic)
                            except Exception:
                                log.exception("Failed to create bus stream for panel %s", panel)
                                continue
                            try:
                                add_provider(panel, stream)
                            except Exception:
                                log.exception(
                                    "Failed to add UI provider for panel %s (topic %s)",
                                    panel,
                                    topic,
                                )
                        self._ui_forwarder = ui_state_obj
                        self.handles.ui_forwarder = ui_state_obj
                        flush = getattr(ui_state_obj, "flush", None)
                        if callable(flush):
                            try:
                                await maybe_await(flush())
                            except Exception:
                                log.debug("UI state flush failed", exc_info=True)
                        return

        push_event = getattr(_ui_module, "push_event", None)
        if not callable(push_event):
            return

        forwarder = _UIPanelForwarder(push_event=push_event)
        try:
            forwarder.start()
        except Exception:
            log.exception("Failed to initialise UI panel forwarders")
            forwarder.stop()
            return

        self._ui_forwarder = forwarder
        self.handles.ui_forwarder = forwarder
        try:
            await maybe_await(forwarder.flush())
        except Exception:
            log.debug("UI forwarder flush failed", exc_info=True)

    async def _run_stage_callable(
        self,
        stage: str,
        func: Callable[[], Any],
        detail_formatter: Callable[[Any], str] | None = None,
    ) -> Any:
        if self._is_stage_skipped(stage):
            await self._publish_stage(stage, True, "skipped")
            return None

        try:
            result = func()
            if inspect.isawaitable(result):
                result = await result
        except SystemExit as exc:
            detail = str(exc)
            await self._publish_stage(stage, False, detail)
            raise
        except Exception as exc:
            await self._publish_stage(stage, False, str(exc))
            raise
        else:
            detail = ""
            if detail_formatter is not None:
                try:
                    detail = detail_formatter(result)
                except Exception:
                    log.debug("Failed to format stage detail for %s", stage, exc_info=True)
                    detail = ""
            await self._publish_stage(stage, True, detail)
            return result

    def _format_connectivity_detail(self, result: Any) -> str:
        try:
            records = list(result or [])
        except TypeError:
            return ""
        failures = [str(item.get("name")) for item in records if not item.get("ok")]
        if failures:
            preview = ",".join(failures[:5])
            if len(failures) > 5:
                preview = f"{preview},..."
            return f"failures={preview}"
        return f"targets={len(records)}"

    def _format_connectivity_soak_detail(self, result: Any) -> str:
        if not isinstance(result, Mapping):
            return ""
        if result.get("disabled"):
            return "disabled"
        detail_parts: list[str] = []
        duration = result.get("duration")
        reconnects = result.get("reconnect_count")
        if isinstance(duration, (int, float)):
            detail_parts.append(f"duration={duration:.1f}s")
        elif duration is not None:
            detail_parts.append(f"duration={duration}")
        if reconnects is not None:
            detail_parts.append(f"reconnects={reconnects}")
        return " ".join(detail_parts)

    async def _run_prelaunch_checks(self) -> None:
        cfg_cache: Mapping[str, object] | None = None

        def _load_env() -> dict[str, str]:
            return load_production_environment()

        def _format_env_detail(env: Any) -> str:
            try:
                count = len(env)
            except Exception:
                count = 0
            return f"count={count}"

        await self._run_stage_callable(
            "load-production-env",
            _load_env,
            _format_env_detail,
        )

        def _apply_defaults() -> dict[str, str]:
            nonlocal cfg_cache
            selected_cfg: Mapping[str, object] | None
            try:
                selected_cfg = load_selected_config()
            except Exception:
                selected_cfg = None
            base_cfg: Mapping[str, object] | None
            if selected_cfg:
                base_cfg = selected_cfg
            else:
                base_cfg = load_config(self.config_path)
            cfg_cache = apply_env_overrides(base_cfg)
            return apply_production_defaults(cfg_cache)

        def _format_defaults_detail(applied: Any) -> str:
            if not isinstance(applied, Mapping):
                return "applied=0"
            keys = sorted(str(k) for k in applied.keys())
            if not keys:
                return "applied=0"
            preview = ",".join(keys[:5])
            if len(keys) > 5:
                preview = f"{preview},..."
            return f"applied={preview}"

        await self._run_stage_callable(
            "apply-prod-defaults",
            _apply_defaults,
            _format_defaults_detail,
        )

        await self._run_stage_callable(
            "validate-keys",
            validate_production_keys,
            lambda msg: str(msg or ""),
        )

        await self._run_stage_callable(
            "connectivity-check",
            connectivity_check_async,
            self._format_connectivity_detail,
        )

        await self._run_stage_callable(
            "connectivity-soak",
            connectivity_soak_async,
            self._format_connectivity_soak_detail,
        )

    async def start_bus(self) -> None:
        await self._publish_stage("bus:init", True)
        # Choose event-bus WS port early and export URLs so init sees them
        ws_port = int(os.getenv("EVENT_BUS_WS_PORT", "8779") or 8779)
        os.environ.setdefault("EVENT_BUS_URL", f"ws://127.0.0.1:{ws_port}")
        os.environ.setdefault("BROKER_WS_URLS", f"ws://127.0.0.1:{ws_port}")
        # Load config early so event bus has proper env/urls
        try:
            from ..config import get_broker_urls, set_env_from_config
            cfg = apply_env_overrides(load_selected_config() or load_config(self.config_path))
            set_env_from_config(cfg)
        except Exception:
            cfg = None
        # Opportunistically ensure local redis if configured to localhost
        try:
            from ..redis_util import ensure_local_redis_if_needed

            urls = []
            if cfg is not None:
                try:
                    urls = get_broker_urls(cfg)
                except Exception:
                    urls = []
            ensure_local_redis_if_needed(urls)
        except Exception:
            pass
        initialize_event_bus()
        # Prefer a dedicated local WS port to avoid conflicts
        try:
            await event_bus.start_ws_server("localhost", ws_port)
            self.handles.bus_started = True
            self._flush_pending_stage_events()
            await self._publish_stage("bus:ws", True, f"port={ws_port}")
        except Exception as exc:
            await self._publish_stage("bus:ws", False, f"{exc}")
        # Ensure peers point to the local WS
        os.environ["BROKER_WS_URLS"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["EVENT_BUS_URL"] = f"ws://127.0.0.1:{ws_port}"
        ok = await event_bus.verify_broker_connection(timeout=5.0)
        if not ok:
            await self._publish_stage("bus:verify", False, "broker roundtrip failed")
            if parse_bool_env("BROKER_VERIFY_ABORT", False):
                raise SystemExit(1)
        else:
            await self._publish_stage("bus:verify", True)
            broker_url = (
                os.getenv("REDIS_URL")
                or os.getenv("BROKER_URL")
                or os.getenv("BROKER_URLS")
                or "redis://localhost:6379/1"
            )
            channel = os.getenv("BROKER_CHANNEL", "solhunter-events-v3")
            log.info("Event bus: connected redis broker %s channel=%s", broker_url, channel)
        # continue even if local ws failed (peers may still be available)
        await asyncio.gather(
            self._maybe_start_mint_stream(),
            self._maybe_start_mempool_stream(),
            self._maybe_start_amm_watch(),
            self._maybe_start_seed_tokens(),
        )

    async def start_ui(self) -> None:
        await self._publish_stage("ui:init", True)
        state_obj = None
        if hasattr(_ui_module, "UIState"):
            try:
                state_obj = _ui_module.UIState()
            except Exception:
                state_obj = None
        if state_obj is not None:
            try:
                self.handles.runtime_wiring = initialise_runtime_wiring(state_obj)
            except Exception:
                log.exception("Failed to initialise runtime wiring for UI state")
        app = _create_ui_app(state_obj)
        threads = _start_ui_ws() if callable(_start_ui_ws) else {}
        if hasattr(_ui_module, "get_ws_urls"):
            try:
                ws_urls = _ui_module.get_ws_urls()  # type: ignore[attr-defined]
            except Exception:
                ws_urls = {}
            else:
                env_map = {
                    "events": ("UI_EVENTS_WS", "UI_EVENTS_WS_URL", "UI_WS_URL"),
                    "rl": ("UI_RL_WS", "UI_RL_WS_URL"),
                    "logs": ("UI_LOGS_WS", "UI_LOG_WS_URL"),
                }
                for channel, keys in env_map.items():
                    url = ws_urls.get(channel)
                    if not url:
                        continue
                    for key in keys:
                        os.environ.setdefault(key, url)
        self.handles.ui_app = app
        self.handles.ui_threads = threads
        self.handles.ui_state = state_obj
        await self._publish_stage("ui:ws", True)
        if self.run_http and str(os.getenv("UI_DISABLE_HTTP_SERVER", "")).lower() not in {"1", "true", "yes"}:
            # Start Flask server in a background thread using werkzeug only if available.
            import threading

            def _select_listen_port(host: str, requested_port: int) -> tuple[int, bool]:
                """Return a usable port and whether it differs from the request."""

                if requested_port <= 0:
                    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                        sock.bind((host, 0))
                        return sock.getsockname()[1], requested_port != 0

                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                    try:
                        sock.bind((host, requested_port))
                        return requested_port, False
                    except OSError as exc:
                        if exc.errno not in {errno.EADDRINUSE, errno.EACCES, errno.EADDRNOTAVAIL}:
                            raise

                # Requested port unavailable; ask OS for an open port
                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                    sock.bind((host, 0))
                    return sock.getsockname()[1], True

            def _serve(port_queue: Queue[Any]) -> None:
                server = None
                try:
                    host = os.getenv("UI_HOST", "127.0.0.1")
                    port_env = os.getenv("UI_PORT", os.getenv("PORT", "5000") or 5000)
                    try:
                        requested_port = int(port_env)
                    except (TypeError, ValueError):
                        requested_port = 0

                    port, changed = _select_listen_port(host, requested_port)
                    if changed:
                        log.warning(
                            "Requested UI port %s unavailable; using %s instead", requested_port, port
                        )
                    os.environ["UI_PORT"] = str(port)

                    app = self.handles.ui_app or _create_ui_app(self.handles.ui_state)
                    server = make_server(host, port, app)
                    server.daemon_threads = True
                    self.handles.ui_server = server
                    port_queue.put(port)
                    try:
                        self._emit_ui_ready(host, port)
                    except Exception:
                        log.exception("Failed to emit UI readiness signal")
                    server.serve_forever()
                except Exception:
                    with suppress(Exception):
                        port_queue.put(sys.exc_info()[1] or RuntimeError("ui serve failed"))
                    log.exception("UI HTTP server failed")
                finally:
                    self.handles.ui_server = None
                    if server is not None:
                        with suppress(Exception):
                            server.shutdown()
                        with suppress(Exception):
                            server.server_close()

            port_queue: Queue[Any] = Queue(maxsize=1)
            t = threading.Thread(target=_serve, args=(port_queue,), daemon=True)
            t.start()
            actual_port: str | None = None
            try:
                result = port_queue.get(timeout=5)
            except Empty:
                await self._publish_stage(
                    "ui:http",
                    False,
                    "ui server did not report readiness within timeout",
                )
            else:
                if isinstance(result, Exception):
                    await self._publish_stage("ui:http", False, str(result))
                else:
                    actual_port = str(result)
                    await self._publish_stage(
                        "ui:http",
                        True,
                        f"host={os.getenv('UI_HOST','127.0.0.1')} port={actual_port}",
                    )

    async def start_agents(self) -> None:
        await self._run_prelaunch_checks()
        # Use existing startup path to ensure consistent connectivity + depth_service
        await self._publish_stage("agents:startup", True)
        cfg, runtime_cfg, proc = await perform_startup_async(self.config_path, offline=False, dry_run=False)
        self.handles.depth_proc = proc

        await self._ensure_ui_forwarder()

        # Build runtime services
        memory_path = os.getenv("MEMORY_PATH", "sqlite:///memory.db")
        portfolio_path = os.getenv("PORTFOLIO_PATH", "portfolio.json")
        memory = Memory(memory_path)
        memory.start_writer()
        portfolio = Portfolio(path=portfolio_path)
        state = TradingState()

        # Strategy/agents selection mirrors main.main
        agent_manager: AgentManager | None = None
        strategies = cfg.get("strategies")
        if isinstance(strategies, str):
            strategies = [s.strip() for s in strategies.split(",") if s.strip()]
        elif not strategies:
            strategies = []
        if cfg.get("agents"):
            agent_manager = AgentManager.from_config(cfg)
            strategy_manager = None
            if agent_manager is None:
                strategy_manager = StrategyManager(strategies)
        else:
            agent_manager = AgentManager.from_default()
            strategy_manager = None if agent_manager is not None else StrategyManager(strategies)

        # Golden pipeline service wiring
        golden_enabled = resolve_golden_enabled(cfg)
        self._golden_enabled = golden_enabled
        if golden_enabled and agent_manager is not None and portfolio is not None:
            try:
                from ..golden_pipeline.service import GoldenPipelineService

                service = GoldenPipelineService(
                    agent_manager=agent_manager,
                    portfolio=portfolio,
                )
                await service.start()
                self._golden_service = service
                agent_count = len(getattr(agent_manager, "agents", []) or [])
                await self._publish_stage(
                    "golden:start",
                    True,
                    f"providers={agent_count}",
                )
                log.info("GOLDEN_READY topic=x:mint.golden providers=%s", agent_count)
                wiring = self.handles.runtime_wiring
                ready = True
                if wiring is not None:
                    ready = await wiring.wait_for_topic("x:mint.golden", timeout=15.0)
                if ready:
                    await self._publish_stage("golden:ready", True, "topic=x:mint.golden")
                else:
                    detail = "missing topic x:mint.golden"
                    log.info(
                        "Golden pipeline topic x:mint.golden not observed within readiness window; continuing (detail=%s)",
                        detail,
                    )
                    await self._publish_stage("golden:ready", True, "deferred-topic-observation")
            except Exception as exc:
                await self._publish_stage("golden:start", False, str(exc))
                log.exception("Failed to start Golden pipeline service")
                with suppress(Exception):
                    await service.stop()
                self._golden_service = None
                raise
        else:
            await self._publish_stage("golden:start", True, "disabled")

        # Announce loaded agents
        try:
            names = [a.name for a in (agent_manager.agents if agent_manager else [])]
            await self._publish_stage("agents:loaded", True, f"count={len(names)} names={','.join(names[:10])}{'...' if len(names)>10 else ''}")
        except Exception:
            pass

        # Choose mode: event-driven vs classic loop
        event_driven = parse_bool_env("EVENT_DRIVEN", True)
        # Live drill: simulate executions on live data without broadcasting trades
        try:
            import os as _os
            if bool(cfg.get("live_drill", False)):
                _os.environ["LIVE_DRILL"] = "1"
                await self._publish_stage("drill:on", True, "live trade broadcasting disabled; simulation active")
        except Exception:
            pass

        # Trading loop task (classic mode: reuses adaptive loop with evolve/cull)
        loop_delay = int(cfg.get("loop_delay", int(os.getenv("LOOP_DELAY", "60") or 60)))
        min_delay = int(cfg.get("min_delay", 1))
        max_delay = int(cfg.get("max_delay", loop_delay))
        cpu_low_threshold = float(cfg.get("cpu_low_threshold", 20.0))
        cpu_high_threshold = float(cfg.get("cpu_high_threshold", 80.0))
        depth_freq_low = float(cfg.get("depth_freq_low", 1.0))
        depth_freq_high = float(cfg.get("depth_freq_high", 10.0))
        depth_rate_limit = float(cfg.get("depth_rate_limit", 0.1))
        rl_daemon = bool(cfg.get("rl_auto_train", False)) or parse_bool_env("RL_DAEMON", False)
        rl_interval = float(cfg.get("rl_interval", 3600.0))

        # Derive discovery & arbitrage
        discovery_method = resolve_discovery_method(cfg.get("discovery_method"))
        if discovery_method is None:
            discovery_method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        if discovery_method is None:
            discovery_method = DEFAULT_DISCOVERY_METHOD
        arbitrage_threshold = float(cfg.get("arbitrage_threshold", 0.0))
        arbitrage_amount = float(cfg.get("arbitrage_amount", 0.0))
        arbitrage_tokens = None
        tokens_cfg = cfg.get("arbitrage_tokens")
        if isinstance(tokens_cfg, str):
            arbitrage_tokens = [t.strip() for t in tokens_cfg.split(",") if t.strip()]
        elif tokens_cfg:
            arbitrage_tokens = list(tokens_cfg)

        async def _run_classic():
            await _trading_loop(
                cfg,
                runtime_cfg,
                memory,
                portfolio,
                state,
                loop_delay=loop_delay,
                min_delay=min_delay,
                max_delay=max_delay,
                cpu_low_threshold=cpu_low_threshold,
                cpu_high_threshold=cpu_high_threshold,
                depth_freq_low=depth_freq_low,
                depth_freq_high=depth_freq_high,
                depth_rate_limit=depth_rate_limit,
                iterations=None,
                testnet=False,
                dry_run=False,
                offline=False,
                token_file=None,
                discovery_method=discovery_method,
                keypair=None,
                stop_loss=cfg.get("stop_loss"),
                take_profit=cfg.get("take_profit"),
                trailing_stop=cfg.get("trailing_stop"),
                max_drawdown=float(cfg.get("max_drawdown", 1.0)),
                volatility_factor=float(cfg.get("volatility_factor", 1.0)),
                arbitrage_threshold=arbitrage_threshold,
                arbitrage_amount=arbitrage_amount,
                strategy_manager=strategy_manager,
                agent_manager=agent_manager,
                market_ws_url=cfg.get("market_ws_url"),
                order_book_ws_url=cfg.get("order_book_ws_url"),
                arbitrage_tokens=arbitrage_tokens,
                rl_daemon=rl_daemon,
                rl_interval=rl_interval,
                proc_ref=[self.handles.depth_proc],
                live_discovery=cfg.get("live_discovery"),
            )

        if not event_driven:
            task = asyncio.create_task(_run_classic(), name="trading_loop")
            self._register_task(task)
            await self._publish_stage("agents:loop", True)
            return

        # Event-driven mode: start agent runtime, swarm coordinator and executor
        from ..agents.runtime import AgentRuntime
        from ..exec_service import TradeExecutor
        from ..agents.discovery import DiscoveryAgent
        from ..loop import _init_rl_training as _init_rl_training  # type: ignore

        aruntime = AgentRuntime(agent_manager or AgentManager.from_default(), portfolio)
        await aruntime.start()
        execu = TradeExecutor(memory, portfolio)
        execu.start()

        self.handles.agent_runtime = aruntime
        self.handles.trade_executor = execu

        async def _discovery_loop():
            agent = DiscoveryAgent()
            method = discovery_method
            while True:
                try:
                    tokens = await agent.discover_tokens(method=method, offline=False)
                    if tokens:
                        event_bus.publish("token_discovered", list(tokens))
                except Exception:
                    pass
                await asyncio.sleep(max(5, min(60, loop_delay)))

        async def _evolve_loop():
            # Evolve/mutate/cull continually based on success metrics
            interval = int(getattr(agent_manager, "evolve_interval", 30) or 30)
            while True:
                try:
                    if agent_manager is not None:
                        await agent_manager.evolve(
                            threshold=getattr(agent_manager, "mutation_threshold", 0.0)
                        )
                        await agent_manager.update_weights()
                        agent_manager.save_weights()
                except Exception:
                    pass
                await asyncio.sleep(max(5, interval))

        # Start RL training if enabled
        rl_task = await _init_rl_training(cfg, rl_daemon=rl_daemon, rl_interval=rl_interval)
        if isinstance(rl_task, asyncio.Task):
            self._register_task(rl_task)
        elif rl_task is not None:
            self.handles.tasks.append(rl_task)

        # MEV bundles readiness hint
        use_bundles = bool(cfg.get("use_mev_bundles", False))
        if use_bundles and (not os.getenv("JITO_RPC_URL") or not os.getenv("JITO_AUTH")):
            await self._publish_stage("mev:warn", False, "MEV bundles enabled but JITO credentials missing")

        self._register_task(asyncio.create_task(_discovery_loop(), name="discovery_loop"))
        self._register_task(asyncio.create_task(_evolve_loop(), name="evolve_loop"))
        await self._publish_stage("agents:event_runtime", True)

    async def start(self) -> None:
        # Make orchestrator subscribe to control messages
        def _ctl(payload: dict) -> None:
            cmd = (payload or {}).get("cmd")
            if cmd == "stop":
                asyncio.get_event_loop().create_task(self.stop_all())

        unsub = event_bus.subscribe("control", _ctl)
        self.handles.bus_subscriptions.append(unsub)

        # Prime production environment so UI and bus pick up overrides even if
        # the full prelaunch pipeline runs later.
        try:
            load_production_environment()
            apply_production_defaults()
        except Exception:
            log.debug("Initial production environment load failed", exc_info=True)

        # 1) UI, 2) Bus, 3) Agents
        await self.start_ui()
        await self.start_bus()
        # Start decision metrics aggregator
        try:
            from ..metrics.decision_metrics import DecisionMetrics

            self._dec_metrics = DecisionMetrics()
            self._dec_metrics.start()
        except Exception:
            self._dec_metrics = None
        # Start adaptive risk controller
        try:
            from ..risk_controller import RiskController

            self._risk_ctl = RiskController()
            self._risk_ctl.start()
        except Exception:
            self._risk_ctl = None
        await self.start_agents()
        await self._publish_stage("runtime:ready", True)

    async def stop_all(self) -> None:
        if self._closed:
            return
        self._closed = True
        await self._publish_stage("runtime:stopping", True)
        # Cancel tasks
        tasks = list(self.handles.tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.handles.tasks.clear()
        forwarder = self._ui_forwarder or self.handles.ui_forwarder
        if forwarder is not None:
            try:
                forwarder.stop()
            except Exception:
                pass
        self._ui_forwarder = None
        self.handles.ui_forwarder = None
        for unsub in list(self.handles.bus_subscriptions):
            try:
                unsub()
            except Exception:
                pass
        self.handles.bus_subscriptions.clear()
        if self.handles.agent_runtime is not None:
            try:
                await maybe_await(self.handles.agent_runtime.stop())
            finally:
                self.handles.agent_runtime = None
        if self.handles.trade_executor is not None:
            try:
                await maybe_await(self.handles.trade_executor.stop())
            finally:
                self.handles.trade_executor = None
        # Stop wiring subscriptions
        try:
            wiring = self.handles.runtime_wiring
            if wiring is not None:
                wiring.close()
        except Exception:
            pass
        self.handles.runtime_wiring = None
        # Stop Golden pipeline service
        if self._golden_service is not None:
            try:
                await self._golden_service.stop()
            except Exception:
                pass
            self._golden_service = None
        # Stop bus server
        if self.handles.bus_started:
            try:
                await event_bus.stop_ws_server()
            except Exception:
                pass
        # Stop metrics
        try:
            m = getattr(self, "_dec_metrics", None)
            if m is not None:
                m.stop()
        except Exception:
            pass
        # Stop risk controller
        try:
            rc = getattr(self, "_risk_ctl", None)
            if rc is not None:
                rc.stop()
        except Exception:
            pass
        # Close UI WS threads are daemonic; HTTP server stops with process
        try:
            from ..http import close_session

            await close_session()
        except Exception:
            pass
        await self._publish_stage("runtime:stopped", True)


    async def _maybe_start_mint_stream(self) -> None:
        if not parse_bool_env("MINT_STREAM_ENABLE", False):
            return
        try:
            from ..rpc_mint_stream import run_rpc_mint_stream  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            await self._publish_stage(
                "discovery:mint_stream",
                False,
                f"import failed ({exc})",
            )
            return
        if any(
            task.get_name() == "rpc_mint_stream" and not task.done()
            for task in self.handles.tasks
        ):
            return
        try:
            task = asyncio.create_task(run_rpc_mint_stream(), name="rpc_mint_stream")
        except Exception as exc:  # pragma: no cover - unlikely
            await self._publish_stage(
                "discovery:mint_stream",
                False,
                f"task start failed ({exc})",
            )
            return

        def _log_mint_stream_done(fut: asyncio.Task) -> None:
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("RPC mint stream terminated unexpectedly")

        task.add_done_callback(_log_mint_stream_done)
        self._register_task(task)
        ws_url = os.getenv("MINT_STREAM_WS_URL") or "-"
        await self._publish_stage(
            "discovery:mint_stream",
            True,
            f"ws={ws_url}",
        )

    async def _maybe_start_mempool_stream(self) -> None:
        if not parse_bool_env("MEMPOOL_STREAM_ENABLE", False):
            return
        try:
            from ..jito_mempool_stream import run_jito_mempool_stream  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            await self._publish_stage(
                "discovery:mempool_stream",
                False,
                f"import failed ({exc})",
            )
            return
        if any(
            task.get_name() == "jito_mempool_stream" and not task.done()
            for task in self.handles.tasks
        ):
            return
        try:
            task = asyncio.create_task(run_jito_mempool_stream(), name="jito_mempool_stream")
        except Exception as exc:
            await self._publish_stage(
                "discovery:mempool_stream",
                False,
                f"task start failed ({exc})",
            )
            return

        def _log_mempool_done(fut: asyncio.Task) -> None:
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Jito mempool stream terminated unexpectedly")

        task.add_done_callback(_log_mempool_done)
        self._register_task(task)
        await self._publish_stage(
            "discovery:mempool_stream",
            True,
            "status=running",
        )

    async def _maybe_start_amm_watch(self) -> None:
        if not parse_bool_env("AMM_WATCH_ENABLE", False):
            return
        try:
            from ..amm_pool_watcher import run_amm_pool_watcher  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            await self._publish_stage(
                "discovery:amm_watch",
                False,
                f"import failed ({exc})",
            )
            return
        if any(
            task.get_name() == "amm_pool_watcher" and not task.done()
            for task in self.handles.tasks
        ):
            return
        try:
            task = asyncio.create_task(run_amm_pool_watcher(), name="amm_pool_watcher")
        except Exception as exc:
            await self._publish_stage(
                "discovery:amm_watch",
                False,
                f"task start failed ({exc})",
            )
            return

        def _log_amm_done(fut: asyncio.Task) -> None:
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("AMM pool watcher terminated unexpectedly")

        task.add_done_callback(_log_amm_done)
        self._register_task(task)
        await self._publish_stage(
            "discovery:amm_watch",
            True,
            "status=running",
        )

    async def _maybe_start_seed_tokens(self) -> None:
        if not (parse_bool_env("SEED_PUBLISH_ENABLE", False) and os.getenv("SEED_TOKENS")):
            return
        try:
            from ..seed_token_publisher import run_seed_token_publisher  # type: ignore
        except Exception as exc:  # pragma: no cover
            await self._publish_stage(
                "discovery:seed_tokens",
                False,
                f"import failed ({exc})",
            )
            return
        if any(
            task.get_name() == "seed_token_publisher" and not task.done()
            for task in self.handles.tasks
        ):
            return
        try:
            task = asyncio.create_task(run_seed_token_publisher(), name="seed_token_publisher")
        except Exception as exc:
            await self._publish_stage(
                "discovery:seed_tokens",
                False,
                f"task start failed ({exc})",
            )
            return

        def _log_seed_done(fut: asyncio.Task) -> None:
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                log.exception("Seed token publisher terminated unexpectedly")

        task.add_done_callback(_log_seed_done)
        self._register_task(task)
        await self._publish_stage(
            "discovery:seed_tokens",
            True,
            "status=running",
        )


def _parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SolHunter Zero Runtime Orchestrator")
    p.add_argument("--config", default=None, help="Path to config file")
    p.add_argument("--no-http", action="store_true", help="Do not run UI HTTP server")
    p.add_argument("--verbose-stages", action="store_true", help="Log stage changes verbosely")
    return p.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> int:
    args = _parse_cli(argv)
    run_http = not args.no_http
    if args.verbose_stages:
        os.environ["ORCH_VERBOSE"] = "1"
    orch = RuntimeOrchestrator(config_path=args.config, run_http=run_http)

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(orch.stop_all()))
        except NotImplementedError:
            pass

    await orch.start()
    # Keep process alive while trading loop runs
    while not orch._closed:
        await asyncio.sleep(1)

    return 0


def main(argv: list[str] | None = None) -> None:
    if os.getenv("NEW_RUNTIME", "").lower() not in {"1", "true", "yes"}:
        print("NEW_RUNTIME is not enabled; aborting orchestrator.")
        raise SystemExit(2)
    try:
        asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        pass
