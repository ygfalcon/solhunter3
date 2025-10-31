"""Lightweight UI server for SolHunter Zero.

The previous UI module mixed Flask wiring, websocket threads, and a large
amount of legacy state.  For the new runtime we provide a focused
implementation that exposes the runtime status over a small REST surface and
can be embedded cleanly inside the trading orchestrator.

The UI server is intentionally simple:

* ``UIState`` exposes callables that the trading runtime wires up.  The UI
  never reaches into runtime objects directly which keeps threading concerns
  manageable and avoids circular imports.
* ``create_app`` produces a Flask application with a tiny JSON API.  Endpoints
  are designed to be polled by dashboards, CLI tooling, or simple HTTP checks.
* ``UIServer`` starts/stops the Flask development server in a background
  thread.  The runtime controls its lifecycle explicitly so a one-click
  startup flow can guarantee the UI is available once trading begins.

This module is dependency-light (only Flask) and stays synchronous so it can
run happily inside the existing asyncio runtime without requiring ASGI
bridges.  The goal is reliability and observability rather than complex
visualisations; users can extend the JSON outputs or put a reverse proxy in
front of it if richer dashboards are required.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

from flask import Flask, jsonify, render_template, request

from .agents.discovery import (
    DEFAULT_DISCOVERY_METHOD,
    DISCOVERY_METHODS,
    resolve_discovery_method,
)


log = logging.getLogger(__name__)
PACKAGE_ROOT = Path(__file__).resolve().parent


StatusProvider = Callable[[], Dict[str, Any]]
ListProvider = Callable[[], Iterable[Dict[str, Any]]]
DictProvider = Callable[[], Dict[str, Any]]


@dataclass
class UIState:
    """Holds callables that provide live data to the UI endpoints."""

    status_provider: StatusProvider = field(
        default=lambda: {"event_bus": False, "trading_loop": False}
    )
    activity_provider: ListProvider = field(default=lambda: [])
    trades_provider: ListProvider = field(default=lambda: [])
    weights_provider: DictProvider = field(default=lambda: {})
    rl_status_provider: DictProvider = field(default=lambda: {})
    logs_provider: ListProvider = field(default=lambda: [])
    summary_provider: DictProvider = field(default=lambda: {})
    discovery_provider: DictProvider = field(default=lambda: {"recent": []})
    config_provider: DictProvider = field(default=lambda: {})
    actions_provider: ListProvider = field(default=lambda: [])
    history_provider: ListProvider = field(default=lambda: [])

    def snapshot_status(self) -> Dict[str, Any]:
        try:
            return dict(self.status_provider())
        except Exception:  # pragma: no cover - defensive coding
            log.exception("UI status provider failed")
            return {"event_bus": False, "trading_loop": False}

    def snapshot_activity(self) -> List[Dict[str, Any]]:
        try:
            return list(self.activity_provider())
        except Exception:  # pragma: no cover
            log.exception("UI activity provider failed")
            return []

    def snapshot_trades(self) -> List[Dict[str, Any]]:
        try:
            return list(self.trades_provider())
        except Exception:  # pragma: no cover
            log.exception("UI trades provider failed")
            return []

    def snapshot_weights(self) -> Dict[str, Any]:
        try:
            return dict(self.weights_provider())
        except Exception:  # pragma: no cover
            log.exception("UI weights provider failed")
            return {}

    def snapshot_rl(self) -> Dict[str, Any]:
        try:
            return dict(self.rl_status_provider())
        except Exception:  # pragma: no cover
            log.exception("UI RL status provider failed")
            return {}

    def snapshot_logs(self) -> List[Dict[str, Any]]:
        try:
            return list(self.logs_provider())
        except Exception:  # pragma: no cover
            log.exception("UI log provider failed")
            return []

    def snapshot_summary(self) -> Dict[str, Any]:
        try:
            return dict(self.summary_provider())
        except Exception:  # pragma: no cover
            log.exception("UI summary provider failed")
            return {}

    def snapshot_discovery(self) -> Dict[str, Any]:
        try:
            data = self.discovery_provider()
            if isinstance(data, dict):
                return dict(data)
            return {"recent": list(data)}
        except Exception:  # pragma: no cover
            log.exception("UI discovery provider failed")
            return {"recent": []}

    def snapshot_config(self) -> Dict[str, Any]:
        try:
            return dict(self.config_provider())
        except Exception:  # pragma: no cover
            log.exception("UI config provider failed")
            return {}

    def snapshot_history(self) -> List[Dict[str, Any]]:
        try:
            return list(self.history_provider())
        except Exception:  # pragma: no cover
            log.exception("UI history provider failed")
            return []

    def snapshot_actions(self) -> List[Dict[str, Any]]:
        try:
            return list(self.actions_provider())
        except Exception:  # pragma: no cover
            log.exception("UI actions provider failed")
            return []


def create_app(state: UIState | None = None) -> Flask:
    """Return a configured Flask application bound to *state*."""

    if state is None:
        state = UIState()

    app = Flask(
        __name__,
        template_folder=str(PACKAGE_ROOT / "templates"),
        static_folder=str(PACKAGE_ROOT / "static"),
    )  # type: ignore[arg-type]

    @app.get("/")
    def index() -> Any:
        if request.args.get("format", "").lower() == "json":
            status = state.snapshot_status()
            summary = state.snapshot_summary()
            discovery = state.snapshot_discovery()
            actions = state.snapshot_actions()
            activity = state.snapshot_activity()
            trades = state.snapshot_trades()
            logs = state.snapshot_logs()
            weights = state.snapshot_weights()
            config_summary = state.snapshot_config()
            return jsonify(
                {
                    "message": "SolHunter Zero UI",
                    "status": status,
                    "summary": summary,
                    "discovery": discovery,
                    "actions": actions,
                    "activity": activity,
                    "trades": trades,
                    "logs": logs,
                    "weights": weights,
                    "config_overview": config_summary,
                    "endpoints": [
                        "/health",
                        "/status",
                        "/summary",
                        "/tokens",
                        "/actions",
                        "/activity",
                        "/trades",
                        "/weights",
                        "/rl/status",
                        "/config",
                        "/logs",
                    ],
                }
            )

        status = state.snapshot_status()
        summary = state.snapshot_summary()
        discovery = state.snapshot_discovery()
        activity = state.snapshot_activity()
        trades = state.snapshot_trades()
        logs = state.snapshot_logs()
        weights_raw = state.snapshot_weights()
        weights = weights_raw if isinstance(weights_raw, dict) else {}
        actions = state.snapshot_actions()
        config_summary = state.snapshot_config()
        history = state.snapshot_history()

        counts = {
            "activity": len(activity),
            "trades": len(trades),
            "logs": len(logs),
            "weights": len(weights),
            "actions": len(actions),
        }
        heartbeat_value = status.get("heartbeat") or "n/a"
        iterations_completed_raw = (
            status.get("iterations_completed")
            or status.get("iterations")
            or status.get("iterations_complete")
        )
        try:
            iterations_completed = int(iterations_completed_raw)
        except (TypeError, ValueError):
            iterations_completed = 0
        trade_count_raw = status.get("trade_count")
        if trade_count_raw is None:
            trade_count_raw = len(trades)
        try:
            trade_count = int(trade_count_raw)
        except (TypeError, ValueError):
            trade_count = len(trades)
        last_elapsed = None
        if summary:
            elapsed_val = summary.get("elapsed_s")
            try:
                last_elapsed = float(elapsed_val) if elapsed_val is not None else None
            except (TypeError, ValueError):
                last_elapsed = None
        trades_per_iteration = (
            trade_count / iterations_completed if iterations_completed else 0.0
        )
        iteration_caption: str
        if iterations_completed:
            if last_elapsed is not None:
                iteration_caption = f"Last run {last_elapsed:.1f}s"
            else:
                iteration_caption = "Tracking iterations"
        else:
            iteration_caption = "Awaiting first iteration"
        trades_caption = (
            f"{trades_per_iteration:.2f} per iteration"
            if iterations_completed
            else "No iterations yet"
        )
        heartbeat_caption = (
            "Trading loop online"
            if status.get("trading_loop") or status.get("event_bus")
            else "Loop offline"
        )
        stat_tiles = [
            {
                "title": "Heartbeat",
                "value": heartbeat_value,
                "caption": heartbeat_caption,
                "icon": """
                    <svg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.6\" stroke-linecap=\"round\" stroke-linejoin=\"round\">
                        <path d=\"M4.318 6.318c-1.756 1.756-1.756 4.604 0 6.36L12 20.36l7.682-7.682c1.756-1.756 1.756-4.604 0-6.36-1.756-1.756-4.604-1.756-6.36 0L12 4.64l-1.322-1.322c-1.756-1.756-4.604-1.756-6.36 0z\" />
                        <polyline points=\"9 11.5 11 14 13 10 15 12\" />
                    </svg>
                """,
                "css_class": "heartbeat",
            },
            {
                "title": "Iterations",
                "value": f"{iterations_completed:,}",
                "caption": iteration_caption,
                "icon": """
                    <svg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.6\" stroke-linecap=\"round\" stroke-linejoin=\"round\">
                        <path d=\"M3 12a9 9 0 1 1 9 9\" />
                        <polyline points=\"3 3 3 9 9 9\" />
                        <path d=\"M12 7v5l3 2\" />
                    </svg>
                """,
                "css_class": "iterations",
            },
            {
                "title": "Trades",
                "value": f"{trade_count:,}",
                "caption": trades_caption,
                "icon": """
                    <svg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1.6\" stroke-linecap=\"round\" stroke-linejoin=\"round\">
                        <path d=\"M3 6h18\" />
                        <path d=\"M5 6v14h14V6\" />
                        <path d=\"M9 10h6\" />
                        <path d=\"M9 14h4\" />
                    </svg>
                """,
                "css_class": "trades",
            },
        ]
        weights_sample = dict(list(weights.items())[:10]) if weights else {}
        weight_items = []
        for name, value in weights.items():
            try:
                weight_value = float(value)
            except (TypeError, ValueError):
                continue
            weight_items.append((str(name), weight_value))
        weights_sorted = sorted(weight_items, key=lambda item: item[1], reverse=True)
        weights_labels = [name for name, _ in weights_sorted]
        weights_values = [value for _, value in weights_sorted]
        if weights_sorted:
            weights_aria_label = "Agent weights distribution: " + ", ".join(
                f"{name} weight {value}" for name, value in weights_sorted
            )
        else:
            weights_aria_label = "Agent weights unavailable"
        samples = {
            "activity": activity[-5:],
            "trades": trades[-5:],
            "logs": logs[-5:],
            "weights": weights_sample,
            "actions": actions[-5:],
        }
        discovery_recent_all = list(discovery.get("recent", []))
        discovery_recent_total = len(discovery_recent_all)
        discovery_recent_summary = list(
            reversed(discovery_recent_all[-3:])
        )
        discovery_recent_display = list(
            reversed(discovery_recent_all[-120:])
        )
        logs_all = list(logs)
        logs_total = len(logs_all)
        logs_summary = list(reversed(logs_all[-3:]))
        logs_display = list(reversed(logs_all[-200:]))
        config_overview = {
            "config_path": config_summary.get("config_path"),
            "agents": config_summary.get("agents"),
            "loop_delay": config_summary.get("loop_delay"),
            "min_delay": config_summary.get("min_delay"),
            "max_delay": config_summary.get("max_delay"),
        }
        context = {
            "status": status,
            "summary": summary,
            "discovery": discovery,
            "discovery_recent_display": discovery_recent_display,
            "discovery_recent_summary": discovery_recent_summary,
            "discovery_recent_total": discovery_recent_total,
            "counts": counts,
            "samples": samples,
            "config_overview": config_overview,
            "actions": actions,
            "logs_display": logs_display,
            "logs_summary": logs_summary,
            "logs_total": logs_total,
            "history": history,
            "weights": weights,
            "weights_sorted": weights_sorted,
            "weights_labels": weights_labels,
            "weights_values": weights_values,
            "weights_aria_label": weights_aria_label,
            "stat_tiles": stat_tiles,
            "dashboard_data": {
                "history": history,
                "weights_labels": weights_labels,
                "weights_values": weights_values,
            },
        }
        return render_template("dashboard.html", **context)


    @app.get("/health")
    def health() -> Any:
        return {"ok": True}

    @app.get("/status")
    def status() -> Any:
        data = state.snapshot_status()
        summary = state.snapshot_summary()
        discovery = state.snapshot_discovery()
        data.setdefault("activity_count", len(state.snapshot_activity()))
        data.setdefault("trade_count", len(state.snapshot_trades()))
        if summary:
            data.setdefault("last_iteration", {
                "timestamp": summary.get("timestamp"),
                "actions": summary.get("actions_count"),
                "discovered": summary.get("discovered_count"),
                "elapsed_s": summary.get("elapsed_s"),
            })
        data.setdefault("recent_tokens", discovery.get("recent", [])[:10])
        return jsonify(data)

    @app.get("/summary")
    def summary() -> Any:
        return jsonify(state.snapshot_summary())

    @app.get("/tokens")
    def tokens() -> Any:
        return jsonify(state.snapshot_discovery())

    @app.get("/actions")
    def actions() -> Any:
        return jsonify(state.snapshot_actions())

    @app.get("/activity")
    def activity() -> Any:
        return jsonify({"entries": state.snapshot_activity()})

    @app.get("/trades")
    def trades() -> Any:
        return jsonify(list(state.snapshot_trades()))

    @app.get("/weights")
    def weights() -> Any:
        return jsonify(state.snapshot_weights())

    @app.get("/rl/status")
    def rl_status() -> Any:
        return jsonify(state.snapshot_rl())

    @app.get("/config")
    def config() -> Any:
        return jsonify(state.snapshot_config())

    @app.get("/logs")
    def logs() -> Any:
        return jsonify({"entries": state.snapshot_logs()})

    @app.get("/discovery")
    def discovery_settings() -> Any:
        method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        if method is None:
            method = DEFAULT_DISCOVERY_METHOD
        return jsonify(
            {
                "method": method,
                "allowed_methods": sorted(DISCOVERY_METHODS),
            }
        )

    @app.post("/discovery")
    def update_discovery() -> Any:
        payload = request.get_json(silent=True) or {}
        raw_method = payload.get("method")
        if not isinstance(raw_method, str) or not raw_method.strip():
            return (
                jsonify(
                    {
                        "error": "method must be a non-empty string",
                        "allowed_methods": sorted(DISCOVERY_METHODS),
                    }
                ),
                400,
            )
        method = resolve_discovery_method(raw_method)
        if method is None:
            return (
                jsonify(
                    {
                        "error": f"Invalid discovery method: {raw_method}",
                        "allowed_methods": sorted(DISCOVERY_METHODS),
                    }
                ),
                400,
            )
        os.environ["DISCOVERY_METHOD"] = method
        return jsonify(
            {
                "status": "ok",
                "method": method,
                "allowed_methods": sorted(DISCOVERY_METHODS),
            }
        )

    @app.get("/__shutdown__")
    def _shutdown() -> Any:  # pragma: no cover - invoked via HTTP
        from flask import request

        func = request.environ.get("werkzeug.server.shutdown")
        if func is None:
            raise RuntimeError("Not running with the Werkzeug Server")
        func()
        return {"ok": True}

    return app


class UIServer:
    """Utility wrapper that runs the Flask app in a background thread."""

    def __init__(
        self,
        state: UIState,
        *,
        host: str = "127.0.0.1",
        port: int = 5000,
    ) -> None:
        self.state = state
        self.host = host
        self.port = int(port)
        self.app = create_app(state)
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _serve() -> None:
            try:
                # ``use_reloader`` must be False otherwise Flask tries to spawn
                # a new process.
                self.app.run(host=self.host, port=self.port, use_reloader=False)
            except Exception:  # pragma: no cover - best effort logging
                log.exception("UI server crashed")

        self._thread = threading.Thread(target=_serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._thread:
            return
        try:
            import urllib.request

            urllib.request.urlopen(
                f"http://{self.host}:{self.port}/__shutdown__", timeout=1
            )
        except Exception:
            pass
        if self._thread:
            self._thread.join(timeout=2)
        self._thread = None
