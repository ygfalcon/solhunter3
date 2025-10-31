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
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Optional

from flask import Flask, jsonify, render_template_string, request

from .agents.discovery import (
    DEFAULT_DISCOVERY_METHOD,
    DISCOVERY_METHODS,
    resolve_discovery_method,
)


log = logging.getLogger(__name__)


StatusProvider = Callable[[], Dict[str, Any]]
ListProvider = Callable[..., Iterable[Dict[str, Any]]]
DictProvider = Callable[[], Dict[str, Any]]


DEFAULT_HISTORY_WINDOW = int(os.getenv("UI_HISTORY_WINDOW", "120") or 120)
DEFAULT_HISTORY_PAGE_SIZE = int(os.getenv("UI_HISTORY_PAGE_SIZE", "200") or 200)
MAX_HISTORY_PAGE_SIZE = int(os.getenv("UI_HISTORY_MAX_PAGE_SIZE", "1000") or 1000)


def _history_timestamp(entry: Dict[str, Any]) -> float:
    ts = entry.get("timestamp_epoch")
    if isinstance(ts, (int, float)):
        try:
            return float(ts)
        except (TypeError, ValueError):
            return 0.0
    if isinstance(ts, str):
        try:
            return float(ts)
        except (TypeError, ValueError):
            pass
    stamp = entry.get("timestamp")
    if isinstance(stamp, str) and stamp:
        try:
            normalized = stamp[:-1] if stamp.endswith("Z") else stamp
            return datetime.fromisoformat(normalized).timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _filter_history_window(
    items: Iterable[Dict[str, Any]],
    *,
    limit: Optional[int] = None,
    since: Optional[float] = None,
    before: Optional[float] = None,
) -> List[Dict[str, Any]]:
    window = list(items)
    if since is not None:
        window = [entry for entry in window if _history_timestamp(entry) > since]
    if before is not None:
        window = [entry for entry in window if _history_timestamp(entry) < before]
    if limit is not None:
        try:
            limit_value = int(limit)
        except (TypeError, ValueError):
            limit_value = 0
        if limit_value <= 0:
            return []
        window = window[-limit_value:]
    return window


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

    def snapshot_history(
        self,
        *,
        limit: Optional[int] = None,
        since: Optional[float] = None,
        before: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        try:
            provider = self.history_provider
            if limit is None and since is None and before is None:
                data = provider()
            else:
                try:
                    data = provider(limit=limit, since=since, before=before)
                except TypeError:
                    data = provider()
            window = list(data)
            return _filter_history_window(
                window, limit=limit, since=since, before=before
            )
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

    app = Flask(__name__)  # type: ignore[arg-type]

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
                        "/history",
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
        history_window = max(1, DEFAULT_HISTORY_WINDOW)
        history = state.snapshot_history(limit=history_window)
        history_page_size = max(1, min(DEFAULT_HISTORY_PAGE_SIZE, MAX_HISTORY_PAGE_SIZE))

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
        template = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <meta http-equiv="refresh" content="5" />
            <title>SolHunter Zero Dashboard</title>
            <style>
                :root {
                    color-scheme: dark;
                    --bg: #0d1117;
                    --panel: #161b22;
                    --border: #30363d;
                    --text: #e6edf3;
                    --muted: #8b949e;
                    --accent: #58a6ff;
                    --danger: #ff7b72;
                    --success: #3fb950;
                }
                body {
                    margin: 0;
                    padding: 24px;
                    font-family: "Inter", "SF Pro Display", -apple-system, BlinkMacSystemFont, sans-serif;
                    background: linear-gradient(160deg, #05070d 0%, #0d1117 40%, #05070d 100%);
                    color: var(--text);
                }
                h1, h2, h3 {
                    margin-top: 0;
                    font-weight: 600;
                }
                a { color: var(--accent); text-decoration: none; }
                a:hover { text-decoration: underline; }
                .grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
                    gap: 20px;
                }
                .panel {
                    background: rgba(22, 27, 34, 0.85);
                    border: 1px solid var(--border);
                    border-radius: 18px;
                    padding: 20px;
                    box-shadow: 0 18px 50px rgba(0, 0, 0, 0.35);
                    backdrop-filter: blur(10px);
                }
                .panel h2 {
                    border-bottom: 1px solid rgba(88, 166, 255, 0.2);
                    padding-bottom: 8px;
                    margin-bottom: 16px;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.95rem;
                }
                th, td {
                    text-align: left;
                    padding: 6px 4px;
                    border-bottom: 1px solid rgba(48, 54, 61, 0.6);
                }
                th { color: var(--muted); font-weight: 500; }
                .badge {
                    display: inline-block;
                    padding: 4px 8px;
                    border-radius: 999px;
                    background-color: rgba(88, 166, 255, 0.12);
                    color: var(--accent);
                    font-size: 0.8rem;
                    margin-right: 6px;
                }
                .badge.danger { color: var(--danger); background: rgba(255, 123, 114, 0.12); }
                .badge.success { color: var(--success); background: rgba(63, 185, 80, 0.12); }
                .badge.disabled { color: var(--muted); background: rgba(139, 148, 158, 0.18); }
                ul { padding-left: 18px; margin: 0; }
                li { margin-bottom: 6px; }
                .muted { color: var(--muted); font-size: 0.88rem; }
                .endpoint-list { display: flex; flex-wrap: wrap; gap: 10px; }
                .endpoint-list a {
                    padding: 6px 10px;
                    border-radius: 12px;
                    background: rgba(88, 166, 255, 0.12);
                    border: 1px solid rgba(88, 166, 255, 0.2);
                }
                .two-column {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                    gap: 20px;
                }
                pre {
                    background: rgba(13, 17, 23, 0.8);
                    border: 1px solid var(--border);
                    border-radius: 12px;
                    padding: 16px;
                    overflow: auto;
                    max-height: 360px;
                }
                @keyframes pulseGlow {
                    0% { text-shadow: 0 0 0 rgba(88, 166, 255, 0.0); }
                    40% { text-shadow: 0 0 12px rgba(88, 166, 255, 0.7); }
                    70% { text-shadow: 0 0 8px rgba(88, 166, 255, 0.4); }
                    100% { text-shadow: 0 0 0 rgba(88, 166, 255, 0.0); }
                }
                header {
                    display: grid;
                    grid-template-columns: 1fr auto;
                    align-items: start;
                    gap: 20px;
                    margin-bottom: 24px;
                }
                header h1 { font-size: 1.8rem; margin-bottom: 12px; }
                header .meta { text-align: right; font-size: 0.9rem; color: var(--muted); }
                header .headline { display: flex; flex-direction: column; gap: 12px; }
                header .stat-tiles {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                    gap: 14px;
                }
                .stat-tile {
                    position: relative;
                    overflow: hidden;
                    border-radius: 16px;
                    padding: 14px 16px;
                    border: 1px solid rgba(88, 166, 255, 0.15);
                    background: linear-gradient(135deg, rgba(13, 17, 23, 0.9), rgba(23, 32, 45, 0.8));
                    box-shadow: 0 12px 32px rgba(0, 0, 0, 0.35);
                    display: grid;
                    grid-template-columns: auto 1fr;
                    gap: 12px;
                    align-items: center;
                }
                .stat-tile::before {
                    content: "";
                    position: absolute;
                    inset: -40% -60% auto -60%;
                    height: 140%;
                    background: radial-gradient(circle at top, rgba(88, 166, 255, 0.45), transparent 65%);
                    transform: rotate(12deg);
                    pointer-events: none;
                }
                .stat-tile.heartbeat::before {
                    background: radial-gradient(circle at top, rgba(255, 123, 114, 0.55), transparent 65%);
                }
                .stat-icon {
                    width: 44px;
                    height: 44px;
                    border-radius: 12px;
                    background: linear-gradient(160deg, rgba(88, 166, 255, 0.18), rgba(88, 166, 255, 0));
                    display: grid;
                    place-items: center;
                    color: var(--accent);
                    box-shadow: inset 0 0 12px rgba(88, 166, 255, 0.25);
                }
                .stat-icon svg { width: 26px; height: 26px; }
                .stat-content { position: relative; z-index: 1; }
                .stat-label { font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.08em; color: var(--muted); }
                .stat-value { font-size: 1.5rem; font-weight: 600; margin-top: 4px; }
                .stat-caption { font-size: 0.85rem; color: rgba(230, 237, 243, 0.75); margin-top: 4px; }
                .heartbeat-value { animation: pulseGlow 2.8s ease-in-out infinite; color: var(--accent); }
                @media (max-width: 900px) {
                    header { grid-template-columns: 1fr; }
                    header .meta { text-align: left; }
                }
                @media (max-width: 540px) {
                    body { padding: 18px 16px; }
                    header .stat-tiles { grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); }
                }
                @media (max-width: 360px) {
                    header .stat-tiles { grid-template-columns: 1fr; }
                    .stat-tile { grid-template-columns: 1fr; }
                    .stat-icon { width: 38px; height: 38px; }
                }
                .status-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                    gap: 12px;
                    margin-top: 12px;
                }
                .status-card {
                    background: rgba(13, 17, 23, 0.7);
                    border: 1px solid var(--border);
                    border-radius: 14px;
                    padding: 12px;
                }
                details.collapsible {
                    border: 1px solid rgba(88, 166, 255, 0.18);
                    border-radius: 14px;
                    background: rgba(13, 17, 23, 0.6);
                    transition: border-color 0.3s ease, box-shadow 0.3s ease;
                    position: relative;
                }
                details.collapsible:hover,
                details.collapsible:focus-within {
                    border-color: rgba(88, 166, 255, 0.45);
                    box-shadow: 0 0 0 1px rgba(88, 166, 255, 0.15), 0 12px 40px rgba(45, 104, 255, 0.18);
                }
                details.collapsible summary {
                    list-style: none;
                    cursor: pointer;
                    padding: 14px 18px;
                    display: flex;
                    align-items: center;
                    gap: 16px;
                    user-select: none;
                }
                .weights-chart-container {
                    position: relative;
                    min-height: 220px;
                }
                .weights-chart-container canvas {
                    display: block;
                    max-width: 420px;
                    margin: 0 auto;
                }
                .weights-legend {
                    margin-top: 18px;
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: center;
                    gap: 10px;
                }
                .weights-legend span {
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;
                    padding: 6px 12px;
                    border-radius: 999px;
                    background: rgba(88, 166, 255, 0.12);
                    border: 1px solid rgba(88, 166, 255, 0.25);
                    color: var(--text);
                    font-size: 0.85rem;
                }
                .weights-legend .legend-dot {
                    width: 10px;
                    height: 10px;
                    border-radius: 50%;
                    background: var(--accent);
                }
                .weights-legend .legend-value {
                    color: var(--muted);
                    font-variant-numeric: tabular-nums;
                }
                .sr-only {
                    position: absolute;
                    width: 1px;
                    height: 1px;
                    padding: 0;
                    margin: -1px;
                    overflow: hidden;
                    clip: rect(0, 0, 0, 0);
                    white-space: nowrap;
                    border: 0;
                }
                details.collapsible summary::-webkit-details-marker { display: none; }
                details.collapsible summary:focus-visible {
                    outline: 2px solid var(--accent);
                    outline-offset: 4px;
                }
                .collapsible-summary {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 12px;
                    flex: 1;
                    align-items: center;
                }
                .summary-stack {
                    display: flex;
                    flex-direction: column;
                    gap: 4px;
                    min-width: 160px;
                }
                .summary-title {
                    font-size: 0.75rem;
                    letter-spacing: 0.08em;
                    text-transform: uppercase;
                    color: var(--muted);
                }
                .summary-count {
                    font-size: 0.95rem;
                    color: var(--accent);
                    font-weight: 600;
                }
                details.collapsible[open] .summary-count {
                    color: #8dbbff;
                }
                .summary-peek {
                    display: flex;
                    align-items: center;
                    flex-wrap: wrap;
                    gap: 8px;
                    font-size: 0.85rem;
                    color: rgba(230, 237, 243, 0.8);
                }
                .peek-chip {
                    padding: 4px 8px;
                    border-radius: 999px;
                    background: rgba(88, 166, 255, 0.12);
                    border: 1px solid rgba(88, 166, 255, 0.2);
                    box-shadow: 0 0 8px rgba(88, 166, 255, 0.2);
                    white-space: nowrap;
                }
                .caret {
                    margin-left: auto;
                    width: 14px;
                    height: 14px;
                    position: relative;
                }
                .caret::before {
                    content: "";
                    position: absolute;
                    inset: 0;
                    border-right: 2px solid var(--accent);
                    border-bottom: 2px solid var(--accent);
                    transform: rotate(-45deg);
                    transform-origin: center;
                    transition: transform 0.35s ease, box-shadow 0.35s ease;
                    box-shadow: 0 0 10px rgba(88, 166, 255, 0.6);
                }
                details.collapsible[open] .caret::before {
                    transform: rotate(45deg);
                    box-shadow: 0 0 14px rgba(88, 166, 255, 0.85);
                }
                .collapsible-body {
                    max-height: 0;
                    opacity: 0;
                    overflow: hidden;
                    transition: max-height 0.4s ease, opacity 0.4s ease, padding 0.4s ease;
                    padding: 0 18px;
                }
                details.collapsible[open] .collapsible-body {
                    max-height: 520px;
                    opacity: 1;
                    padding: 12px 18px 18px;
                }
                .collapsible-scroll {
                    max-height: 320px;
                    overflow-y: auto;
                    padding-right: 6px;
                    margin-top: 10px;
                }
                .chart-grid {
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
                    gap: 20px;
                }
                .button {
                    background: rgba(22, 27, 34, 0.85);
                    border: 1px solid var(--border);
                    border-radius: 12px;
                    color: var(--text);
                    cursor: pointer;
                    font: inherit;
                    padding: 8px 16px;
                    transition: background 0.2s ease, border-color 0.2s ease;
                }
                .button:hover {
                    background: rgba(88, 166, 255, 0.12);
                    border-color: rgba(88, 166, 255, 0.6);
                }
                .button:disabled {
                    opacity: 0.5;
                    cursor: default;
                }
                .chip-group {
                    display: flex;
                    flex-wrap: wrap;
                    gap: 8px;
                    margin-top: 10px;
                }
            </style>
            <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        </head>
        <body>
            <header>
                <div class="headline">
                    <h1>SolHunter Zero Dashboard</h1>
                    <div class="stat-tiles">
                        {% for tile in stat_tiles %}
                            <div class="stat-tile {{ tile.css_class }}">
                                <div class="stat-icon" aria-hidden="true">{{ tile.icon | safe }}</div>
                                <div class="stat-content">
                                    <div class="stat-label">{{ tile.title }}</div>
                                    <div class="stat-value {% if tile.css_class == 'heartbeat' %}heartbeat-value{% endif %}">{{ tile.value }}</div>
                                    <div class="stat-caption">{{ tile.caption }}</div>
                                </div>
                            </div>
                        {% endfor %}
                    </div>
                </div>
                <div class="meta">
                    <div>Auto-refreshing every 5s</div>
                    <div>JSON view: <a href="/?format=json">/?format=json</a></div>
                </div>
            </header>

            <section class="grid">
                <div class="panel">
                    <h2>Status</h2>
                    <div class="status-grid">
                        {% for key, value in status.items() %}
                            {% if key not in ('recent_tokens', 'last_iteration', 'iterations_completed', 'trade_count', 'activity_count', 'heartbeat', 'pipeline_tokens', 'pipeline_size', 'rl_daemon_status') %}
                                <div class="status-card">
                                    <div class="muted">{{ key }}</div>
                                    <div style="font-size:1.2rem; font-weight:600; margin-top:4px;">
                                        {% if key == 'rl_daemon' and status.get('rl_daemon_status') %}
                                            {% set rl = status.get('rl_daemon_status') %}
                                            {% if not rl.get('enabled', False) %}
                                                <span class="badge disabled">DISABLED</span>
                                            {% else %}
                                                <span class="badge {{ 'success' if rl.get('running') else 'danger' }}">{{ 'ONLINE' if rl.get('running') else 'OFFLINE' }}</span>
                                            {% endif %}
                                        {% elif value in (True, False) %}
                                            <span class="badge {{ 'success' if value else 'danger' }}">{{ 'ONLINE' if value else 'OFFLINE' }}</span>
                                        {% else %}
                                            {{ value if value is not none else '—' }}
                                        {% endif %}
                                    </div>
                                    {% if key == 'rl_daemon' and status.get('rl_daemon_status') %}
                                        {% set rl = status.get('rl_daemon_status') %}
                                        {% if rl.get('source') or rl.get('error') %}
                                            <div class="muted" style="font-size:0.75rem; margin-top:4px;">
                                                {% if rl.get('source') %}{{ rl.get('source').capitalize() }} daemon{% endif %}
                                                {% if rl.get('error') %}
                                                    {% if rl.get('source') %} · {% endif %}{{ rl.get('error') }}
                                                {% endif %}
                                            </div>
                                        {% endif %}
                                    {% endif %}
                                </div>
                            {% endif %}
                        {% endfor %}
                    </div>
                    {% if status.get('last_iteration') %}
                        <div style="margin-top:16px;">
                            <div class="muted">Last iteration</div>
                            <div style="margin-top:6px;">Timestamp: {{ status['last_iteration'].get('timestamp') or 'n/a' }}</div>
                            <div>Actions: {{ status['last_iteration'].get('actions') or 0 }} · Discovered: {{ status['last_iteration'].get('discovered') or 0 }} · Duration: {{ status['last_iteration'].get('elapsed_s') or 0 }}s</div>
                            <div>Fallback used: {{ 'Yes' if status['last_iteration'].get('fallback_used') else 'No' }}</div>
                        </div>
                    {% endif %}
                    {% if status.get('pipeline_tokens') %}
                        <div style="margin-top:16px;">
                            <div class="muted">Pipeline ({{ status.get('pipeline_size', 0) }} queued)</div>
                            <div style="display:flex; flex-wrap:wrap; gap:6px; margin-top:6px;">
                                {% for token in status.get('pipeline_tokens')[:12] %}
                                    <span class="badge">{{ token }}</span>
                                {% endfor %}
                            </div>
                        </div>
                    {% endif %}
                </div>

                <div class="panel">
                    <h2>Iteration Summary</h2>
                    {% if summary %}
                        <table>
                            <tr><th>Timestamp</th><td>{{ summary.get('timestamp') }}</td></tr>
                            <tr><th>Elapsed</th><td>{{ summary.get('elapsed_s') or '—' }} s</td></tr>
                            <tr><th>Actions</th><td>{{ summary.get('actions_count') }}</td></tr>
                            <tr><th>Any trade</th><td>{{ 'Yes' if summary.get('any_trade') else 'No' }}</td></tr>
                            <tr><th>Discovered</th><td>{{ summary.get('discovered_count') }}</td></tr>
                            <tr><th>Picked Tokens</th><td>{{ summary.get('picked_tokens') }}</td></tr>
                            <tr><th>Committed</th><td>{{ 'Yes' if summary.get('committed') else 'No' }}</td></tr>
                        </table>
                        {% set telemetry = summary.get('telemetry') or {} %}
                        {% if telemetry %}
                            <div style="margin-top:12px;">
                                <div class="muted">Telemetry</div>
                                <table style="margin-top:6px;">
                                    {% if telemetry.get('evaluation') %}
                                        <tr>
                                            <th>Eval Workers</th>
                                            <td>{{ telemetry['evaluation'].get('workers') }} · avg {{ '%.2f'|format(telemetry['evaluation'].get('latency_avg', 0)) }}s · max {{ '%.2f'|format(telemetry['evaluation'].get('latency_max', 0)) }}s</td>
                                        </tr>
                                        <tr><th>Evaluations</th><td>{{ telemetry['evaluation'].get('completed') }}</td></tr>
                                    {% endif %}
                                    {% if telemetry.get('execution') %}
                                        <tr>
                                            <th>Execution Lanes</th>
                                            <td>
                                                {% for lane, size in (telemetry['execution'].get('lanes') or {}).items() %}
                                                    <span class="badge">{{ lane }}: {{ size }}</span>
                                                {% else %}
                                                    none
                                                {% endfor %}
                                            </td>
                                        </tr>
                                        <tr><th>Submitted</th><td>{{ telemetry['execution'].get('submitted') }} · workers {{ telemetry['execution'].get('lane_workers') }}</td></tr>
                                    {% endif %}
                                    {% if telemetry.get('pipeline') %}
                                        <tr><th>Queued</th><td>{{ telemetry['pipeline'].get('queued') }} / {{ telemetry['pipeline'].get('limit') }}</td></tr>
                                    {% endif %}
                                </table>
                            </div>
                        {% endif %}
                        {% if summary.get('errors') %}
                            <div style="margin-top:12px;">
                                <div class="muted">Errors</div>
                                <ul>
                                    {% for err in summary.get('errors') %}
                                        <li style="color: var(--danger);">{{ err }}</li>
                                    {% endfor %}
                                </ul>
                            </div>
                        {% endif %}
                    {% else %}
                        <div class="muted">Trading loop has not completed an iteration yet.</div>
                    {% endif %}
                </div>

                <div class="panel">
                    <h2>Discovery</h2>
                    <details class="collapsible">
                        <summary>
                            <div class="collapsible-summary">
                                <div class="summary-stack">
                                    <div class="summary-title">Recent Tokens</div>
                                    <div class="summary-count">{{ discovery_recent_total }} tracked</div>
                                </div>
                                <div class="summary-peek" aria-hidden="true">
                                    {% if discovery_recent_summary %}
                                        {% for token in discovery_recent_summary %}
                                            <span class="peek-chip">{{ token }}</span>
                                        {% endfor %}
                                    {% else %}
                                        <span class="muted">Waiting for discovery results…</span>
                                    {% endif %}
                                </div>
                            </div>
                            <span class="caret" aria-hidden="true"></span>
                        </summary>
                        <div class="collapsible-body">
                            {% if discovery_recent_display %}
                                <div class="muted">Newest {{ discovery_recent_display|length }} tokens shown below.</div>
                                <div class="collapsible-scroll">
                                    <ul>
                                        {% for token in discovery_recent_display %}
                                            <li>{{ token }}</li>
                                        {% endfor %}
                                    </ul>
                                </div>
                            {% else %}
                                <div class="muted">Waiting for discovery results…</div>
                            {% endif %}
                            {% if discovery.get('latest_iteration_tokens') %}
                                <div class="muted" style="margin-top:14px;">Latest iteration tokens ({{ discovery.get('latest_iteration_tokens')|length }}):</div>
                                <div class="chip-group" role="list">
                                    {% for token in discovery.get('latest_iteration_tokens')[:20] %}
                                        <span class="peek-chip" role="listitem">{{ token }}</span>
                                    {% endfor %}
                                </div>
                            {% endif %}
                        </div>
                    </details>
                </div>

                <div class="panel">
                    <h2>Counts</h2>
                    <table>
                        {% for key, val in counts.items() %}
                            <tr><th>{{ key }}</th><td>{{ val }}</td></tr>
                        {% endfor %}
                    </table>
                    <div style="margin-top: 14px;" class="muted">Endpoints</div>
                    <div class="endpoint-list">
                        {% for link in ['health','status','summary','tokens','actions','activity','trades','weights','rl/status','config','logs','history'] %}
                            <a href="/{{ link }}">/{{ link }}</a>
                        {% endfor %}
                    </div>
                </div>
            </section>

            <section class="grid" style="margin-top:24px;">
                <div class="panel" style="grid-column: span 2;">
                    <h2>Iteration Charts</h2>
                    <div id="historyChartsPlaceholder" class="muted"{% if history %} style="display:none;"{% endif %}>
                        Waiting for iteration history…
                    </div>
                    <div id="historyChartsGrid" class="chart-grid"{% if not history %} style="display:none;"{% endif %}>
                        <canvas id="actionsChart" height="180"></canvas>
                        <canvas id="latencyChart" height="180"></canvas>
                    </div>
                </div>
            </section>

            <section class="grid" style="margin-top:24px;">
                <div class="panel" style="grid-column: span 2;">
                    <details class="collapsible" id="historyDetails">
                        <summary>
                            <div class="collapsible-summary">
                                <div class="summary-stack">
                                    <div class="summary-title">Iteration History</div>
                                    <div class="summary-count" id="historySummaryCount">Tap to load</div>
                                </div>
                                <div class="summary-peek" aria-hidden="true">
                                    <span class="muted" id="historySummaryPeek">History loads on demand</span>
                                </div>
                            </div>
                            <span class="caret" aria-hidden="true"></span>
                        </summary>
                        <div class="collapsible-body">
                            <div class="muted" id="historyListStatus">History has not been loaded yet.</div>
                            <div class="collapsible-scroll" id="historyTableContainer" style="display:none;">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Timestamp</th>
                                            <th>Actions</th>
                                            <th>Discovered</th>
                                            <th>Committed</th>
                                            <th>Elapsed (s)</th>
                                        </tr>
                                    </thead>
                                    <tbody id="historyTableBody"></tbody>
                                </table>
                            </div>
                            <div style="margin-top:12px; display:flex; gap:12px;">
                                <button class="button" id="historyLoadMore" type="button" disabled>Load older iterations</button>
                                <div class="muted" id="historyLoadHint" style="align-self:center;"></div>
                            </div>
                        </div>
                    </details>
                </div>
            </section>

            <section class="grid" style="margin-top:24px;">
                <div class="panel">
                    <h2>Token Results</h2>
                    {% if summary and summary.get('token_results') %}
                        <table>
                            <tr><th>Token</th><th>Actions</th><th>Errors</th><th>Score</th></tr>
                            {% for result in summary.get('token_results')[:15] %}
                                <tr>
                                    <td>{{ result.get('token') }}</td>
                                    <td>{{ result.get('actions')|length }}</td>
                                    <td>{{ result.get('errors')|length }}</td>
                                    <td>{{ '%.3f'|format(result.get('score', 0)) }}</td>
                                </tr>
                            {% endfor %}
                        </table>
                    {% else %}
                        <div class="muted">No token results captured yet.</div>
                    {% endif %}
                </div>

                <div class="panel">
                    <h2>Recent Actions</h2>
                    {% if actions %}
                        <table>
                            <tr><th>Agent</th><th>Token</th><th>Side</th><th>Amount</th><th>Result</th></tr>
                            {% for action in actions[-15:]|reverse %}
                                <tr>
                                    <td>{{ action.get('agent') or '—' }}</td>
                                    <td>{{ action.get('token') or '—' }}</td>
                                    <td>{{ action.get('side') or '—' }}</td>
                                    <td>{{ action.get('amount') or '—' }}</td>
                                    <td>{{ action.get('result') or '—' }}</td>
                                </tr>
                            {% endfor %}
                        </table>
                    {% else %}
                        <div class="muted">No actions recorded yet.</div>
                    {% endif %}
                </div>

                <div class="panel">
                    <h2>Event Log</h2>
                    <details class="collapsible">
                        <summary>
                            <div class="collapsible-summary">
                                <div class="summary-stack">
                                    <div class="summary-title">Latest Events</div>
                                    <div class="summary-count">{{ logs_total }} recorded</div>
                                </div>
                                <div class="summary-peek" aria-hidden="true">
                                    {% if logs_summary %}
                                        {% for entry in logs_summary %}
                                            {% set stage = entry.get('payload', {}).get('stage', entry.get('topic')) or '—' %}
                                            <span class="peek-chip">{{ stage }}</span>
                                        {% endfor %}
                                    {% else %}
                                        <span class="muted">No log entries yet.</span>
                                    {% endif %}
                                </div>
                            </div>
                            <span class="caret" aria-hidden="true"></span>
                        </summary>
                        <div class="collapsible-body">
                            {% if logs_display %}
                                <div class="muted">Showing the freshest {{ logs_display|length }} entries.</div>
                                <div class="collapsible-scroll">
                                    <ul>
                                        {% for entry in logs_display %}
                                            {% set detail = entry.get('payload', {}).get('detail') or entry %}
                                            {% set stage = entry.get('payload', {}).get('stage', entry.get('topic')) or '—' %}
                                            <li><span class="muted">{{ entry.get('timestamp') }}</span> · <strong>{{ stage }}</strong> — {{ detail }}</li>
                                        {% endfor %}
                                    </ul>
                                </div>
                            {% else %}
                                <div class="muted">No log entries yet.</div>
                            {% endif %}
                        </div>
                    </details>
                </div>

                <div class="panel">
                    <h2>Weights</h2>
                    {% if weights_labels %}
                        <div class="weights-chart-container">
                            <canvas id="weightsChart" role="img" aria-label="{{ weights_aria_label }}" data-summary="{{ weights_aria_label }}"></canvas>
                        </div>
                        <div class="weights-legend" id="weightsLegend" role="list">
                            {% for name, value in weights_sorted %}
                                <span role="listitem" aria-label="{{ name }} weight {{ '%.6f'|format(value) }}" data-index="{{ loop.index0 }}">
                                    <span class="legend-dot" aria-hidden="true"></span>
                                    <span class="legend-label">{{ name }}</span>
                                    <span class="legend-value" aria-hidden="true">{{ '%.4f'|format(value) }}</span>
                                </span>
                            {% endfor %}
                        </div>
                        <div class="sr-only" id="weightsTextSummary">{{ weights_aria_label }}</div>
                    {% else %}
                        <div class="muted">Weights unavailable. The coordinator has not provided agent weights yet.</div>
                    {% endif %}
                </div>

                <div class="panel">
                    <h2>Configuration</h2>
                    <div class="muted">Active agents:</div>
                    <div style="margin:10px 0;">
                        {% for agent in config_overview.get('agents') or [] %}
                            <span class="badge">{{ agent }}</span>
                        {% endfor %}
                    </div>
                    <div class="two-column">
                        <div>
                            <div class="muted">Loop delay</div>
                            <div>{{ config_overview.get('loop_delay') }}s</div>
                        </div>
                        <div>
                            <div class="muted">Min delay</div>
                            <div>{{ config_overview.get('min_delay') }}s</div>
                        </div>
                        <div>
                            <div class="muted">Max delay</div>
                            <div>{{ config_overview.get('max_delay') }}s</div>
                        </div>
                        <div>
                            <div class="muted">Config path</div>
                            <div style="word-break: break-all;">{{ config_overview.get('config_path') }}</div>
                        </div>
                    </div>
                </div>
            </section>

            <section class="panel" style="margin-top:24px;">
                <h2>Raw Summary JSON</h2>
                <pre>{{ summary | tojson(indent=2) }}</pre>
            </section>
            <script>
            (function() {
                const historySeed = {{ history | tojson | safe }};
                const historyChartLimit = {{ history_window }};
                const historyPageSize = {{ history_page_size }};
                const historyEndpoint = '/history';
                const weightLabels = {{ weights_labels | tojson | safe }};
                const weightValues = {{ weights_values | tojson | safe }};
                const chartElements = {
                    grid: document.getElementById('historyChartsGrid'),
                    placeholder: document.getElementById('historyChartsPlaceholder'),
                    actions: document.getElementById('actionsChart'),
                    latency: document.getElementById('latencyChart'),
                };
                const chartState = { actions: null, latency: null };
                let chartHistory = Array.isArray(historySeed) ? historySeed : [];

                function updateHistoryCharts(historyData) {
                    const hasData = Array.isArray(historyData) && historyData.length;
                    if (!hasData) {
                        if (chartElements.placeholder) {
                            chartElements.placeholder.style.display = '';
                        }
                        if (chartElements.grid) {
                            chartElements.grid.style.display = 'none';
                        }
                        if (chartState.actions) {
                            chartState.actions.destroy();
                            chartState.actions = null;
                        }
                        if (chartState.latency) {
                            chartState.latency.destroy();
                            chartState.latency = null;
                        }
                        return;
                    }

                    if (chartElements.placeholder) {
                        chartElements.placeholder.style.display = 'none';
                    }
                    if (chartElements.grid) {
                        chartElements.grid.style.display = '';
                    }

                    const labels = historyData.map(h => (h.timestamp || '').slice(11, 19));
                    const actionsData = historyData.map(h => h.actions_count || 0);
                    const discoveredData = historyData.map(h => h.discovered_count || 0);
                    const committedData = historyData.map(h => (h.committed ? 1 : 0));
                    const latencyData = historyData.map(h => (h.elapsed_s || 0));
                    const budgetData = historyData.map(h => {
                        const telemetry = h && h.telemetry ? h.telemetry : {};
                        const pipeline = telemetry && telemetry.pipeline ? telemetry.pipeline : {};
                        return typeof pipeline.budget === 'number' ? pipeline.budget : null;
                    });
                    const wantsBudget = budgetData.some(v => typeof v === 'number');

                    if (chartState.actions) {
                        chartState.actions.destroy();
                    }
                    if (chartElements.actions && window.Chart) {
                        chartState.actions = new Chart(chartElements.actions.getContext('2d'), {
                            type: 'line',
                            data: {
                                labels,
                                datasets: [
                                    {
                                        label: 'Actions',
                                        data: actionsData,
                                        borderColor: '#58a6ff',
                                        backgroundColor: 'rgba(88,166,255,0.2)',
                                        tension: 0.3,
                                    },
                                    {
                                        label: 'Discovered',
                                        data: discoveredData,
                                        borderColor: '#3fb950',
                                        backgroundColor: 'rgba(63,185,80,0.2)',
                                        tension: 0.3,
                                    },
                                    {
                                        label: 'Committed',
                                        data: committedData,
                                        borderColor: '#ffdf5d',
                                        backgroundColor: 'rgba(255,223,93,0.2)',
                                        tension: 0.1,
                                        yAxisID: 'y2',
                                        stepped: true,
                                    },
                                ],
                            },
                            options: {
                                plugins: {
                                    legend: { labels: { color: '#e6edf3' } },
                                },
                                scales: {
                                    x: {
                                        ticks: { color: '#8b949e' },
                                        grid: { color: 'rgba(48,54,61,0.4)' },
                                    },
                                    y: {
                                        ticks: { color: '#8b949e' },
                                        grid: { color: 'rgba(48,54,61,0.4)' },
                                    },
                                    y2: {
                                        position: 'right',
                                        ticks: { color: '#8b949e', callback: value => (value ? 'Yes' : 'No') },
                                        grid: { display: false },
                                        suggestedMax: 1,
                                        suggestedMin: 0,
                                    },
                                },
                            },
                        });
                    }

                    if (chartState.latency) {
                        chartState.latency.destroy();
                    }
                    if (chartElements.latency && window.Chart) {
                        const datasets = [
                            {
                                label: 'Iteration Seconds',
                                data: latencyData,
                                borderColor: '#ff7b72',
                                backgroundColor: 'rgba(255,123,114,0.25)',
                                tension: 0.2,
                            },
                        ];
                        if (wantsBudget) {
                            datasets.push({
                                label: 'Budget',
                                data: budgetData,
                                borderColor: '#8b949e',
                                borderDash: [6, 6],
                                fill: false,
                            });
                        }
                        chartState.latency = new Chart(chartElements.latency.getContext('2d'), {
                            type: 'line',
                            data: { labels, datasets },
                            options: {
                                plugins: { legend: { labels: { color: '#e6edf3' } } },
                                scales: {
                                    x: { ticks: { color: '#8b949e' }, grid: { color: 'rgba(48,54,61,0.4)' } },
                                    y: { ticks: { color: '#8b949e' }, grid: { color: 'rgba(48,54,61,0.4)' } },
                                },
                            },
                        });
                    }
                }

                updateHistoryCharts(chartHistory);

                function refreshChartHistory() {
                    const params = new URLSearchParams();
                    params.set('limit', historyChartLimit);
                    fetch(`${historyEndpoint}?${params.toString()}`)
                        .then(response => (response.ok ? response.json() : null))
                        .then(payload => {
                            if (!payload || !Array.isArray(payload.items)) {
                                return;
                            }
                            chartHistory = payload.items;
                            updateHistoryCharts(chartHistory);
                        })
                        .catch(() => {});
                }

                refreshChartHistory();

                const historyDetails = document.getElementById('historyDetails');
                const historyTableBody = document.getElementById('historyTableBody');
                const historyTableContainer = document.getElementById('historyTableContainer');
                const historyListStatus = document.getElementById('historyListStatus');
                const historyLoadMore = document.getElementById('historyLoadMore');
                const historyLoadHint = document.getElementById('historyLoadHint');
                const historySummaryCount = document.getElementById('historySummaryCount');
                const historySummaryPeek = document.getElementById('historySummaryPeek');
                let historyListCursor = null;
                let historyHasMore = false;
                let historyListLoaded = false;
                let historyListLatestEntry = null;

                function formatTimestamp(entry) {
                    if (!entry) {
                        return '—';
                    }
                    if (entry.timestamp) {
                        return entry.timestamp;
                    }
                    const epoch = entry.timestamp_epoch;
                    if (typeof epoch === 'number' && !Number.isNaN(epoch)) {
                        try {
                            return new Date(epoch * 1000).toISOString();
                        } catch (err) {
                            return String(epoch);
                        }
                    }
                    return '—';
                }

                function formatNumber(value, fractionDigits) {
                    if (typeof value === 'number' && Number.isFinite(value)) {
                        if (typeof fractionDigits === 'number') {
                            return value.toFixed(fractionDigits);
                        }
                        return value.toString();
                    }
                    if (value === 0) {
                        return '0';
                    }
                    return value != null ? String(value) : '—';
                }

                function appendHistoryRows(items, append) {
                    if (!historyTableBody || !Array.isArray(items) || !items.length) {
                        if (!append && historyTableBody) {
                            historyTableBody.innerHTML = '';
                        }
                        return;
                    }
                    const fragment = document.createDocumentFragment();
                    const rows = items.slice().reverse();
                    rows.forEach(item => {
                        const tr = document.createElement('tr');
                        const committedLabel = item && item.committed ? 'Yes' : 'No';
                        const cells = [
                            formatTimestamp(item),
                            formatNumber(item ? item.actions_count : null),
                            formatNumber(item ? item.discovered_count : null),
                            committedLabel,
                            formatNumber(item ? item.elapsed_s : null, 2),
                        ];
                        cells.forEach(text => {
                            const td = document.createElement('td');
                            td.textContent = text;
                            tr.appendChild(td);
                        });
                        fragment.appendChild(tr);
                    });
                    if (!append) {
                        historyTableBody.innerHTML = '';
                    }
                    historyTableBody.appendChild(fragment);
                }

                function updateHistorySummary(payload) {
                    const totalRows = historyTableBody ? historyTableBody.children.length : 0;
                    if (historySummaryCount) {
                        if (totalRows) {
                            const total = payload && typeof payload.total === 'number' && payload.total >= totalRows
                                ? payload.total
                                : null;
                            historySummaryCount.textContent = total
                                ? `${totalRows} of ${total} shown`
                                : `${totalRows} shown`;
                        } else {
                            historySummaryCount.textContent = 'Tap to load';
                        }
                    }
                    if (historySummaryPeek) {
                        if (historyListLatestEntry) {
                            historySummaryPeek.textContent = `Latest: ${formatTimestamp(historyListLatestEntry)}`;
                        } else if (totalRows) {
                            historySummaryPeek.textContent = 'Displaying iteration history';
                        } else {
                            historySummaryPeek.textContent = 'History loads on demand';
                        }
                    }
                }

                function loadHistoryList(options = {}) {
                    const params = new URLSearchParams();
                    params.set('limit', historyPageSize);
                    if (options.before) {
                        params.set('before', options.before);
                    }
                    fetch(`${historyEndpoint}?${params.toString()}`)
                        .then(response => (response.ok ? response.json() : null))
                        .then(payload => {
                            if (!payload) {
                                throw new Error('bad response');
                            }
                            const items = Array.isArray(payload.items) ? payload.items : [];
                            historyListLoaded = true;

                            if (!items.length && !options.append && (!historyTableBody || !historyTableBody.children.length)) {
                                if (historyListStatus) {
                                    historyListStatus.style.display = '';
                                    historyListStatus.textContent = 'No iteration history is available yet.';
                                }
                                if (historyTableContainer) {
                                    historyTableContainer.style.display = 'none';
                                }
                                if (historyLoadMore) {
                                    historyLoadMore.disabled = true;
                                }
                                if (historyLoadHint) {
                                    historyLoadHint.textContent = '';
                                }
                                historyListCursor = null;
                                historyHasMore = false;
                                historyListLatestEntry = null;
                                updateHistorySummary(payload);
                                return;
                            }

                            if (historyTableContainer) {
                                historyTableContainer.style.display = '';
                            }
                            if (historyListStatus) {
                                historyListStatus.style.display = 'none';
                            }

                            appendHistoryRows(items, Boolean(options.append));
                            if (!options.append && items.length) {
                                historyListLatestEntry = items[items.length - 1];
                            }

                            if (Array.isArray(items) && items.length) {
                                const earliest = items[0];
                                const earliestTs = earliest ? earliest.timestamp_epoch : null;
                                if (typeof earliestTs === 'number' && !Number.isNaN(earliestTs)) {
                                    historyListCursor = earliestTs;
                                }
                            }
                            if (payload.next_before !== undefined && payload.next_before !== null) {
                                historyListCursor = payload.next_before;
                            }
                            historyHasMore = Boolean(payload.has_more);
                            if (historyLoadMore) {
                                historyLoadMore.disabled = !historyHasMore;
                            }
                            if (historyLoadHint) {
                                if (historyHasMore) {
                                    historyLoadHint.textContent = '';
                                } else if ((historyTableBody && historyTableBody.children.length) || items.length) {
                                    historyLoadHint.textContent = 'Reached beginning of retained history.';
                                } else {
                                    historyLoadHint.textContent = '';
                                }
                            }
                            updateHistorySummary(payload);
                        })
                        .catch(() => {
                            if (historyListStatus) {
                                historyListStatus.style.display = '';
                                historyListStatus.textContent = 'Failed to load history data.';
                            }
                            if (historyLoadMore) {
                                historyLoadMore.disabled = true;
                            }
                        });
                }

                if (historyDetails) {
                    historyDetails.addEventListener('toggle', () => {
                        if (historyDetails.open && !historyListLoaded) {
                            loadHistoryList();
                        }
                    });
                }
                if (historyLoadMore) {
                    historyLoadMore.addEventListener('click', () => {
                        if (!historyHasMore || historyListCursor === null) {
                            return;
                        }
                        loadHistoryList({ before: historyListCursor, append: true });
                    });
                }

                const weightsCanvas = document.getElementById('weightsChart');
                if (weightsCanvas && window.Chart && weightLabels.length) {
                    const palette = ['#7afcff', '#f6a6ff', '#9effa9', '#ffe29a', '#b5b0ff', '#ffb8a5', '#aff8db', '#f3c4fb'];
                    const backgroundColors = weightLabels.map((_, index) => palette[index % palette.length]);
                    const totalWeight = weightValues.reduce((acc, value) => acc + value, 0);
                    new Chart(weightsCanvas.getContext('2d'), {
                        type: 'doughnut',
                        data: {
                            labels: weightLabels,
                            datasets: [
                                {
                                    data: weightValues,
                                    backgroundColor: backgroundColors,
                                    borderColor: '#0d1117',
                                    borderWidth: 2,
                                },
                            ],
                        },
                        options: {
                            cutout: '55%',
                            plugins: {
                                legend: { display: false },
                                tooltip: {
                                    callbacks: {
                                        label(context) {
                                            const value = context.parsed || 0;
                                            const percent = totalWeight ? ((value / totalWeight) * 100).toFixed(1) : '0.0';
                                            return `${context.label}: ${value} (${percent}%)`;
                                        },
                                    },
                                },
                            },
                        },
                    });
                    const legendEl = document.getElementById('weightsLegend');
                    if (legendEl) {
                        legendEl.querySelectorAll('[data-index]').forEach((pill, index) => {
                            const color = backgroundColors[index % backgroundColors.length];
                            pill.style.borderColor = `${color}55`;
                            const dot = pill.querySelector('.legend-dot');
                            if (dot) {
                                dot.style.background = color;
                            }
                        });
                    }
                    const summary = weightsCanvas.getAttribute('data-summary');
                    if (summary) {
                        weightsCanvas.setAttribute('aria-label', summary);
                    }
                }
            })();
            </script>
        </body>
        </html>
        """

        return render_template_string(
            template,
            status=status,
            summary=summary,
            discovery=discovery,
            discovery_recent_display=discovery_recent_display,
            discovery_recent_summary=discovery_recent_summary,
            discovery_recent_total=discovery_recent_total,
            counts=counts,
            samples=samples,
            config_overview=config_overview,
            actions=actions,
            logs_display=logs_display,
            logs_summary=logs_summary,
            logs_total=logs_total,
            history=history,
            weights=weights,
            weights_sorted=weights_sorted,
            weights_labels=weights_labels,
            weights_values=weights_values,
            weights_aria_label=weights_aria_label,
            stat_tiles=stat_tiles,
            history_window=history_window,
            history_page_size=history_page_size,
        )

    @app.get("/history")
    def history_json() -> Any:
        def parse_float_arg(name: str) -> Optional[float]:
            raw = request.args.get(name)
            if raw in (None, "", "null"):
                return None
            try:
                return float(raw)
            except (TypeError, ValueError):
                return None

        try:
            limit_val = int(
                request.args.get("limit", str(DEFAULT_HISTORY_PAGE_SIZE))
                or DEFAULT_HISTORY_PAGE_SIZE
            )
        except (TypeError, ValueError):
            limit_val = DEFAULT_HISTORY_PAGE_SIZE
        limit_val = max(1, min(limit_val, MAX_HISTORY_PAGE_SIZE))
        since_val = parse_float_arg("since")
        before_val = parse_float_arg("before")

        items = state.snapshot_history(limit=limit_val, since=since_val, before=before_val)
        latest_ts = items[-1].get("timestamp_epoch") if items else None
        next_before = items[0].get("timestamp_epoch") if items else None
        has_more = False
        if next_before is not None:
            probe = state.snapshot_history(limit=1, before=next_before)
            has_more = bool(probe)
        total_entries = len(state.snapshot_history())

        return jsonify(
            {
                "items": items,
                "count": len(items),
                "limit": limit_val,
                "since": since_val,
                "before": before_val,
                "latest": latest_ts,
                "next_before": next_before,
                "has_more": has_more,
                "total": total_entries,
            }
        )

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
