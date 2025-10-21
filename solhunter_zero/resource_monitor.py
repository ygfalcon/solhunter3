from __future__ import annotations

import asyncio
import copy
import dataclasses
import asyncio
import copy
import dataclasses
import logging
import os
import time
from typing import Dict, Optional

try:  # pragma: no cover - optional dependency
    import psutil
except Exception:  # pragma: no cover - psutil optional
    psutil = None  # type: ignore
    logging.getLogger(__name__).warning(
        "psutil not installed; resource monitoring disabled"
    )

from .event_bus import publish

_CPU_PERCENT: float = 0.0
_PROC_CPU_PERCENT: float = 0.0
_CPU_LAST: float = 0.0

_TASK: Optional[asyncio.Task] = None


@dataclasses.dataclass(slots=True)
class _Budget:
    """Runtime configuration for a single resource budget."""

    name: str
    ceiling: float | None = None
    action: str = "warn"
    grace_samples: int = 3
    active: bool = False
    over_count: int = 0
    last_violation: float = 0.0
    last_event: float = 0.0
    last_value: float = 0.0

    def as_dict(self) -> Dict[str, float | int | str | bool]:
        return {
            "resource": self.name,
            "ceiling": self.ceiling,
            "action": self.action,
            "active": self.active,
            "breach_samples": self.over_count,
            "grace_samples": self.grace_samples,
            "last_violation": self.last_violation or None,
            "last_event": self.last_event or None,
            "value": self.last_value,
        }


_BUDGETS: dict[str, _Budget] = {}
_BUDGET_STATUS: dict[str, Dict[str, object]] = {}


def _coerce_float(value: object, default: Optional[float] = None) -> Optional[float]:
    try:
        if value is None:
            return default
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _coerce_int(value: object, default: int) -> int:
    try:
        if value is None:
            return default
        return int(float(value))
    except Exception:
        return default


def _load_budget_settings(interval: float) -> None:
    """Initialise resource budgets from configuration and environment."""

    global _BUDGETS, _BUDGET_STATUS

    cfg: dict[str, object] = {}
    try:  # pragma: no cover - optional during bootstrap
        from .config import load_selected_config

        cfg = load_selected_config() or {}
    except Exception:
        cfg = {}

    cpu_cfg: Dict[str, object] = {}
    mem_cfg: Dict[str, object] = {}

    raw_limits = cfg.get("resource_limits")
    if isinstance(raw_limits, dict):
        for key, value in raw_limits.items():
            key_str = str(key).lower()
            if isinstance(value, dict):
                if key_str.startswith("cpu"):
                    cpu_cfg.update(value)
                elif key_str.startswith("mem") or key_str.startswith("memory"):
                    mem_cfg.update(value)
                else:
                    continue
            else:
                if key_str.startswith("cpu_"):
                    suffix = key_str.split("_", 1)[1]
                    cpu_cfg.setdefault(suffix, value)
                elif key_str.startswith("mem_") or key_str.startswith("memory_"):
                    suffix = key_str.split("_", 1)[1]
                    mem_cfg.setdefault(suffix, value)
                elif key_str in {"percent", "ceiling"}:
                    cpu_cfg.setdefault(key_str, value)
                    mem_cfg.setdefault(key_str, value)
                elif key_str in {"grace", "grace_samples", "grace_seconds"}:
                    cpu_cfg.setdefault(key_str, value)
                    mem_cfg.setdefault(key_str, value)
    elif isinstance(raw_limits, list):  # pragma: no cover - defensive
        for entry in raw_limits:
            if not isinstance(entry, dict):
                continue
            resource_name = str(entry.get("resource") or entry.get("name") or "").lower()
            if resource_name.startswith("cpu"):
                cpu_cfg.update(entry)
            elif resource_name.startswith("mem") or resource_name.startswith("memory"):
                mem_cfg.update(entry)

    # Fallbacks for flat configuration keys
    for key, dest in (
        ("resource_cpu_percent", cpu_cfg),
        ("resource_cpu_ceiling", cpu_cfg),
        ("cpu_budget_percent", cpu_cfg),
    ):
        if key in cfg and "percent" not in dest:
            dest["percent"] = cfg[key]
    for key, dest in (
        ("resource_memory_percent", mem_cfg),
        ("resource_memory_ceiling", mem_cfg),
        ("memory_budget_percent", mem_cfg),
    ):
        if key in cfg and "percent" not in dest:
            dest["percent"] = cfg[key]
    for key, dest in (
        ("resource_cpu_action", cpu_cfg),
        ("resource_memory_action", mem_cfg),
    ):
        if key in cfg and "action" not in dest:
            dest["action"] = cfg[key]
    if "resource_grace_samples" in cfg and "grace" not in cpu_cfg:
        cpu_cfg.setdefault("grace", cfg["resource_grace_samples"])
        mem_cfg.setdefault("grace", cfg["resource_grace_samples"])
    if "resource_grace_seconds" in cfg and "grace_seconds" not in cpu_cfg:
        cpu_cfg.setdefault("grace_seconds", cfg["resource_grace_seconds"])
        mem_cfg.setdefault("grace_seconds", cfg["resource_grace_seconds"])

    def _env_override(name: str) -> Optional[str]:
        value = os.getenv(name)
        if value is None:
            return None
        stripped = value.strip()
        return stripped if stripped else None

    env_cpu_percent = _env_override("RESOURCE_CPU_CEILING")
    env_mem_percent = _env_override("RESOURCE_MEMORY_CEILING")
    env_cpu_action = _env_override("RESOURCE_CPU_ACTION")
    env_mem_action = _env_override("RESOURCE_MEMORY_ACTION")
    env_grace_samples = _env_override("RESOURCE_BUDGET_GRACE")
    env_grace_seconds = _env_override("RESOURCE_BUDGET_WINDOW")

    if env_cpu_percent is not None:
        cpu_cfg["percent"] = env_cpu_percent
    if env_mem_percent is not None:
        mem_cfg["percent"] = env_mem_percent
    if env_cpu_action is not None:
        cpu_cfg["action"] = env_cpu_action
    if env_mem_action is not None:
        mem_cfg["action"] = env_mem_action
    if env_grace_samples is not None:
        cpu_cfg["grace"] = env_grace_samples
        mem_cfg["grace"] = env_grace_samples
    if env_grace_seconds is not None:
        cpu_cfg["grace_seconds"] = env_grace_seconds
        mem_cfg["grace_seconds"] = env_grace_seconds

    def _create_budget(name: str, cfg_entry: Dict[str, object]) -> _Budget:
        percent = _coerce_float(cfg_entry.get("percent"))
        if percent is None:
            percent = _coerce_float(cfg_entry.get("ceiling"))
        action = str(cfg_entry.get("action") or cfg_entry.get("mode") or "warn")
        grace_samples = _coerce_int(cfg_entry.get("grace"), 3)
        grace_seconds = _coerce_float(cfg_entry.get("grace_seconds"))
        if grace_seconds is not None and grace_seconds > 0:
            grace_samples = max(1, int(round(grace_seconds / interval)))
        if grace_samples <= 0:
            grace_samples = 1
        budget = _Budget(
            name=name,
            ceiling=float(percent) if percent is not None else None,
            action=action.lower(),
            grace_samples=grace_samples,
        )
        return budget

    budgets: dict[str, _Budget] = {}
    for name, cfg_entry in (("cpu", cpu_cfg), ("memory", mem_cfg)):
        if not cfg_entry:
            continue
        budget = _create_budget(name, cfg_entry)
        if budget.ceiling is None or budget.ceiling <= 0:
            continue
        budgets[name] = budget

    _BUDGETS = budgets
    _BUDGET_STATUS = {name: budget.as_dict() for name, budget in budgets.items()}


def _update_budget(name: str, value: float, now: float) -> None:
    budget = _BUDGETS.get(name)
    if budget is None:
        return
    budget.last_value = float(value)
    if budget.ceiling is None or budget.ceiling <= 0:
        budget.active = False
        budget.over_count = 0
        _BUDGET_STATUS[name] = budget.as_dict()
        return

    if value > budget.ceiling:
        budget.over_count += 1
        budget.last_violation = now
        if not budget.active and budget.over_count >= budget.grace_samples:
            budget.active = True
            budget.last_event = now
            alert = budget.as_dict()
            alert["timestamp"] = now
            publish("resource_alert", alert)
    else:
        if budget.active:
            budget.active = False
            budget.over_count = 0
            budget.last_event = now
            alert = budget.as_dict()
            alert["timestamp"] = now
            alert["active"] = False
            publish("resource_alert", alert)
        else:
            budget.over_count = 0

    _BUDGET_STATUS[name] = budget.as_dict()


async def _monitor(interval: float) -> None:
    """Publish system metrics every ``interval`` seconds."""
    if psutil is None:
        return
    try:
        proc = psutil.Process()
        _load_budget_settings(interval)
        while True:
            cpu = psutil.cpu_percent(interval=None)
            proc_cpu = proc.cpu_percent(interval=None)
            mem = psutil.virtual_memory().percent
            payload = {
                "cpu": float(cpu),
                "proc_cpu": float(proc_cpu),
                "memory": float(mem),
            }
            global _CPU_PERCENT, _PROC_CPU_PERCENT, _CPU_LAST
            _CPU_PERCENT = float(cpu)
            _PROC_CPU_PERCENT = float(proc_cpu)
            _CPU_LAST = time.monotonic()
            now = time.time()
            for name, value in (("cpu", cpu), ("memory", mem)):
                _update_budget(name, float(value), now)
            if _BUDGET_STATUS:
                payload["budgets"] = copy.deepcopy(_BUDGET_STATUS)
            publish("system_metrics", payload)
            publish("remote_system_metrics", payload)
            if _BUDGET_STATUS:
                publish(
                    "resource_update",
                    {
                        "cpu": float(cpu),
                        "proc_cpu": float(proc_cpu),
                        "memory": float(mem),
                        "budgets": copy.deepcopy(_BUDGET_STATUS),
                        "timestamp": now,
                    },
                )
            await asyncio.sleep(interval)
    except asyncio.CancelledError:  # pragma: no cover - cancellation
        pass


def start_monitor(interval: float = 1.0) -> asyncio.Task | None:
    """Start background resource monitoring task."""
    global _TASK
    if psutil is None:
        return None
    if interval <= 0:
        interval = 1.0
    if _TASK is None or _TASK.done():
        loop = asyncio.get_running_loop()
        _TASK = loop.create_task(_monitor(interval))
    return _TASK


def stop_monitor() -> None:
    """Stop the running resource monitor, if any."""
    global _TASK
    if _TASK is not None:
        _TASK.cancel()
        _TASK = None


def get_cpu_usage() -> float:
    """Return the most recent CPU usage percentage."""
    global _CPU_PERCENT, _PROC_CPU_PERCENT, _CPU_LAST
    if psutil is None:
        return 0.0
    if time.monotonic() - _CPU_LAST > 2.0:
        try:
            _CPU_PERCENT = float(psutil.cpu_percent(interval=None))
            _PROC_CPU_PERCENT = float(psutil.Process().cpu_percent(interval=None))
            _CPU_LAST = time.monotonic()
        except Exception:
            pass
    return _CPU_PERCENT


def get_budget_status() -> Dict[str, Dict[str, object]]:
    """Return a snapshot of the current resource budget state."""

    if not _BUDGET_STATUS:
        return {}
    return copy.deepcopy(_BUDGET_STATUS)


def active_budget(action: str | None = None) -> list[Dict[str, object]]:
    """Return a list of active budgets filtered by ``action``."""

    status = get_budget_status()
    result: list[Dict[str, object]] = []
    for name, info in status.items():
        if not info.get("active"):
            continue
        payload = dict(info)
        payload.setdefault("resource", name)
        if action is None or str(info.get("action")).lower() == action.lower():
            result.append(payload)
    return result


try:  # pragma: no cover - initialization best effort
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = None
if loop and psutil is not None:
    loop.call_soon(start_monitor)
