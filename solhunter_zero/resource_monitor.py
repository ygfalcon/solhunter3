from __future__ import annotations

import asyncio
import time
from typing import Optional
import logging

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


async def _monitor(interval: float) -> None:
    """Publish system metrics every ``interval`` seconds."""
    if psutil is None:
        return
    try:
        proc = psutil.Process()
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
            publish("system_metrics", payload)
            publish("remote_system_metrics", payload)
            await asyncio.sleep(interval)
    except asyncio.CancelledError:  # pragma: no cover - cancellation
        pass


def start_monitor(interval: float = 1.0) -> asyncio.Task | None:
    """Start background resource monitoring task."""
    global _TASK
    if psutil is None:
        return None
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


try:  # pragma: no cover - initialization best effort
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = None
if loop and psutil is not None:
    loop.call_soon(start_monitor)
