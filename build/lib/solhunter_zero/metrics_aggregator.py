from __future__ import annotations

"""Aggregate system metrics from multiple sources."""

import asyncio
from collections import deque
from typing import Any, Deque, Tuple

from .event_bus import publish, subscription

# Keep the last few readings
_HISTORY: Deque[Tuple[float, float]] = deque(maxlen=4)


def _on_metrics(msg: Any) -> None:
    """Handle incoming ``system_metrics`` events and publish an average."""
    cpu = getattr(msg, "cpu", None)
    mem = getattr(msg, "memory", None)
    if cpu is None and isinstance(msg, dict):
        cpu = msg.get("cpu")
        mem = msg.get("memory")
    if cpu is None or mem is None:
        return
    try:
        cpu = float(cpu)
        mem = float(mem)
    except Exception:
        return
    _HISTORY.append((cpu, mem))
    avg_cpu = sum(c for c, _ in _HISTORY) / len(_HISTORY)
    avg_mem = sum(m for _, m in _HISTORY) / len(_HISTORY)
    publish("system_metrics_combined", {"cpu": avg_cpu, "memory": avg_mem})


_def_subs = []


def start() -> None:
    """Begin aggregating ``system_metrics`` events from local and remote peers."""
    global _def_subs
    if not _def_subs:
        _def_subs = [
            subscription("system_metrics", _on_metrics),
            subscription("remote_system_metrics", _on_metrics),
        ]
        for sub in _def_subs:
            sub.__enter__()
        if _HISTORY:
            avg_cpu = sum(c for c, _ in _HISTORY) / len(_HISTORY)
            avg_mem = sum(m for _, m in _HISTORY) / len(_HISTORY)
            publish(
                "system_metrics_combined",
                {"cpu": avg_cpu, "memory": avg_mem},
            )


def emit_startup_complete(startup_duration_ms: float) -> None:
    """Publish metric indicating startup finished with given duration."""
    publish("startup_complete", {"startup_duration_ms": float(startup_duration_ms)})


async def _run_forever() -> None:
    start()
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":  # pragma: no cover - manual execution
    asyncio.run(_run_forever())
