"""Agent compatibility shim for resource monitoring helpers."""
from __future__ import annotations

from .. import resource_monitor as _resource_monitor

__all__ = ["start_monitor", "stop_monitor", "get_cpu_usage"]

start_monitor = _resource_monitor.start_monitor
stop_monitor = _resource_monitor.stop_monitor
get_cpu_usage = _resource_monitor.get_cpu_usage
