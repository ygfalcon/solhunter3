"""Runtime health check utilities.

This module provides lightweight, dependency-free helpers used by runtime
startup scripts to verify that required services are available.  The checks are
designed to be simple and fast so they can execute in constrained CI
environments without external services.
"""

from __future__ import annotations

import asyncio
import json
import socket
import time
import urllib.parse
import urllib.request
from typing import Callable, Tuple

CheckResult = Tuple[bool, str]


def check_redis(url: str) -> CheckResult:
    """Verify that a Redis server is reachable at ``url``.

    The function performs a plain TCP connection check so it does not require
    the ``redis`` package to be installed.
    """

    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 6379
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True, "ok"
    except OSError as exc:  # pragma: no cover - network failure paths
        return False, str(exc)


def check_event_bus(url: str | None = None, timeout: float = 3.0) -> CheckResult:
    """Verify that the runtime event bus broker is reachable."""

    try:
        from .event_bus import verify_broker_connection

        coroutine = (
            verify_broker_connection(url, timeout=timeout)
            if url is not None
            else verify_broker_connection(timeout=timeout)
        )
        try:
            ok = asyncio.run(coroutine)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                ok = loop.run_until_complete(coroutine)
            finally:
                loop.close()
        if ok:
            return True, "ok"
        return False, "broker unreachable"
    except Exception as exc:  # pragma: no cover - defensive
        return False, str(exc)


def _runtime_health_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if not base:
        base = "http://127.0.0.1:5000"
    return urllib.parse.urljoin(base + "/", "health/runtime")


def _fetch_runtime_health(base_url: str, timeout: float = 1.0) -> dict:
    url = _runtime_health_url(base_url)
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
        data = resp.read()
    payload = json.loads(data.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid runtime health payload")
    return payload


def check_ui_websockets(
    base_url: str,
    *,
    min_clients: int = 0,
    timeout: float = 1.0,
) -> CheckResult:
    """Ensure UI websocket telemetry is accessible and valid."""

    try:
        payload = _fetch_runtime_health(base_url, timeout=timeout)
    except Exception as exc:  # pragma: no cover - network failure paths
        return False, str(exc)
    ui_info = payload.get("ui") or {}
    clients = ui_info.get("ws_clients") or {}
    if not isinstance(clients, dict):
        return False, "missing websocket telemetry"
    normalized: dict[str, int] = {}
    for name, value in clients.items():
        try:
            normalized[name] = int(value)
        except Exception:
            return False, f"invalid websocket count for {name}"
    required = {"events", "logs", "rl"}
    missing = [name for name in required if name not in normalized]
    if missing:
        return False, f"missing channels: {', '.join(sorted(missing))}"
    total = sum(normalized.values())
    if min_clients and total < min_clients:
        return False, f"clients={total}<min={min_clients}"
    summary = ", ".join(f"{name}={count}" for name, count in sorted(normalized.items()))
    return True, summary


def check_agent_loop(
    base_url: str,
    *,
    max_age: float = 60.0,
    timeout: float = 1.0,
) -> CheckResult:
    """Verify that the agent manager loop heartbeat is fresh."""

    try:
        payload = _fetch_runtime_health(base_url, timeout=timeout)
    except Exception as exc:  # pragma: no cover - network failure paths
        return False, str(exc)
    heartbeat = payload.get("heartbeat") or {}
    age = heartbeat.get("age")
    if age is None:
        return False, "no heartbeat"
    try:
        age_val = float(age)
    except Exception:
        return False, "invalid heartbeat age"
    limit = heartbeat.get("threshold")
    try:
        limit_val = float(limit) if limit is not None else max_age
    except Exception:
        limit_val = max_age
    limit_val = max(limit_val, max_age)
    if age_val <= limit_val:
        return True, f"age={age_val:.2f}s"
    return False, f"age={age_val:.2f}s>max={limit_val:.2f}s"


def check_execution_queue(
    base_url: str,
    *,
    max_depth: int = 200,
    timeout: float = 1.0,
) -> CheckResult:
    """Ensure execution queue depth stays within the configured budget."""

    try:
        payload = _fetch_runtime_health(base_url, timeout=timeout)
    except Exception as exc:  # pragma: no cover - network failure paths
        return False, str(exc)
    queues = payload.get("queues")
    if not isinstance(queues, dict):
        return False, "missing queue telemetry"
    depth = queues.get("execution_queue")
    if depth is None:
        return False, "missing execution_queue"
    try:
        depth_val = int(depth)
    except Exception:
        return False, "invalid execution_queue"
    if depth_val <= max_depth:
        return True, f"depth={depth_val}"
    return False, f"depth={depth_val}>max={max_depth}"


def http_ok(url: str) -> CheckResult:
    """Return ``(True, msg)`` if an HTTP GET request succeeds.

    The ``msg`` contains the HTTP status code on success or the exception text
    on failure.
    """

    try:
        with urllib.request.urlopen(url, timeout=1.0) as resp:  # noqa: S310
            return 200 <= resp.status < 400, f"http {resp.status}"
    except Exception as exc:  # pragma: no cover - network failure paths
        return False, str(exc)


def wait_for(
    func: Callable[[], CheckResult],
    *,
    retries: int = 30,
    sleep: float = 0.5,
) -> CheckResult:
    """Poll ``func`` until it reports success or ``retries`` is exhausted."""

    last: CheckResult = (False, "no result")
    for _ in range(retries):
        ok, msg = func()
        last = (ok, msg)
        if ok:
            return last
        time.sleep(sleep)
    return last


__all__ = [
    "check_redis",
    "check_event_bus",
    "check_ui_websockets",
    "check_agent_loop",
    "check_execution_queue",
    "http_ok",
    "wait_for",
]

