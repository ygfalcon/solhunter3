"""Runtime health check utilities.

This module provides lightweight, dependency-free helpers used by runtime
startup scripts to verify that required services are available.  The checks are
designed to be simple and fast so they can execute in constrained CI
environments without external services.
"""

from __future__ import annotations

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


def check_depth_service() -> CheckResult:
    """Placeholder depth service check.

    In a production environment this would verify the presence and health of
    the Rust ``depth_service`` binary.  For tests we simply report success.
    """

    return True, "ok"


def check_ffi() -> CheckResult:
    """Placeholder FFI library check.

    The actual project performs a more elaborate verification of the compiled
    FFI bindings.  Here we merely signal success to keep the check lightweight.
    """

    return True, "ok"


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
    "check_depth_service",
    "check_ffi",
    "http_ok",
    "wait_for",
]

