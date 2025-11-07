from __future__ import annotations

import os
import socket
import subprocess
import time
from typing import Iterable


def _is_local_host(host: str) -> bool:
    return host in {"127.0.0.1", "localhost", "0.0.0.0"}


def _can_connect(host: str, port: int, timeout: float = 0.2) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_local_redis_if_needed(urls: Iterable[str] | None) -> None:
    """Start a local redis-server if any URL targets localhost and isn't reachable.

    Raises ``RuntimeError`` when redis-server cannot be launched or refuses to
    accept connections after a short grace period. Callers should treat such
    failures as fatal because they imply the broker cannot be reached.
    """
    if not urls:
        return
    host, port = None, None
    for u in urls:
        if not (u.startswith("redis://") or u.startswith("rediss://")):
            continue
        try:
            import urllib.parse as _urlparse

            p = _urlparse.urlparse(u)
            h = p.hostname or "127.0.0.1"
            pt = p.port or 6379
        except Exception:
            continue
        if _is_local_host(h):
            host, port = h, pt
            break
    if host is None:
        return
    if _can_connect(host, port):
        return
    try:
        # Silence output; rely on later ping attempts to confirm readiness
        proc = subprocess.Popen(
            ["redis-server"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
    except FileNotFoundError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError("redis-server executable not found on PATH") from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"failed to launch redis-server: {exc}") from exc

    # Wait briefly for startup
    for _ in range(10):
        if _can_connect(host, port):
            return
        time.sleep(0.3)

    exit_code = None
    try:
        exit_code = proc.poll()
    except Exception:  # pragma: no cover - defensive
        exit_code = None
    if exit_code is not None:
        raise RuntimeError(
            f"redis-server exited with code {exit_code} before accepting connections on {host}:{port}"
        )

    raise RuntimeError(
        f"timed out waiting for redis-server to accept connections on {host}:{port}"
    )

