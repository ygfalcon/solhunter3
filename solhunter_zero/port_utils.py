"""Helpers for probing and selecting TCP ports on localhost.

These utilities centralise "find the next open port" logic so every service
in the runtime can gracefully fall back when the preferred port is already in
use (for example when multiple stacks are running on the same machine).
"""

from __future__ import annotations

import socket
from typing import Iterable

DEFAULT_ATTEMPTS = 32


def is_port_open(host: str, port: int, *, timeout: float = 0.25) -> bool:
    """Return ``True`` if ``host:port`` accepts TCP connections."""

    target_host = host
    if host in {"0.0.0.0", "::"}:
        target_host = "127.0.0.1"

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        try:
            sock.connect((target_host, port))
        except OSError:
            return False
    return True


def find_available_port(
    host: str,
    preferred: int,
    *,
    attempts: int = DEFAULT_ATTEMPTS,
    blacklist: Iterable[int] | None = None,
) -> int:
    """Return an available port on ``host`` starting from ``preferred``.

    If the preferred port is in use we scan upwards until we find a free slot.
    ``blacklist`` provides additional ports to skip (for example when the UI
    and websocket stack must not clash).
    """

    blocked = set(int(p) for p in blacklist or ())
    port = int(preferred)
    for _ in range(max(1, attempts)):
        if port not in blocked and not is_port_open(host, port):
            return port
        port += 1
    return port


__all__ = ["is_port_open", "find_available_port"]
