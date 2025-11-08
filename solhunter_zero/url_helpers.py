"""URL normalization helpers for Solana websocket endpoints."""
from __future__ import annotations

import ipaddress
from typing import Optional


_LOCAL_HOSTNAMES = {
    "localhost",
    "ip6-localhost",
    "ip6-loopback",
    "host.docker.internal",
}


def _is_local_address(authority: str) -> bool:
    """Return ``True`` if *authority* refers to a local or private host."""

    if not authority:
        return False

    host_port = authority.split("/", 1)[0]
    host_port = host_port.split("?", 1)[0]
    host, _, _ = host_port.partition(":")
    host = host.strip("[]").strip()
    if not host:
        return False

    lowered = host.lower()
    if lowered in _LOCAL_HOSTNAMES:
        return True
    if lowered.endswith(".local") or lowered.endswith(".lan"):
        return True

    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return False

    return addr.is_private or addr.is_loopback or addr.is_link_local


def as_websocket_url(url: Optional[str]) -> Optional[str]:
    """Coerce *url* into a websocket URL when possible.

    ``ws://`` is preferred for loopback/private hosts where TLS is unlikely to
    be configured, while ``wss://`` is used for public hosts. When *url* already
    specifies a websocket or HTTP(S) scheme it is normalised accordingly.
    """

    if not url:
        return None

    trimmed = url.strip()
    if not trimmed:
        return None

    lowered = trimmed.lower()
    if lowered.startswith("ws://") or lowered.startswith("wss://"):
        return trimmed
    if lowered.startswith("http://"):
        return "ws://" + trimmed[7:]
    if lowered.startswith("https://"):
        return "wss://" + trimmed[8:]

    if "://" not in trimmed:
        candidate = trimmed.lstrip("/")
        if not candidate:
            return None
        authority = candidate.split("/", 1)[0]
        scheme = "ws" if _is_local_address(authority) else "wss"
        return f"{scheme}://{candidate}"

    return trimmed


__all__ = ["as_websocket_url"]
