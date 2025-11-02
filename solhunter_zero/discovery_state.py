from __future__ import annotations

import os
import threading
from typing import Any, Mapping, Optional

from .agents.discovery import DEFAULT_DISCOVERY_METHOD, resolve_discovery_method

_lock = threading.Lock()
_override: Optional[str] = None


def set_override(method: str | None) -> tuple[Optional[str], Optional[str]]:
    """Set the active discovery override to *method*.

    Returns a tuple of ``(previous, current)`` values using canonical method
    names.  Passing ``None`` clears any override.
    """

    canonical = resolve_discovery_method(method)
    with _lock:
        global _override
        previous = _override
        _override = canonical
    return previous, canonical


def clear_override() -> None:
    """Clear any discovery method override."""

    set_override(None)


def get_override() -> Optional[str]:
    """Return the currently configured discovery method override if set."""

    with _lock:
        return _override


def current_method(
    *,
    config: Mapping[str, Any] | None = None,
    explicit: str | None = None,
) -> str:
    """Return the discovery method that should be used for trading iterations."""

    override = get_override()
    if override:
        return override

    if explicit:
        explicit_method = resolve_discovery_method(explicit)
        if explicit_method:
            return explicit_method

    method: Optional[str] = None
    if config is not None:
        try:
            value = config.get("discovery_method")
        except Exception:
            value = None
        else:
            method = resolve_discovery_method(value)

    if method is None:
        method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))

    if method is None:
        method = DEFAULT_DISCOVERY_METHOD

    return method
