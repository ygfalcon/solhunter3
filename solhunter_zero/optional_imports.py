from __future__ import annotations

"""Utilities for importing optional dependencies."""

from importlib import import_module
from typing import Any

_SENTINEL: Any = object()


def try_import(name: str, stub: Any = _SENTINEL) -> Any:
    """Try to import *name* returning *stub* if unavailable.

    Parameters
    ----------
    name:
        Module path to import.
    stub:
        Object returned when the import fails.  If omitted an
        :class:`ImportError` is raised instead.  Passing ``None`` is allowed
        and results in ``None`` being returned on failure.
    """

    try:
        return import_module(name)
    except Exception:
        if stub is _SENTINEL:
            raise
        return stub
