"""Provide backwards compatibility for ``solhunter_zero.agents.routeffi``.

Some legacy components still import the Rust routing bindings through the
``solhunter_zero.agents`` namespace.  The real implementation lives alongside
the core modules as :mod:`solhunter_zero.routeffi`.  When that import path was
removed the arbitrage agent emitted noisy warnings about the missing module and
fell back to the pure Python implementation.

Re-exporting the canonical module from here keeps those imports working without
duplicating any logic.
"""

from __future__ import annotations

from .. import routeffi as _routeffi

__all__ = getattr(_routeffi, "__all__", [])


def __getattr__(name: str):
    return getattr(_routeffi, name)


def __dir__() -> list[str]:  # pragma: no cover - trivial helper
    local = set(globals()) - {"_routeffi"}
    return sorted(local | set(dir(_routeffi)))
