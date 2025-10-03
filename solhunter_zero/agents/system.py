"""Compatibility shims for ``solhunter_zero.system`` helpers."""
from __future__ import annotations

from .. import system as _system

__all__ = ["detect_cpu_count", "set_rayon_threads"]

# Re-export the public helper functions so legacy imports continue to work.
detect_cpu_count = _system.detect_cpu_count
set_rayon_threads = _system.set_rayon_threads

# ``main`` is technically part of the module's public surface, but it is
# primarily used for ``python -m solhunter_zero.system``.  Re-export it as well
# in case any downstream tooling relied on the old location.
if hasattr(_system, "main"):
    main = _system.main  # type: ignore[attr-defined]
    __all__.append("main")
