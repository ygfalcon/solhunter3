"""Compatibility shim for :mod:`solhunter_zero.scanner_common` helpers."""
from __future__ import annotations

from types import ModuleType
from typing import Iterable

from .. import scanner_common as _scanner_common


def _collect_export_names(module: ModuleType) -> list[str]:
    """Return attribute names that should be exported for ``module``.

    The helper prefers an explicit ``__all__`` definition when present.  Older
    builds of the project exposed a handful of private helpers (prefixed with an
    underscore) that some extensions imported directly; include those as well if
    they exist so the compatibility shim faithfully mirrors the historical
    surface area.
    """

    explicit: Iterable[str] | None = getattr(module, "__all__", None)
    if explicit is None:
        # Expose every non-dunder attribute by default; legacy consumers have
        # relied on the private helpers historically.
        export = [name for name in dir(module) if not name.startswith("__")]
    else:
        export = [str(name) for name in explicit]

    legacy_privates = (
        "refresh_runtime_values",
        "_derive_ws_from_rpc",
        "_ensure_env",
    )
    for name in legacy_privates:
        if hasattr(module, name) and name not in export:
            export.append(name)
    return export


__all__ = _collect_export_names(_scanner_common)

for _name in __all__:
    if hasattr(_scanner_common, _name):
        globals()[_name] = getattr(_scanner_common, _name)

# Mirror docstring/metadata for better introspection parity.
__doc__ = _scanner_common.__doc__
