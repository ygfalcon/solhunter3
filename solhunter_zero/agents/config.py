"""Compatibility shim for :mod:`solhunter_zero.config` helpers.

This module mirrors ``solhunter_zero.config`` to keep historic import paths
functional for out-of-tree integrations.  The trading agents used to ship
the configuration utilities under their namespace; the functions now live at
package root but we continue to re-export them here to avoid breaking
extensions that still rely on the legacy location.
"""

from __future__ import annotations

from types import ModuleType
from typing import Iterable

from .. import config as _config


def _collect_public_names(module: ModuleType) -> list[str]:
    """Return the public attribute names for ``module``.

    The helper respects an explicit ``__all__`` if present, otherwise it falls
    back to exporting every attribute that does not start with an underscore.
    """

    export: Iterable[str] | None = getattr(module, "__all__", None)
    if export is None:
        export = (name for name in dir(module) if not name.startswith("_"))
    return [str(name) for name in export]


__all__ = _collect_public_names(_config)

for _name in __all__:
    globals()[_name] = getattr(_config, _name)

# Ensure ``__doc__`` and other metadata mirror the source module for
# introspection tools.  ``__all__`` is already defined above.
__doc__ = _config.__doc__
