"""Compatibility shim for :mod:`solhunter_zero.util` helpers.

The utilities that power the trading agents historically lived under the
``solhunter_zero.agents`` namespace.  They were promoted to the package root
to make them reusable by non-agent components, but a number of extensions and
third-party integrations still import them from their legacy location.  This
module re-exports the live implementations so those imports remain functional
without needing to keep duplicate code in sync.
"""

from __future__ import annotations

from types import ModuleType
from typing import Iterable

from .. import util as _util


def _collect_public_names(module: ModuleType) -> list[str]:
    """Return the public attribute names for ``module``.

    The helper respects an explicit ``__all__`` if present, otherwise it falls
    back to exporting every attribute that does not start with an underscore.
    """

    export: Iterable[str] | None = getattr(module, "__all__", None)
    if export is None:
        export = (name for name in dir(module) if not name.startswith("_"))
    return [str(name) for name in export]


__all__ = _collect_public_names(_util)

for _name in __all__:
    globals()[_name] = getattr(_util, _name)

# Ensure ``__doc__`` and other metadata mirror the source module for
# introspection tools.  ``__all__`` is already defined above.
__doc__ = _util.__doc__
