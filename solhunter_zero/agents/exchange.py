"""Compatibility shim for :mod:`solhunter_zero.exchange` helpers.

This module preserves legacy import paths that historically exposed the
exchange utilities via ``solhunter_zero.agents.exchange``.  Downstream
extensions and notebooks may still rely on those imports, so we re-export the
live implementations from the consolidated module to avoid duplication.
"""

from __future__ import annotations

from types import ModuleType
from typing import Iterable

from .. import exchange as _exchange


def _collect_public_names(module: ModuleType) -> list[str]:
    """Return the public attribute names for ``module``.

    When the source module defines an explicit ``__all__`` we respect it;
    otherwise every attribute that is not private (``_``-prefixed) is exported.
    """

    export: Iterable[str] | None = getattr(module, "__all__", None)
    if export is None:
        export = (name for name in dir(module) if not name.startswith("_"))
    return [str(name) for name in export]


# Private helpers that callers historically imported from the legacy path.
_PRIVATE_EXPORTS = ["_sign_transaction"]

__all__ = sorted(set(_collect_public_names(_exchange)) | set(_PRIVATE_EXPORTS))

for _name in __all__:
    globals()[_name] = getattr(_exchange, _name)

# Mirror the source module metadata for nicer introspection.
__doc__ = _exchange.__doc__
