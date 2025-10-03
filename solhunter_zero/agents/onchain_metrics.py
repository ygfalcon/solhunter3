"""Compatibility shim for :mod:`solhunter_zero.onchain_metrics` helpers."""
from __future__ import annotations

from types import ModuleType
from typing import Iterable

from .. import onchain_metrics as _onchain_metrics


def _collect_public_names(module: ModuleType) -> list[str]:
    """Return the public attribute names for ``module``.

    Mirrors the export behaviour from :mod:`solhunter_zero.util`'s shim by
    respecting an explicit ``__all__`` and otherwise exposing every attribute
    that is not private.
    """

    export: Iterable[str] | None = getattr(module, "__all__", None)
    if export is None:
        export = (name for name in dir(module) if not name.startswith("_"))
    return [str(name) for name in export]


__all__ = _collect_public_names(_onchain_metrics)

for _name in __all__:
    globals()[_name] = getattr(_onchain_metrics, _name)

# Preserve documentation for introspection tools.
__doc__ = _onchain_metrics.__doc__
