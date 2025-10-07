"""Compatibility shim that re-exports flash loan helpers.

The flash-loan utilities were moved to :mod:`solhunter_zero.flash_loans` so
that non-agent components could reuse them.  Older modules – including some of
the agent plug-ins – still import the helpers via
``solhunter_zero.agents.flash_loans``.  Re-exporting the public API keeps those
imports working without needing to modify ``sys.path`` at runtime.
"""

from __future__ import annotations

from ..flash_loans import *  # noqa: F401,F403 - legacy re-export

__all__ = [name for name in globals() if not name.startswith("_")]
