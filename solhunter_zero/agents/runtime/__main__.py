"""Compatibility shim for :mod:`solhunter_zero.runtime.__main__`."""

from __future__ import annotations

from solhunter_zero.runtime.__main__ import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
