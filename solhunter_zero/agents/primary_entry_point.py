"""Compatibility shim for :mod:`solhunter_zero.primary_entry_point`."""

from __future__ import annotations

from solhunter_zero.primary_entry_point import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
