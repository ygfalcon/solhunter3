"""Compatibility shim for legacy depth client imports."""
from __future__ import annotations

from solhunter_zero.depth_client import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
