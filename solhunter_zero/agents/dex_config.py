"""Compatibility shim for legacy DEX configuration imports."""
from __future__ import annotations

from solhunter_zero.dex_config import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
