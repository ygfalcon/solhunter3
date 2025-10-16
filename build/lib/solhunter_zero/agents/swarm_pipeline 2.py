"""Compatibility shim for :mod:`solhunter_zero.swarm_pipeline`."""

from __future__ import annotations

from solhunter_zero.swarm_pipeline import *  # noqa: F401,F403

__all__ = [name for name in globals() if not name.startswith("_")]
