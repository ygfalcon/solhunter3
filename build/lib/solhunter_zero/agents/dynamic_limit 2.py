"""Agent compatibility shim for dynamic limit utilities."""
from __future__ import annotations

from .. import dynamic_limit as _dynamic_limit

__all__ = ["refresh_params", "_target_concurrency", "_step_limit"]

refresh_params = _dynamic_limit.refresh_params
_target_concurrency = _dynamic_limit._target_concurrency  # type: ignore[attr-defined]
_step_limit = _dynamic_limit._step_limit  # type: ignore[attr-defined]
