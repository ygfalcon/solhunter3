from __future__ import annotations

"""Project-wide path helpers."""

from pathlib import Path

# Repository root path
ROOT = Path(__file__).resolve().parent.parent

__all__ = ["ROOT"]
