"""Utilities for loading lightweight datasets bundled with Solhunter."""

from __future__ import annotations

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

__all__ = ["DATA_DIR"]
