from __future__ import annotations

"""Common cache-related paths used across the package."""

from pathlib import Path

from .paths import ROOT

CACHE_DIR = ROOT / ".cache"
MPS_SENTINEL = CACHE_DIR / "torch_mps_ready"
MAC_SETUP_MARKER = CACHE_DIR / "mac_setup_complete"
TOOLS_OK_MARKER = CACHE_DIR / "macos_tools_ok"
CARGO_MARKER = CACHE_DIR / "cargo-installed"
VENV_OK_MARKER = CACHE_DIR / "venv_ok"
RUNTIME_LOCK_PATH = CACHE_DIR / "runtime.lock"

__all__ = [
    "CACHE_DIR",
    "MPS_SENTINEL",
    "MAC_SETUP_MARKER",
    "TOOLS_OK_MARKER",
    "CARGO_MARKER",
    "VENV_OK_MARKER",
    "RUNTIME_LOCK_PATH",
]
