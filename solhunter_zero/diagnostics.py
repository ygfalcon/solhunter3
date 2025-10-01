from __future__ import annotations

"""Runtime diagnostics utilities for SolHunter Zero.

This module provides a single helper :func:`write_diagnostics` which writes a
small JSON file summarising key runtime information for accessibility tools.
"""

from pathlib import Path
import json
import os
from typing import Any

from .paths import ROOT
from .logging_utils import log_startup


def _coerce_path(value: Any) -> str | None:
    """Return *value* as a string path if possible."""

    if isinstance(value, Path):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def write_diagnostics(status: dict) -> None:
    """Write a ``diagnostics.json`` summary for accessibility tools.

    Parameters
    ----------
    status:
        Dictionary containing runtime information.  Expected keys are
        ``gpu_backend``, ``config_path``, ``keypair_path`` and optionally
        ``preflight`` or ``preflight_warnings`` describing any preflight
        failures.

    The function attempts to write a compact summary to ``diagnostics.json``
    in the project root.  Failures are ignored.  When the file is written
    successfully its path is appended to ``startup.log`` for visibility.
    """

    summary: dict[str, Any] = {
        "gpu_backend": status.get("gpu_backend"),
        "config_path": _coerce_path(status.get("config_path")),
        "keypair_path": _coerce_path(status.get("keypair_path")),
        "preflight_warnings": [],
    }

    # Extract preflight warnings from either a list of failures or from
    # a preflight result structure.  If none are supplied attempt to run the
    # preflight checks directly unless explicitly disabled via the
    # ``SOLHUNTER_SKIP_PREFLIGHT`` environment variable.
    if "preflight_warnings" in status:
        warnings = status.get("preflight_warnings", [])
    else:
        preflight = status.get("preflight") or {}
        warnings = []
        if isinstance(preflight, dict):
            warnings = preflight.get("failures", [])
        elif isinstance(preflight, list):
            # assume list of (name, ok, msg)
            for item in preflight:
                try:
                    name, ok, msg = item
                except Exception:  # pragma: no cover - defensive
                    continue
                if not ok:
                    warnings.append({"name": name, "message": msg})
        elif os.getenv("SOLHUNTER_SKIP_PREFLIGHT") != "1":
            try:  # pragma: no cover - best effort only
                from scripts import preflight as _preflight

                results = _preflight.run_preflight()
            except Exception:
                warnings = []
            else:
                warnings = [
                    {"name": name, "message": msg}
                    for name, ok, msg in results
                    if not ok
                ]
    # Normalise warnings into list of dicts with name/message.
    normalised: list[dict[str, str]] = []
    for item in warnings:
        if isinstance(item, dict):
            name = str(item.get("name", ""))
            msg = str(item.get("message", ""))
        elif isinstance(item, (list, tuple)) and len(item) >= 2:
            name = str(item[0])
            msg = str(item[2] if len(item) > 2 else item[1])
        else:
            name = ""
            msg = str(item)
        normalised.append({"name": name, "message": msg})
    summary["preflight_warnings"] = normalised

    path = ROOT / "diagnostics.json"
    try:
        path.write_text(json.dumps(summary, indent=2))
    except OSError:
        return
    log_startup(f"Diagnostics written to {path}")
