from __future__ import annotations

"""Utilities for loading environment variables from files."""

from pathlib import Path
import os

from .logging_utils import log_startup
from .paths import ROOT
from .console_utils import console_print, console_warning

TEMPLATE_PATH = ROOT / "solhunter_zero" / "static" / "env.template"

__all__ = ["load_env_file"]


def load_env_file(path: Path) -> None:
    """Load ``KEY=VALUE`` pairs from *path* into ``os.environ``.

    Blank lines and ``#`` comments are ignored. Existing environment variables
    are preserved. If *path* is missing, a template is created from
    ``static/env.template`` and its creation is logged to ``startup.log``.
    """

    if not path.exists():
        try:
            template_text = TEMPLATE_PATH.read_text(encoding="utf-8")
        except OSError:
            template_text = "# Environment variables\n"

        try:
            path.write_text(template_text, encoding="utf-8")
            msg = (
                f"Created default environment file at {path}. "
                "Please update it with your settings."
            )
            console_print(msg)
        except OSError:
            msg = (
                f"Warning: environment file {path} not found and could not be created"
            )
            console_warning(msg)
        log_startup(msg)
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        os.environ.setdefault(key, value)
