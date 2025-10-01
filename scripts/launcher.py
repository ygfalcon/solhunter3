#!/usr/bin/env python3
"""Deprecated shim for backward compatibility.

This module re-exports :func:`solhunter_zero.launcher.main`.
"""
from __future__ import annotations

import os
import platform
import sys
import warnings

# During regular execution SolHunter Zero requires an arm64 build of Python on
# macOS.  Tests, however, run under x86_64 Linux and need to bypass this check
# to simulate arm64 behaviour.  Allow tests to skip the enforcement either when
# running on non-Darwin platforms or when the SOLHUNTER_TESTING flag is set.
if (
    platform.system() == "Darwin"
    and platform.machine() != "arm64"
    and not os.getenv("SOLHUNTER_TESTING")
):
    print(
        "Non-arm64 Python detected. Please install an arm64 build of Python to run SolHunter Zero.",
    )
    raise SystemExit(1)

from solhunter_zero.launcher import main

warnings.warn(
    "scripts.launcher is deprecated; use solhunter_zero.launcher instead",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["main"]


if __name__ == "__main__":  # pragma: no cover - legacy behaviour
    main()
