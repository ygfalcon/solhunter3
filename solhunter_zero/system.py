from __future__ import annotations

import os
import platform
import subprocess
import sys
from typing import Iterable


def _try_commands(cmds: Iterable[list[str]]) -> int | None:
    """Try each command in order and return the first successful integer output."""
    for cmd in cmds:
        try:
            out = subprocess.check_output(cmd, text=True).strip()
            return int(out)
        except Exception:
            continue
    return None


def detect_cpu_count() -> int:
    """Detect the number of CPUs across platforms."""
    system = platform.system()
    if system == "Linux":
        count = _try_commands([["nproc"], ["getconf", "_NPROCESSORS_ONLN"]])
    elif system == "Darwin":
        count = _try_commands([["sysctl", "-n", "hw.ncpu"]])
    else:
        count = None
    return count or os.cpu_count() or 1


def set_rayon_threads() -> None:
    """Set ``RAYON_NUM_THREADS`` based on the detected CPU count.

    The environment variable is only set if it is not already defined so that
    callers may override the value manually.  This central helper allows other
    modules to configure the Rayon thread pool without duplicating environment
    logic across the codebase.
    """

    if "RAYON_NUM_THREADS" not in os.environ:
        os.environ["RAYON_NUM_THREADS"] = str(detect_cpu_count())


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] == "cpu-count":
        print(detect_cpu_count())
        return 0
    print("usage: python -m solhunter_zero.system cpu-count", file=sys.stderr)
    return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
