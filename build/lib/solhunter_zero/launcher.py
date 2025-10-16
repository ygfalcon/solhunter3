#!/usr/bin/env python3
"""Unified launcher for SolHunter Zero.

This script mirrors the behaviour of the previous ``start.command`` shell
script so that all entry points invoke the same Python-based logic.

* Uses ``.venv`` if present.
* Caches the detected Python interpreter for faster startups.
* Sets ``RAYON_NUM_THREADS`` based on the CPU count.
* On macOS re-execs via ``arch -arm64`` to ensure native arm64 binaries.
* Delegates all arguments to ``scripts/startup.py``.
"""
from __future__ import annotations

import argparse
import os
import platform
import shutil
import sys
from pathlib import Path
from typing import Callable, NoReturn

from .paths import ROOT
from .python_env import find_python
# Importing the module instead of the function makes it easier to monkeypatch
# during testing.
from . import system
set_rayon_threads = system.set_rayon_threads


class _LazyDevice:
    """Proxy for :mod:`solhunter_zero.device` imported on demand."""

    _module = None

    def __getattr__(self, name: str):
        if self._module is None:
            from . import device as _device
            self._module = _device
        return getattr(self._module, name)


device = _LazyDevice()


def write_ok_marker(path: Path) -> None:
    """Write an ``ok`` marker file, creating parent directories."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("ok")
    except OSError as exc:
        from .logging_utils import log_startup
        log_startup(f"Failed to write ok marker {path}: {exc}")
        return


def _ensure_arm64_python() -> None:
    """Re-exec via ``arch -arm64`` when running under Rosetta on macOS."""
    if platform.system() == "Darwin" and platform.machine() == "x86_64":
        arch = shutil.which("arch")
        if arch:
            try:
                os.execvp(arch, [arch, "-arm64", sys.executable, *sys.argv])
            except OSError as exc:  # pragma: no cover - hard failure
                print(
                    f"Failed to re-exec via 'arch -arm64': {exc}\n"
                    "Please run with an arm64 Python interpreter.",
                    file=sys.stderr,
                )
                raise SystemExit(1)
        else:  # pragma: no cover - hard failure
            print(
                "The 'arch' command was not found; unable to launch "
                "arm64 Python.",
                file=sys.stderr,
            )
            raise SystemExit(1)


def configure() -> tuple[list[str], bool]:
    """Parse launcher arguments and ensure a suitable interpreter.

    Returns a tuple of remaining arguments to forward to ``scripts.startup``
    and whether fast mode should be enabled.
    """

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--repair", action="store_true")
    parser.add_argument("--fast", action="store_true")
    parsed_args, forward_args = parser.parse_known_args(sys.argv[1:])

    fast_mode = parsed_args.fast or bool(os.environ.get("SOLHUNTER_FAST"))

    python_exe = find_python(repair=parsed_args.repair)
    if Path(python_exe).resolve() != Path(sys.executable).resolve():
        launcher = Path(__file__).resolve()
        try:
            os.execv(python_exe, [python_exe, str(launcher), *forward_args])
        except OSError as exc:  # pragma: no cover - hard failure
            print(
                f"Failed to re-exec launcher via {python_exe}: {exc}",
                file=sys.stderr,
            )
            raise SystemExit(1)

    return forward_args, fast_mode


def main(
    argv: list[str] | None = None,
    fast_mode: bool | None = None,
) -> NoReturn:
    _ensure_arm64_python()
    forward_args, detected_fast = configure()
    argv = forward_args if argv is None else list(argv)
    fast_mode = detected_fast if fast_mode is None else fast_mode

    from solhunter_zero.macos_setup import ensure_tools  # noqa: E402
    from solhunter_zero.bootstrap_utils import ensure_venv  # noqa: E402
    from solhunter_zero.logging_utils import (  # noqa: E402
        log_startup,
        setup_logging,
    )
    from solhunter_zero.cache_paths import (  # noqa: E402
        TOOLS_OK_MARKER,
        VENV_OK_MARKER,
    )

    setup_logging("startup")

    def _ensure_once(
        marker: Path, action: Callable[[], None], description: str, fast: bool
    ) -> None:
        if fast and marker.exists():
            log_startup(f"Fast mode: skipping {description}")
            return
        action()
        if not marker.exists():
            write_ok_marker(marker)

    _ensure_once(
        TOOLS_OK_MARKER,
        lambda: ensure_tools(non_interactive=True),
        "ensure_tools",
        fast_mode,
    )
    _ensure_once(
        VENV_OK_MARKER,
        lambda: ensure_venv(None),
        "ensure_venv",
        fast_mode,
    )

    log_startup(f"Virtual environment: {sys.prefix}")

    import solhunter_zero.env_config as env_config  # noqa: E402

    env_config.configure_startup_env(ROOT)

    # Configure Rayon thread count once for all downstream imports
    # Use the alias so tests can monkeypatch it easily.
    set_rayon_threads()
    if not (platform.system() == "Darwin" and platform.machine() == "x86_64"):
        gpu_env = device.initialize_gpu() or {}
        os.environ.update(gpu_env)

    # Prefer new orchestrator by default (can be disabled via SOLHUNTER_LEGACY=1)
    legacy = os.getenv("SOLHUNTER_LEGACY", "").lower() in {"1", "true", "yes"}
    if not legacy:
        os.environ.setdefault("NEW_RUNTIME", "1")
        python_exe = sys.executable
        script = "solhunter_zero.runtime"
        cmd = [python_exe, "-m", script, *argv]
        try:
            from solhunter_zero.logging_utils import log_startup
            log_startup("PRIMARY ENTRY POINT active: new runtime orchestrator (event-driven)")
        except Exception:
            pass
    else:
        python_exe = sys.executable
        script = "scripts.startup"
        cmd = [python_exe, "-m", script, *argv]

    try:
        os.execvp(cmd[0], cmd)
    except OSError as exc:  # pragma: no cover - hard failure
        print(f"Failed to launch {script}: {exc}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
