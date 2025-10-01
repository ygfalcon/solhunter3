"""Utilities for locating a Python interpreter.

This module encapsulates the logic for discovering a suitable Python
interpreter matching the current runtime's major and minor version. Results
are cached in-memory using :func:`functools.lru_cache` and persisted across
runs via the ``SOLHUNTER_PYTHON`` environment variable and
``.cache/python-exe`` file.
"""

from __future__ import annotations

from functools import lru_cache
import os
import platform
import shutil
import subprocess
import sys

from .paths import ROOT

PY_MAJOR, PY_MINOR = sys.version_info[:2]
PY_VERSION = f"{PY_MAJOR}.{PY_MINOR}"
PYTHON_NAME = f"python{PY_VERSION}"
BREW_FORMULA = f"python@{PY_VERSION}"

cache_env = "SOLHUNTER_PYTHON"
cache_dir = ROOT / ".cache"
cache_file = cache_dir / "python-exe"


def _check_python(exe: str) -> bool:
    """Return ``True`` if ``exe`` is a Python interpreter of sufficient version."""

    try:
        out = subprocess.check_output(
            [
                exe,
                "-c",
                "import sys; print('.'.join(map(str, sys.version_info[:2])))",
            ],
            text=True,
        ).strip()
        major, minor = map(int, out.split(".")[:2])
        return (major, minor) >= (PY_MAJOR, PY_MINOR)
    except Exception:  # pragma: no cover - defensive
        return False


def _finalize(path: str) -> str:
    """Persist and return the resolved interpreter path."""

    os.environ[cache_env] = path
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(path)
    except OSError:
        pass
    return path


@lru_cache(maxsize=1)
def _find_python_impl() -> str:
    """Core interpreter discovery logic without repair handling."""

    env_path = os.environ.get(cache_env)
    if env_path and _check_python(env_path):
        return env_path

    if _check_python(sys.executable):
        return _finalize(sys.executable)

    if cache_file.exists():
        cached = cache_file.read_text().strip()
        if _check_python(cached):
            return _finalize(cached)
        try:
            cache_file.unlink()
        except FileNotFoundError:
            pass

    candidates: list[str] = []

    # Existing virtual environment interpreters
    venv = ROOT / ".venv"
    bin_dir = venv / ("Scripts" if os.name == "nt" else "bin")
    for name in (PYTHON_NAME, "python3", "python"):
        p = bin_dir / name
        if p.exists():
            candidates.append(str(p))

    # Interpreters on PATH
    for name in (PYTHON_NAME, "python3", "python"):
        path = shutil.which(name)
        if path:
            candidates.append(path)

    for candidate in candidates:
        if _check_python(candidate):
            return _finalize(candidate)

    if platform.system() == "Darwin":
        try:
            from solhunter_zero.macos_setup import (  # type: ignore
                prepare_macos_env,
            )
        except Exception:  # pragma: no cover - defensive import
            prepare_macos_env = None  # type: ignore
        if prepare_macos_env is not None:
            print(
                f"Python {PY_VERSION} not found; running macOS setup...",
                file=sys.stderr,
            )
            prepare_macos_env(non_interactive=True)
            for name in (PYTHON_NAME, "python3", "python"):
                path = shutil.which(name)
                if path and _check_python(path):
                    return _finalize(path)

    message = f"Python {PY_VERSION} or higher is required."
    if platform.system() == "Darwin":
        message += (
            " Run 'python -c \"from solhunter_zero.macos_setup import "
            "prepare_macos_env; prepare_macos_env()\"' to install "
            f"Python {PY_VERSION} (Homebrew formula {BREW_FORMULA})."
        )
    else:
        message += f" Please install Python {PY_VERSION} and try again."
    print(message, file=sys.stderr)
    raise SystemExit(1)


def find_python(repair: bool = False) -> str:
    """Locate a suitable Python interpreter.

    ``repair`` forces the resolver to ignore any cached values and recompute
    the interpreter path.  The result is cached for subsequent calls.
    """

    repair = repair or bool(os.environ.get("SOLHUNTER_REPAIR"))
    if repair:
        _find_python_impl.cache_clear()
        try:
            cache_file.unlink()
        except FileNotFoundError:
            pass
        os.environ.pop(cache_env, None)

    return _find_python_impl()


__all__ = ["find_python"]
