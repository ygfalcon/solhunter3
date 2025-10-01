"""Test-time stubs for optional dependencies.

This module installs lightweight shims for heavy optional dependencies when
the environment variable ``SOLHUNTER_TESTING`` is set. In production, where
the variable is unset, missing imports are re-raised so the application fails
fast instead of running with incomplete functionality.
"""

import importlib.machinery
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent
bin_dir = ROOT / "target" / "release"
os.environ["PATH"] = f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}"
lib_dir = ROOT / "solhunter_zero"
_ld = os.environ.get("LD_LIBRARY_PATH")
os.environ["LD_LIBRARY_PATH"] = (
    f"{lib_dir}{os.pathsep}{_ld}" if _ld else str(lib_dir)
)

TESTING = os.getenv("SOLHUNTER_TESTING")

try:  # pragma: no cover - optional dependency
    import sqlalchemy  # type: ignore # noqa: F401
except Exception:  # pragma: no cover
    if TESTING:
        sa = types.ModuleType("sqlalchemy")
        sa.__spec__ = importlib.machinery.ModuleSpec("sqlalchemy", None)
        sys.modules.setdefault("sqlalchemy", sa)
    else:  # pragma: no cover
        raise

try:  # pragma: no cover - optional dependency
    import aiohttp  # type: ignore # noqa: F401
except Exception:  # pragma: no cover
    if TESTING:
        aiohttp_mod = types.ModuleType("aiohttp")
        aiohttp_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp", None)
        aiohttp_mod.ClientSession = object
        aiohttp_mod.TCPConnector = object
        sys.modules.setdefault("aiohttp", aiohttp_mod)
    else:  # pragma: no cover
        raise

if TESTING:
    try:  # pragma: no cover - install broader stubs when available
        from tests import stubs as _stubs  # type: ignore

        _stubs.install_stubs()
    except Exception:  # pragma: no cover
        pass
