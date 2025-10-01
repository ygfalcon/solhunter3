"""Loader for the simple artifact math dataset."""
from __future__ import annotations

from ..jsonutil import loads
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any, List, Dict

# Default dataset path relative to the package
DEFAULT_PATH = resources.files("solhunter_zero") / "data" / "artifact_math.json"


# Module level cache so we do not repeatedly load the same file
_cache_path: str | None = None
_cache_data: Any | None = None


def load_artifact_math(path: Path | Traversable | str = DEFAULT_PATH) -> List[Dict[str, Any]] | Dict[str, Any]:
    """Return the artifact math dataset located at ``path``.

    The result is cached on the module level; subsequent calls with the same
    ``path`` will reuse the previously loaded data.
    """
    global _cache_path, _cache_data
    if _cache_data is not None and _cache_path == str(path):
        return _cache_data

    try:
        obj = Path(path) if isinstance(path, str) else path
        with resources.as_file(obj) as p:
            with p.open("r", encoding="utf-8") as fh:
                data = loads(fh.read())
    except Exception:
        _cache_path = str(path)
        _cache_data = []
        return _cache_data

    _cache_path = str(path)
    _cache_data = data
    return data
