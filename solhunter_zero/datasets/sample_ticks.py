"""Loader for sample tick history used in tests and demos."""
from __future__ import annotations

from ..jsonutil import loads
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Any, Dict, List

# Path to the sample ticks dataset bundled with the package
DEFAULT_PATH = resources.files("solhunter_zero") / "data" / "sample_ticks.json"


_cache_path: str | None = None
_cache_data: List[Dict[str, Any]] | None = None


def load_sample_ticks(path: Path | Traversable | str = DEFAULT_PATH) -> List[Dict[str, Any]]:
    """Return sample tick entries located at ``path``."""
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

    if not isinstance(data, list):
        data = []

    _cache_path = str(path)
    _cache_data = data
    return data
