"""Alien artifact pattern dataset loader."""
from __future__ import annotations

from ..jsonutil import loads
from importlib import resources
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import List, Dict, Any

# Path to the dataset JSON file bundled with the package
DEFAULT_PATH = resources.files("solhunter_zero") / "data" / "alien_artifact_patterns.json"


_patterns: List[Dict[str, Any]] | None = None


def _load_dataset(path: Path | Traversable | str = DEFAULT_PATH) -> List[Dict[str, Any]]:
    obj = Path(path) if isinstance(path, str) else path
    with resources.as_file(obj) as p:
        with p.open("r", encoding="utf-8") as fh:
            return loads(fh.read())


def load_patterns(path: Path | Traversable | str = DEFAULT_PATH) -> List[Dict[str, Any]]:
    """Return list of alien artifact patterns."""
    global _patterns
    if _patterns is None or path != DEFAULT_PATH:
        _patterns = _load_dataset(path)
    return _patterns


def get_encoding_by_glyphs(glyphs: str) -> List[int] | None:
    """Return the encoding list for the given glyph sequence or ``None``."""
    for entry in load_patterns():
        if entry.get("glyphs") == glyphs:
            enc = entry.get("encoding")
            if isinstance(enc, list):
                return enc
    return None

