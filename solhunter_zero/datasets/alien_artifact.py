"""Glyph encoding dataset utilities used by :mod:`solhunter_zero` demos."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List

from . import DATA_DIR

DEFAULT_PATH = str(DATA_DIR / "alien_artifact_patterns.json")


def load_patterns(path: str | Path | None = None) -> List[Dict[str, Any]]:
    """Load artifact glyph patterns from ``path``."""

    data_path = Path(path or DEFAULT_PATH)
    try:
        with data_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return []

    if isinstance(data, list):
        patterns: List[Dict[str, Any]] = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            glyphs = str(entry.get("glyphs", ""))
            encoding = entry.get("encoding", [])
            if isinstance(encoding, list):
                patterns.append({"glyphs": glyphs, "encoding": list(encoding)})
        return patterns

    raise ValueError(f"Unexpected artifact dataset format: {type(data)!r}")


@lru_cache(maxsize=None)
def _lookup_map(path: str | Path | None = None) -> Dict[str, List[int]]:
    key = Path(path or DEFAULT_PATH)
    patterns = load_patterns(key)
    return {entry["glyphs"]: list(entry["encoding"]) for entry in patterns}


def get_encoding_by_glyphs(glyphs: str, path: str | Path | None = None) -> List[int]:
    """Return the encoding associated with ``glyphs``."""

    return list(_lookup_map(path).get(glyphs, []))


__all__ = ["DEFAULT_PATH", "load_patterns", "get_encoding_by_glyphs"]
