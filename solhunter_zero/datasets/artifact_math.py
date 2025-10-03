"""Loader for the artifact glyph arithmetic dataset used by the demo agents."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from . import DATA_DIR

DEFAULT_PATH = str(DATA_DIR / "artifact_math.json")


def load_artifact_math(path: str | Path | None = None) -> List[Dict[str, Any]] | Dict[str, Any]:
    """Load the artifact math dataset from ``path``.

    The file is expected to contain JSON data.  A copy of the dataset is bundled
    with the repository so the function gracefully handles missing files by
    returning an empty list.
    """

    data_path = Path(path or DEFAULT_PATH)
    try:
        with data_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except FileNotFoundError:
        return []


__all__ = ["DEFAULT_PATH", "load_artifact_math"]
