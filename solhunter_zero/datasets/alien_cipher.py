"""Loader for the bundled alien cipher coefficient dataset."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from . import DATA_DIR

DEFAULT_PATH = str(DATA_DIR / "alien_cipher.json")


def load_alien_cipher(path: str | Path | None = None) -> Dict[str, Dict[str, Any]]:
    """Return the alien cipher coefficients from ``path``.

    The dataset ships with the repository under :mod:`solhunter_zero.data` and is
    represented as a JSON object mapping token symbols to coefficient metadata.
    Missing files simply result in an empty mapping which keeps the agents
    importable in environments where the optional datasets are absent.
    """

    data_path = Path(path or DEFAULT_PATH)
    try:
        with data_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return {}

    if isinstance(data, dict):
        return {str(token): dict(info) for token, info in data.items() if isinstance(info, dict)}

    raise ValueError(f"Unexpected alien cipher dataset format: {type(data)!r}")


__all__ = ["DEFAULT_PATH", "load_alien_cipher"]
