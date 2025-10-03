"""Utility helpers for working with bundled sample tick data."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, List

from . import DATA_DIR

DEFAULT_PATH = str(DATA_DIR / "sample_ticks.json")


def load_sample_ticks(path: str | Path | None = None) -> List[dict[str, Any]]:
    """Load sample tick data from ``path``.

    The dataset ships with the repository.  When the path does not exist the
    function returns an empty list which mirrors the behaviour of the legacy
    implementation that silently ignored missing files.
    """

    data_path = Path(path or DEFAULT_PATH)
    try:
        with data_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        return []

    if isinstance(data, list):
        return [
            {
                "timestamp": entry.get("timestamp"),
                "price": float(entry.get("price", 0.0)),
                "depth": float(entry.get("depth", 0.0)),
                "imbalance": float(entry.get("imbalance", 0.0)),
            }
            for entry in data
            if isinstance(entry, dict)
        ]

    raise ValueError(f"Unexpected sample tick dataset format: {type(data)!r}")


def iter_sample_prices(path: str | Path | None = None) -> Iterable[float]:
    """Iterate over prices from :func:`load_sample_ticks`."""

    for entry in load_sample_ticks(path):
        yield float(entry.get("price", 0.0))


__all__ = ["DEFAULT_PATH", "load_sample_ticks", "iter_sample_prices"]
