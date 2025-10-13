"""Load sample tick data bundled with the repository."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Sequence

_DEFAULT_DATA = "sample_ticks.json"
DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / _DEFAULT_DATA


def _normalize_entries(data: Any) -> list[Any]:
    if isinstance(data, list):
        return [entry for entry in data]
    if isinstance(data, Sequence):  # pragma: no cover - defensive fallback
        return list(data)
    return []


def load_sample_ticks(path: str | bytes | Path | None = None) -> list[Any]:
    """Return decoded tick entries from ``path`` or the bundled dataset."""

    target = Path(path) if path else DEFAULT_PATH
    try:
        text = target.read_text()
    except FileNotFoundError:
        return []
    except OSError:
        return []
    try:
        raw = json.loads(text)
    except json.JSONDecodeError:
        return []
    except Exception:  # pragma: no cover - unexpected decoder failure
        return []
    return _normalize_entries(raw)
