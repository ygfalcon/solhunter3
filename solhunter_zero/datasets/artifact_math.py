"""Arithmetic dataset utilities for :class:`ArtifactMathAgent`."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / "artifact_math.json"


def load_artifact_math(path: str | bytes | Path | None = None) -> list[Any] | dict[str, Any]:
    """Return arithmetic mapping data for glyph evaluation."""

    target = Path(path) if path else DEFAULT_PATH
    try:
        data = json.loads(target.read_text())
    except FileNotFoundError:
        return []
    except OSError:
        return []
    except json.JSONDecodeError:
        return []
    except Exception:  # pragma: no cover - unexpected failure
        return []
    if isinstance(data, (list, dict)):
        return data
    return []
