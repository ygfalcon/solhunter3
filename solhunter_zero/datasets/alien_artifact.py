"""Helper utilities for glyph-based artifact datasets."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / "alien_artifact_patterns.json"


def load_patterns(path: str | bytes | Path | None = None) -> list[dict[str, Any]]:
    """Return decoded glyph patterns from ``path`` or the bundled dataset."""

    target = Path(path) if path else DEFAULT_PATH
    try:
        data = json.loads(target.read_text())
    except FileNotFoundError:
        return []
    except OSError:
        return []
    except json.JSONDecodeError:
        return []
    except Exception:  # pragma: no cover - unexpected decoder failure
        return []
    patterns: list[dict[str, Any]] = []
    if isinstance(data, list):
        for entry in data:
            if isinstance(entry, dict) and "glyphs" in entry:
                patterns.append(entry)
    return patterns


def get_encoding_by_glyphs(
    glyphs: str,
    patterns: Sequence[Mapping[str, Any]] | None = None,
) -> list[int]:
    """Lookup the integer encoding for ``glyphs``.

    When ``patterns`` is ``None`` the bundled dataset is used.
    """

    dataset: Iterable[Mapping[str, Any]]
    if patterns is None:
        dataset = load_patterns()
    else:
        dataset = patterns
    for entry in dataset:
        glyph_seq = entry.get("glyphs") if isinstance(entry, Mapping) else None
        if not isinstance(glyph_seq, str) or glyph_seq != glyphs:
            continue
        raw = entry.get("encoding")
        if isinstance(raw, Sequence):
            result: list[int] = []
            for value in raw:
                try:
                    result.append(int(value))
                except Exception:
                    continue
            return result
        break
    return []
