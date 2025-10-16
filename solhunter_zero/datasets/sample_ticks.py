"""Load sample tick data bundled with the repository, robustly.

Supports:
- JSON array files: [1.0, 1.01, ...] or [{"price": ...}, ...]
- NDJSON files (one JSON object/number per line)
- Gzipped variants (*.gz)
- Light schema normalization for common price keys: "price", "close", "p", "vwap"

Always fails soft (returns []) on unrecoverable errors.
"""

from __future__ import annotations

import gzip
import io
import json
import os
from pathlib import Path
from typing import Any, Iterable

_DEFAULT_DATA = "sample_ticks.json"
DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / _DEFAULT_DATA

# Optional soft cap to keep dev/test loops snappy (set 0 or unset to disable)
_MAX_TICKS_ENV = os.getenv("SAMPLE_TICKS_MAX", "").strip()
try:
    _MAX_TICKS = int(_MAX_TICKS_ENV) if _MAX_TICKS_ENV else 0
except ValueError:
    _MAX_TICKS = 0


def _maybe_gzip_read_bytes(target: Path) -> bytes | None:
    """Return file bytes, transparently handling .gz, or None on error."""
    try:
        data = target.read_bytes()
    except (FileNotFoundError, OSError):
        return None
    # If file is gzipped by extension or magic header, decompress
    if target.suffix == ".gz" or (len(data) >= 2 and data[:2] == b"\x1f\x8b"):
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
                return gz.read()
        except OSError:
            # Not actually gzipped or corrupt: fall back to raw bytes
            return data
    return data


def _parse_json_array(text: str) -> list[Any] | None:
    try:
        obj = json.loads(text)
        if isinstance(obj, list):
            return list(obj)
        return None
    except json.JSONDecodeError:
        return None
    except Exception:
        return None


def _parse_ndjson(text: str) -> list[Any] | None:
    # Accept numbers or objects per line; ignore blank/comment-ish lines
    items: list[Any] = []
    got_any = False
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        try:
            items.append(json.loads(s))
            got_any = True
        except json.JSONDecodeError:
            # If any line is malformed, treat whole file as not NDJSON
            return None
        except Exception:
            return None
    return items if got_any else None


def _normalize_entries(data: Iterable[Any]) -> list[Any]:
    """Normalize to a list of numbers or dicts with a 'price' key."""
    out: list[Any] = []
    for entry in data:
        # Already numeric: keep as-is
        if isinstance(entry, (int, float)):
            out.append(entry)
            continue
        # Dict-like with a price-ish field
        if isinstance(entry, dict):
            if "price" in entry:
                out.append(entry)
                continue
            # Common aliases
            for k in ("close", "p", "vwap"):
                if k in entry:
                    # Repackage as a canonical shape to help downstream code
                    try:
                        price = float(entry[k])
                    except Exception:
                        break
                    out.append({"price": price, **{kk: vv for kk, vv in entry.items() if kk != k}})
                    break
            else:
                # Keep unknown dicts; AgentManager tolerates dicts in its own path
                out.append(entry)
                continue
        # Anything else: ignore quietly
        # (Strings, lists, etc. that don't map to a tick entry)
    # Optional soft cap
    if _MAX_TICKS and len(out) > _MAX_TICKS:
        return out[-_MAX_TICKS:]
    return out


def load_sample_ticks(path: str | bytes | Path | None = None) -> list[Any]:
    """Return decoded tick entries from ``path`` or the bundled dataset.

    The function tries, in order:
      1) explicit path (if given),
      2) bundled default (``data/sample_ticks.json``),
    and accepts JSON arrays, NDJSON, and gzipped variants.

    Returns [] on error.
    """
    # Resolve target path
    target = Path(path) if path is not None else DEFAULT_PATH

    # Try direct read of the provided path
    data = _maybe_gzip_read_bytes(target)
    if data is None and path is not None:
        # If explicit path fails, don't try the default; respect caller intent.
        return []

    # Fall back to default if needed
    if data is None:
        data = _maybe_gzip_read_bytes(DEFAULT_PATH)
        if data is None:
            return []

    # Decode text best-effort
    try:
        text = data.decode("utf-8")
    except Exception:
        try:
            text = data.decode("latin-1")
        except Exception:
            return []

    # Try JSON array first
    arr = _parse_json_array(text)
    if arr is None:
        # Try NDJSON
        arr = _parse_ndjson(text)
    if arr is None:
        return []

    return _normalize_entries(arr)
