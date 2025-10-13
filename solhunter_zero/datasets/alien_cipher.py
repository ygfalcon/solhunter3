"""Dataset loader for the logistic-map cipher agent."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

DEFAULT_PATH = Path(__file__).resolve().parents[1] / "data" / "alien_cipher.json"


def load_alien_cipher(path: str | bytes | Path | None = None) -> Dict[str, Dict[str, Any]]:
    """Return mapping of token → cipher parameters."""

    target = Path(path) if path else DEFAULT_PATH
    try:
        data = json.loads(target.read_text())
    except FileNotFoundError:
        return {}
    except OSError:
        return {}
    except json.JSONDecodeError:
        return {}
    except Exception:  # pragma: no cover - defensive
        return {}
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    return {}
