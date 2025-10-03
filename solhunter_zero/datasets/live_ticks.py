"""Helpers for fetching lightweight live tick datasets."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, List
from urllib.error import URLError
from urllib.request import urlopen

from . import DATA_DIR

DEFAULT_URL = "https://api.coingecko.com/api/v3/coins/solana/market_chart?vs_currency=usd&days=1"
DEFAULT_FALLBACK_PATH = str(DATA_DIR / "paper_ticks.json")


def load_live_ticks(
    url: str | None = None,
    *,
    fallback_path: str | Path | None = DEFAULT_FALLBACK_PATH,
    timeout: float = 5.0,
) -> List[dict[str, Any]]:
    """Return recent SOL/USD ticks.

    The function attempts to download JSON from ``url`` first.  When the request
    fails or the environment lacks network access we fall back to reading the
    bundled ``paper_ticks.json`` file.  If both sources are unavailable an empty
    list is returned.
    """

    target = url or os.getenv("SOLHUNTER_LIVE_TICKS_URL") or DEFAULT_URL
    try:
        with urlopen(target, timeout=timeout) as response:  # pragma: no cover - network
            payload = json.load(response)
    except (URLError, OSError, ValueError):
        payload = None

    if isinstance(payload, dict) and "prices" in payload:
        return [
            {"timestamp": entry[0], "price": float(entry[1])}
            for entry in payload.get("prices", [])
            if isinstance(entry, list) and len(entry) >= 2
        ]
    if isinstance(payload, list):
        return [entry for entry in payload if isinstance(entry, dict)]

    if fallback_path:
        path = Path(fallback_path)
        if path.exists():
            try:
                with path.open("r", encoding="utf-8") as handle:
                    data = json.load(handle)
            except (OSError, ValueError):
                data = None
            if isinstance(data, list):
                return [entry for entry in data if isinstance(entry, dict)]
    return []


__all__ = ["DEFAULT_URL", "DEFAULT_FALLBACK_PATH", "load_live_ticks"]
