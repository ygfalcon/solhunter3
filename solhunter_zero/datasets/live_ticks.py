from __future__ import annotations

"""Fetch a small slice of recent SOL/USD market data.

The loader retrieves hourly candles from the same public Codex (Coingecko)
endpoint used by :mod:`paper`.  Results are cached for the duration of the
process so repeat calls avoid additional network traffic.  When the request
fails – for example due to missing network access – an empty list is returned
so callers can gracefully skip live-data scenarios.
"""

from typing import Any, Dict, List
from urllib.error import URLError
from urllib.request import urlopen
import json

# Public endpoint providing recent SOL/USD candles.  The fetch is best-effort
# and consumers are expected to fall back to local samples when unavailable.
CODEX_URL = (
    "https://api.coingecko.com/api/v3/coins/solana/market_chart"
    "?vs_currency=usd&days=1&interval=hourly"
)

_cache: List[Dict[str, Any]] | None = None


def load_live_ticks(limit: int = 120) -> List[Dict[str, Any]]:
    """Return up to ``limit`` recent SOL/USD candles.

    When the endpoint cannot be reached or returns no data an empty list is
    returned.  Entries contain ``timestamp`` (seconds since the epoch) and
    ``price`` fields matching the format expected by strategy tests.
    """

    global _cache
    if _cache is not None and len(_cache) >= limit:
        return _cache[:limit]

    try:
        with urlopen(CODEX_URL, timeout=10) as resp:
            data = json.load(resp)
    except (URLError, OSError, json.JSONDecodeError):
        _cache = []
        return _cache

    prices = data.get("prices") or []
    ticks: List[Dict[str, Any]] = []
    for p in prices:
        try:
            ts = int(p[0] // 1000)
            price = float(p[1])
        except Exception:
            continue
        ticks.append({"timestamp": ts, "price": price})
        if len(ticks) >= limit:
            break

    _cache = ticks
    return ticks


__all__ = ["load_live_ticks"]

