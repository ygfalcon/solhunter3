"""Fetch recent SOL/USD candles for strategy smoke tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable
from urllib.error import URLError, HTTPError
from urllib.request import urlopen

from ..env_settings import api_url

DEFAULT_URL = api_url("COINGECKO_SOLANA_MARKET_URL")
FALLBACK_PATH = Path(__file__).resolve().parents[1] / "data" / "paper_ticks.json"


def _decode_remote(payload: Any) -> list[dict[str, Any]]:
    prices = payload.get("prices") if isinstance(payload, dict) else None
    if not isinstance(prices, Iterable):
        return []
    ticks: list[dict[str, Any]] = []
    for item in prices:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        timestamp, price = item[0], item[1]
        try:
            price_val = float(price)
        except Exception:
            continue
        try:
            ts_val = int(float(timestamp) // 1000)
        except Exception:
            ts_val = 0
        ticks.append({"timestamp": ts_val, "price": price_val})
    return ticks


def _load_fallback(path: Path) -> list[dict[str, Any]]:
    try:
        data = json.loads(path.read_text())
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return []
    if isinstance(data, list):
        return [entry for entry in data if isinstance(entry, dict) and "price" in entry]
    return []


def load_live_ticks(
    url: str | None = None,
    *,
    timeout: float = 10.0,
    fallback_path: str | Path | None = FALLBACK_PATH,
) -> list[dict[str, Any]]:
    """Return recent market data for strategy evaluations.

    Falls back to the bundled ``paper_ticks.json`` dataset when the remote
    request fails.
    """

    target_url = url or DEFAULT_URL
    try:
        with urlopen(target_url, timeout=timeout) as resp:  # type: ignore[arg-type]
            payload = json.load(resp)
    except (URLError, HTTPError, TimeoutError, OSError):
        payload = None
    except Exception:  # pragma: no cover - unexpected failure
        payload = None
    ticks = _decode_remote(payload) if payload else []
    if ticks:
        return ticks
    if fallback_path is None:
        return []
    return _load_fallback(Path(fallback_path))
