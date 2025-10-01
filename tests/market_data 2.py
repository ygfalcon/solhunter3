from __future__ import annotations

import json
from pathlib import Path
from typing import List, Tuple


def load_live_prices() -> Tuple[List[float], List[str]]:
    """Return deterministic price data for tests.

    The data is sourced from ``tests/data/prices_short.json`` which contains
    a small slice of historical prices.  Using a file bundled with the test
    suite avoids any network calls while still exercising the code paths that
    expect "real" market data.
    """
    data_path = Path(__file__).with_name("data") / "prices_short.json"
    data = json.loads(data_path.read_text())
    prices = [float(entry["price"]) for entry in data]
    dates = [str(entry.get("date", i)) for i, entry in enumerate(data)]
    return prices, dates
