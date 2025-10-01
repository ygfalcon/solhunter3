from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, List, Mapping

from .event_bus import subscribe

from .offline_data import OfflineData


class Snapshot:
    """Simple container for order book snapshot attributes."""

    def __init__(self, price: float, depth: float, imbalance: float, *, slippage: float = 0.0, volume: float = 0.0, tx_rate: float = 0.0) -> None:
        self.price = price
        self.depth = depth
        self.slippage = slippage
        self.volume = volume
        self.imbalance = imbalance
        self.tx_rate = tx_rate


def map_snapshot(entry: Mapping[str, float]) -> dict:
    """Return a ``dict`` suitable for :class:`MarketSnapshot` columns."""

    return {
        "price": float(entry.get("price", 0.0)),
        "depth": float(entry.get("depth", 0.0)),
        "total_depth": float(entry.get("total_depth", 0.0)),
        "slippage": float(entry.get("slippage", 0.0)),
        "volume": float(entry.get("volume", 0.0)),
        "imbalance": float(entry.get("imbalance", 0.0)),
        "tx_rate": float(entry.get("tx_rate", 0.0)),
        "whale_share": float(entry.get("whale_share", 0.0)),
        "spread": float(entry.get("spread", 0.0)),
        "sentiment": float(entry.get("sentiment", 0.0)),
    }


def load_high_freq_snapshots(
    db_url: str = "sqlite:///offline_data.db", dataset_dir: str | Path = "datasets"
) -> List[Any]:
    """Load snapshots from ``offline_data.db`` and JSON datasets."""

    data = OfflineData(db_url)
    snaps = list(data.list_snapshots())

    dir_path = Path(dataset_dir)
    for file in dir_path.glob("*.json"):
        try:
            with open(file, "r", encoding="utf-8") as fh:
                items = json.load(fh)
        except Exception:
            continue
        for it in items:
            snaps.append(
                Snapshot(
                    price=float(it.get("price", 0.0)),
                    depth=float(it.get("depth", 0.0)),
                    imbalance=float(it.get("imbalance", 0.0)),
                )
            )
    return snaps


def start_depth_snapshot_listener(
    data: OfflineData | str = "offline_data.db",
) -> Callable[[], None]:
    """Subscribe to ``depth_update`` events and log snapshots.

    Parameters
    ----------
    data:
        Either an :class:`OfflineData` instance or path to the database file.

    Returns
    -------
    Callable
        Function that unregisters the subscriber when called.
    """

    offline = data if isinstance(data, OfflineData) else OfflineData(f"sqlite:///{data}")

    async def _handler(payload: Mapping[str, Mapping[str, float]]) -> None:
        for token, entry in payload.items():
            await offline.log_snapshot(token=token, **map_snapshot(entry))

    return subscribe("depth_update", _handler)

