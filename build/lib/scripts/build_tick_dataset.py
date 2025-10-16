from __future__ import annotations

import json
from argparse import ArgumentParser
from pathlib import Path

from solhunter_zero.offline_data import OfflineData


def build_tick_dataset(db: str, out: str, token: str | None = None) -> None:
    """Export tick-level price and liquidity history from ``db`` to ``out``."""
    data = OfflineData(f"sqlite:///{db}")
    snaps = data.list_snapshots(token)
    ticks = [
        {
            "timestamp": s.timestamp.isoformat(),
            "price": float(s.price),
            "depth": float(s.depth),
            "slippage": float(getattr(s, "slippage", 0.0)),
            "volume": float(getattr(s, "volume", 0.0)),
            "imbalance": float(s.imbalance),
        }
        for s in snaps
    ]
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(ticks, fh)


def main(argv: list[str] | None = None) -> int:
    p = ArgumentParser(description="Export tick history dataset")
    p.add_argument("--db", default="offline_data.db", help="Offline data DB path")
    p.add_argument("--out", default="datasets/tick_history.json", help="Output JSON file")
    p.add_argument("--token", help="Filter by token")
    args = p.parse_args(argv)
    build_tick_dataset(args.db, args.out, token=args.token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
