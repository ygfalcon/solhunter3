from __future__ import annotations

import asyncio
from argparse import ArgumentParser

from solhunter_zero.offline_data import OfflineData


def main(argv: list[str] | None = None) -> int:
    p = ArgumentParser(description="Export offline data to mmap-able npz")
    p.add_argument("--db", default="offline_data.db", help="SQLite DB file")
    p.add_argument("--out", default="datasets/offline_data.npz", help="Output npz file")
    p.add_argument("--token", help="Filter by token")
    args = p.parse_args(argv)

    data = OfflineData(f"sqlite:///{args.db}")
    asyncio.run(data.export_npz(args.out, token=args.token))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
