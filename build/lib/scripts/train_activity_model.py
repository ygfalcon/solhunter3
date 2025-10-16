import argparse
import asyncio

from solhunter_zero.offline_data import OfflineData
from solhunter_zero.models import save_model
from solhunter_zero.models.token_activity_model import train_activity_model


async def _load(data: OfflineData, token: str | None):
    snaps = await data.list_snapshots(token)
    trades = await data.list_trades(token)
    return snaps, trades


def main() -> None:
    p = argparse.ArgumentParser(description="Train token activity model")
    p.add_argument("--db", default="offline_data.db", help="Offline data database")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    args = p.parse_args()

    data = OfflineData(f"sqlite:///{args.db}")
    snaps, trades = asyncio.run(_load(data, args.token))
    model = train_activity_model(snaps, trades, epochs=args.epochs)
    save_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()

