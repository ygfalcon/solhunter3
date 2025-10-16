import argparse
import asyncio

from solhunter_zero.offline_data import OfflineData
from solhunter_zero.models import save_model
from solhunter_zero.models.graph_price_model import train_graph_model


async def _load_snaps(data: OfflineData, token: str | None):
    snaps = await data.list_snapshots(token)
    return snaps


def main() -> None:
    p = argparse.ArgumentParser(description="Train graph-based price model")
    p.add_argument("--db", default="offline_data.db", help="Offline data database")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    args = p.parse_args()

    data = OfflineData(f"sqlite:///{args.db}")
    snaps = asyncio.run(_load_snaps(data, args.token))
    if len(snaps) <= 1:
        raise SystemExit("not enough data to train")

    model = train_graph_model(snaps, epochs=args.epochs)
    save_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
