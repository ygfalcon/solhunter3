import argparse
import asyncio

from solhunter_zero.offline_data import OfflineData
from solhunter_zero.models.var_forecaster import (
    train_var_forecaster,
    save_var_model,
)


async def _load_prices(data: OfflineData, token: str | None):
    snaps = await data.list_snapshots(token)
    return [float(s.price) for s in snaps]


def main() -> None:
    p = argparse.ArgumentParser(description="Train VaR forecasting model")
    p.add_argument("--db", default="offline_data.db", help="Offline data database")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    p.add_argument("--seq-len", type=int, default=30)
    p.add_argument("--confidence", type=float, default=0.95)
    args = p.parse_args()

    data = OfflineData(f"sqlite:///{args.db}")
    prices = asyncio.run(_load_prices(data, args.token))
    if len(prices) <= args.seq_len + 1:
        raise SystemExit("not enough data to train")

    model = train_var_forecaster(
        prices,
        seq_len=args.seq_len,
        epochs=args.epochs,
        confidence=args.confidence,
    )
    save_var_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()

