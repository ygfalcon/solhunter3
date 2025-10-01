import argparse
import asyncio

from solhunter_zero.offline_data import OfflineData
from solhunter_zero.models.regime_classifier import (
    train_regime_classifier,
    save_regime_model,
)


async def _load_prices(data: OfflineData, token: str | None):
    snaps = await data.list_snapshots(token)
    return [float(s.price) for s in snaps]


def main() -> None:
    p = argparse.ArgumentParser(
        description="Train regime classification model"
    )
    p.add_argument(
        "--db",
        default="offline_data.db",
        help="Offline data database",
    )
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    p.add_argument("--seq-len", type=int, default=30)
    p.add_argument("--threshold", type=float, default=0.02)
    p.add_argument("--model", choices=["lstm", "transformer"], default="lstm")
    args = p.parse_args()

    data = OfflineData(f"sqlite:///{args.db}")
    prices = asyncio.run(_load_prices(data, args.token))
    if len(prices) <= args.seq_len + 1:
        raise SystemExit("not enough data to train")

    model = train_regime_classifier(
        prices,
        seq_len=args.seq_len,
        epochs=args.epochs,
        threshold=args.threshold,
        model_type=args.model,
    )
    save_regime_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
