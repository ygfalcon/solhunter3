import argparse
import asyncio

from solhunter_zero.offline_data import OfflineData
from solhunter_zero.models.onchain_forecaster import (
    train_lstm,
    train_transformer,
    save_model,
)


async def _load_snaps(data: OfflineData, token: str | None):
    return await data.list_snapshots(token)


def main() -> None:
    p = argparse.ArgumentParser(description="Train on-chain mempool forecaster")
    p.add_argument("--db", default="offline_data.db", help="Offline data database")
    p.add_argument("--out", required=True, help="Output model path")
    p.add_argument("--token", help="Optional token filter")
    p.add_argument("--epochs", type=int, default=20)
    p.add_argument("--seq-len", type=int, default=30)
    p.add_argument("--model", choices=["lstm", "transformer"], default="lstm")
    args = p.parse_args()

    data = OfflineData(f"sqlite:///{args.db}")
    snaps = asyncio.run(_load_snaps(data, args.token))
    if len(snaps) <= args.seq_len:
        raise SystemExit("not enough data to train")

    if args.model == "transformer":
        model = train_transformer(snaps, seq_len=args.seq_len, epochs=args.epochs)
    else:
        model = train_lstm(snaps, seq_len=args.seq_len, epochs=args.epochs)
    save_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
