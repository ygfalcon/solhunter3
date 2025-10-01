import argparse
import pandas as pd
from solhunter_zero.models import train_price_model, save_model


def main() -> None:
    parser = argparse.ArgumentParser(description="Train price prediction model")
    parser.add_argument(
        "csv",
        help=(
            "CSV with columns price, liquidity, depth, tx_count, mempool_rate, "
            "whale_share, spread"
        ),
    )
    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--seq-len", type=int, default=30)
    parser.add_argument("--out", required=True, help="Path to save trained model")
    args = parser.parse_args()

    data = pd.read_csv(args.csv)
    prices = data["price"].astype(float).tolist()
    liq = data["liquidity"].astype(float).tolist()
    depth = data["depth"].astype(float).tolist()
    tx = data["tx_count"].astype(float).tolist() if "tx_count" in data.columns else None
    mem = (
        data["mempool_rate"].astype(float).tolist()
        if "mempool_rate" in data.columns
        else None
    )
    whales = (
        data["whale_share"].astype(float).tolist()
        if "whale_share" in data.columns
        else None
    )
    spread = (
        data["spread"].astype(float).tolist() if "spread" in data.columns else None
    )

    model = train_price_model(
        prices,
        liq,
        depth,
        tx,
        slippage=None,
        volume=None,
        mempool_rates=mem,
        whale_shares=whales,
        spreads=spread,
        seq_len=args.seq_len,
        epochs=args.epochs,
    )
    save_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
