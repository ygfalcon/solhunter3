import argparse
import time
from datetime import datetime
from pathlib import Path

from solhunter_zero.offline_data import OfflineData
from solhunter_zero import models


def train_once(
    data: OfflineData,
    token: str,
    out: Path,
    *,
    epochs: int = 10,
    seq_len: int = 30,
) -> bool:
    snaps = data.list_snapshots(token)
    if len(snaps) <= seq_len:
        return False
    model = models.train_deep_lstm(snaps, epochs=epochs, seq_len=seq_len)
    out.parent.mkdir(parents=True, exist_ok=True)
    models.save_model(model, str(out))
    return True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Continuously train price models from offline data"
    )
    parser.add_argument("token", help="Token symbol to train on")
    parser.add_argument(
        "--db",
        default="sqlite:///offline_data.db",
        help="Offline data database URL",
    )
    parser.add_argument(
        "--model",
        default="price_model.pt",
        help="Path to save the model",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=3600.0,
        help="Training interval in seconds",
    )
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--seq-len", type=int, default=30)
    args = parser.parse_args()

    data = OfflineData(args.db)
    out = Path(args.model)

    while True:
        ok = train_once(
            data,
            args.token,
            out,
            epochs=args.epochs,
            seq_len=args.seq_len,
        )
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        if ok:
            print(f"[{ts}] Model updated at {out}")
        else:
            print(f"[{ts}] Not enough data to train")
        time.sleep(args.interval)


if __name__ == "__main__":  # pragma: no cover
    main()
