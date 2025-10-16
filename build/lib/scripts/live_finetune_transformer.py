import argparse
import asyncio
import os
from pathlib import Path

import torch
from torch.utils.data import DataLoader, TensorDataset
import solhunter_zero.device as device

from solhunter_zero.offline_data import OfflineData
from solhunter_zero import models


async def train_loop(db_url: str, model_path: Path, device: torch.device, seq_len: int, batch_size: int, interval: float, lr: float) -> None:
    data = OfflineData(db_url)
    while True:
        snaps = data.list_snapshots()
        if len(snaps) <= seq_len:
            await asyncio.sleep(interval)
            continue
        X, y = models.make_snapshot_training_data(snaps, seq_len=seq_len)
        dataset = TensorDataset(X, y)
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

        model = models.get_model(str(model_path))
        if model is None or getattr(model, "input_dim", X.size(-1)) != X.size(-1):
            model = models.DeepTransformerModel(X.size(-1))
        model.to(device)
        opt = torch.optim.Adam(model.parameters(), lr=lr)
        loss_fn = torch.nn.MSELoss()
        for X_b, y_b in loader:
            X_b = X_b.to(device)
            y_b = y_b.to(device)
            opt.zero_grad()
            pred = model(X_b)
            loss = loss_fn(pred, y_b)
            loss.backward()
            opt.step()
        models.save_model(model.to("cpu"), str(model_path))
        model.to(device)
        os.environ["PRICE_MODEL_PATH"] = str(model_path)
        await asyncio.sleep(interval)


def main() -> None:
    p = argparse.ArgumentParser(description="Fine-tune transformer model during trading")
    p.add_argument("--db", default="sqlite:///offline_data.db")
    p.add_argument("--model", default="transformer_model.pt")
    p.add_argument("--device", default="auto")
    p.add_argument("--seq-len", type=int, default=30)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--interval", type=float, default=3600.0)
    p.add_argument("--lr", type=float, default=1e-4)
    args = p.parse_args()

    dev = device.get_default_device(args.device)

    asyncio.run(train_loop(args.db, Path(args.model), dev, args.seq_len, args.batch_size, args.interval, args.lr))


if __name__ == "__main__":  # pragma: no cover
    main()
