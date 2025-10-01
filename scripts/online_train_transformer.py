import argparse
import asyncio
import logging
import os
from pathlib import Path
from itertools import product

import torch
from torch.utils.data import DataLoader, TensorDataset
import solhunter_zero.device as device

from solhunter_zero.offline_data import OfflineData
from solhunter_zero import models
from solhunter_zero.regime import detect_regime


def build_loader(
    data: OfflineData,
    seq_len: int,
    batch_size: int,
    regime: str | None = None,
):
    """Return DataLoader and feature dimension."""
    snaps = data.list_snapshots()
    if len(snaps) <= seq_len:
        return None, 0
    prices = [float(s.price) for s in snaps]
    X_full, y_full = models.make_snapshot_training_data(snaps, seq_len=seq_len)
    regimes = [
        detect_regime(prices[i : i + seq_len + 1]) for i in range(len(prices) - seq_len)
    ]
    if regime is not None:
        idx = [i for i, r in enumerate(regimes) if r == regime]
        if not idx:
            return None, 0
        X_full = X_full[idx]
        y_full = y_full[idx]
    dataset = TensorDataset(X_full, y_full)
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    return loader, X_full.size(-1)


def load_or_create_model(path: Path, device: torch.device, input_dim: int) -> torch.nn.Module:
    model = models.get_model(str(path))
    if model is None or getattr(model, "input_dim", input_dim) != input_dim:
        model = models.DeepTransformerModel(input_dim)
    model.to(device)
    return model


def evaluate(model: torch.nn.Module, loader: DataLoader, device: torch.device) -> float:
    loss_fn = torch.nn.MSELoss()
    total = 0.0
    n = 0
    model.eval()
    with torch.no_grad():
        for X_b, y_b in loader:
            X_b = X_b.to(device)
            y_b = y_b.to(device)
            pred = model(X_b)
            total += loss_fn(pred, y_b).item() * len(y_b)
            n += len(y_b)
    return total / max(n, 1)


def hyper_search(loader: DataLoader, val_loader: DataLoader, input_dim: int, device: torch.device) -> tuple[float, int]:
    params = [(lr, h) for lr, h in product([1e-3, 5e-4], [32, 64])]
    best = (1e-3, 32)
    best_loss = float("inf")
    for lr, hid in params:
        model = models.DeepTransformerModel(input_dim, hidden_dim=hid)
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
        val_loss = evaluate(model, val_loader, device)
        if val_loss < best_loss:
            best_loss = val_loss
            best = (lr, hid)
    return best


async def train_loop(
    db_url: str,
    model_path: Path,
    device: torch.device,
    seq_len: int,
    batch_size: int,
    interval: float,
    regime: str | None,
    *,
    daemon: bool = False,
    log_progress: bool = False,
    eval_interval: float | None = None,
    search: bool = False,
) -> None:
    data = OfflineData(db_url)
    loader, input_dim = build_loader(data, seq_len, batch_size, regime)
    if loader is None:
        return
    dataset = loader.dataset
    n = len(dataset)
    split = int(n * 0.8)
    train_ds, val_ds = torch.utils.data.random_split(dataset, [split, n - split])
    train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_ds, batch_size=batch_size)

    lr = 1e-3
    hidden = 64
    if search:
        lr, hidden = hyper_search(train_loader, val_loader, input_dim, device)

    model = models.DeepTransformerModel(input_dim, hidden_dim=hidden)
    model.to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = torch.nn.MSELoss()
    last_eval = 0.0

    while True:
        loader, _ = build_loader(data, seq_len, batch_size, regime)
        if loader is not None:
            model.train()
            for X_b, y_b in loader:
                X_b = X_b.to(device)
                y_b = y_b.to(device)
                opt.zero_grad()
                pred = model(X_b)
                loss = loss_fn(pred, y_b)
                loss.backward()
                opt.step()
            if eval_interval is not None:
                val_loss = evaluate(model, val_loader, device)
                logging.info("val_loss=%.4f", val_loss)
            models.save_model(model.to("cpu"), str(model_path))
            model.to(device)
            os.environ["PRICE_MODEL_PATH"] = str(model_path)
            if log_progress:
                logging.info("Saved checkpoint to %s", model_path)
        if not daemon:
            break
        await asyncio.sleep(interval)


async def main() -> None:
    p = argparse.ArgumentParser(description="Continuous transformer training")
    p.add_argument("--db", default="sqlite:///offline_data.db")
    p.add_argument("--model", default="transformer_model.pt")
    p.add_argument("--interval", type=float, default=3600.0)
    p.add_argument("--device", default="auto")
    p.add_argument("--seq-len", type=int, default=30)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--regime", default=None)
    p.add_argument("--daemon", action="store_true", help="keep training in a loop")
    p.add_argument("--log-progress", action="store_true", help="log checkpoint saves")
    p.add_argument("--eval-interval", type=float, default=None, help="evaluate every N seconds")
    p.add_argument("--search", action="store_true", help="perform simple hyperparameter search")
    args = p.parse_args()

    dev = device.get_default_device(args.device)

    if args.log_progress:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    await train_loop(
        args.db,
        Path(args.model),
        dev,
        args.seq_len,
        args.batch_size,
        args.interval,
        args.regime,
        daemon=args.daemon,
        log_progress=args.log_progress,
        eval_interval=args.eval_interval,
        search=args.search,
    )


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
