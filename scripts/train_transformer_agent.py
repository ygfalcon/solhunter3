import argparse
import torch
from torch.utils.data import DataLoader, TensorDataset
import pytorch_lightning as pl
import numpy as np
from solhunter_zero.offline_data import OfflineData
from solhunter_zero import models


class LightningTransformer(pl.LightningModule):
    def __init__(self, input_dim: int, hidden_dim: int, num_layers: int, lr: float) -> None:
        super().__init__()
        layer = torch.nn.TransformerEncoderLayer(input_dim, nhead=8, dim_feedforward=hidden_dim)
        self.encoder = torch.nn.TransformerEncoder(layer, num_layers)
        self.fc = torch.nn.Linear(input_dim, 1)
        self.lr = lr
        self.loss_fn = torch.nn.MSELoss()

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        t = self.encoder(x)
        out = t[:, -1]
        return self.fc(out).squeeze(-1)

    def training_step(self, batch, batch_idx):
        x, y = batch
        pred = self(x)
        loss = self.loss_fn(pred, y)
        self.log("loss", loss)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.lr)


def main() -> None:
    p = argparse.ArgumentParser(description="Train transformer model from offline data")
    p.add_argument("--db", default="sqlite:///offline_data.db", help="Offline data database URL")
    p.add_argument("--out", required=True, help="Path to save trained model")
    p.add_argument("--epochs", type=int, default=10)
    p.add_argument("--seq-len", type=int, default=30)
    p.add_argument("--batch-size", type=int, default=64)
    p.add_argument("--lr", type=float, default=1e-3)
    p.add_argument("--hidden-dim", type=int, default=64)
    p.add_argument("--layers", type=int, default=4, dest="layers")
    args = p.parse_args()

    data = OfflineData(args.db)
    snaps = data.list_snapshots()
    if len(snaps) <= args.seq_len:
        raise SystemExit("not enough snapshots to train")

    X, y = models.make_snapshot_training_data(snaps, seq_len=args.seq_len)
    dataset = TensorDataset(X, y)
    loader = DataLoader(dataset, batch_size=args.batch_size, shuffle=True)

    lt_model = LightningTransformer(X.size(-1), args.hidden_dim, args.layers, args.lr)
    trainer = pl.Trainer(max_epochs=args.epochs, accelerator="auto", enable_progress_bar=False)
    trainer.fit(lt_model, loader)

    model = models.DeepTransformerModel(X.size(-1), hidden_dim=args.hidden_dim, num_layers=args.layers)
    model.load_state_dict(lt_model.state_dict())
    models.save_model(model, args.out)
    print(f"Model saved to {args.out}")


if __name__ == "__main__":  # pragma: no cover
    main()
