from __future__ import annotations

from typing import Sequence, Iterable, Tuple, Any

import os
try:
    import torch
    import torch.nn as nn
except ImportError as exc:  # pragma: no cover - optional dependency
    class _TorchStub:
        class Tensor:
            pass

        class device:
            def __init__(self, *a, **k) -> None:
                pass

        class Module:
            def __init__(self, *a, **k) -> None:
                raise ImportError(
                    "torch is required for onchain_forecaster"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for onchain_forecaster"
            )

    torch = nn = _TorchStub()  # type: ignore


class LSTMForecaster(nn.Module):
    """LSTM network forecasting mempool transaction rate."""

    def __init__(self, input_dim: int = 4, hidden_dim: int = 32, num_layers: int = 2, seq_len: int = 30) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.seq_len = seq_len
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1]
        return self.fc(out).squeeze(-1)

    def predict(self, seq: Sequence[Sequence[float]]) -> float:
        self.eval()
        with torch.no_grad():
            t = torch.tensor(seq, dtype=torch.float32).unsqueeze(0)
            pred = self(t)
            return float(pred.item())


class TransformerForecaster(nn.Module):
    """Transformer encoder forecasting mempool transaction rate."""

    def __init__(self, input_dim: int = 4, hidden_dim: int = 32, num_layers: int = 2, seq_len: int = 30) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.seq_len = seq_len
        nhead = max(1, min(4, input_dim))
        if input_dim % nhead != 0:
            nhead = 1
        layer = nn.TransformerEncoderLayer(input_dim, nhead=nhead, dim_feedforward=hidden_dim)
        self.encoder = nn.TransformerEncoder(layer, num_layers)
        self.fc = nn.Linear(input_dim, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        t = self.encoder(x)
        out = t[:, -1]
        return self.fc(out).squeeze(-1)

    def predict(self, seq: Sequence[Sequence[float]]) -> float:
        self.eval()
        with torch.no_grad():
            t = torch.tensor(seq, dtype=torch.float32).unsqueeze(0)
            pred = self(t)
            return float(pred.item())


def make_dataset(snaps: Iterable[Any], seq_len: int = 30) -> Tuple[torch.Tensor, torch.Tensor]:
    """Construct training tensors from offline snapshots."""

    rows = sorted(snaps, key=lambda s: s.timestamp)
    feats: list[list[float]] = []
    for prev, curr in zip(rows[:-1], rows[1:]):
        depth_change = float(curr.depth - prev.depth)
        tx_rate = float(curr.tx_rate)
        whale = float(getattr(curr, "whale_share", getattr(curr, "whale_activity", 0.0)))
        avg_swap = float(curr.volume) / tx_rate if tx_rate else 0.0
        feats.append([depth_change, tx_rate, whale, avg_swap])

    n = len(feats) - seq_len
    if n <= 0:
        raise ValueError("not enough data for seq_len")

    seqs = []
    targets = []
    for i in range(n):
        seqs.append(torch.tensor(feats[i : i + seq_len], dtype=torch.float32))
        targets.append(feats[i + seq_len][1])

    X = torch.stack(seqs)
    y = torch.tensor(targets, dtype=torch.float32)
    return X, y


def train_lstm(
    snaps: Iterable[Any],
    *,
    seq_len: int = 30,
    epochs: int = 20,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
) -> LSTMForecaster:
    """Train :class:`LSTMForecaster` on snapshots."""

    X, y = make_dataset(snaps, seq_len)
    model = LSTMForecaster(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers, seq_len=seq_len)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    for _ in range(epochs):
        opt.zero_grad()
        pred = model(X)
        loss = loss_fn(pred, y)
        loss.backward()
        opt.step()
    model.eval()
    return model


def train_transformer(
    snaps: Iterable[Any],
    *,
    seq_len: int = 30,
    epochs: int = 20,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
) -> TransformerForecaster:
    """Train :class:`TransformerForecaster` on snapshots."""

    X, y = make_dataset(snaps, seq_len)
    model = TransformerForecaster(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers, seq_len=seq_len)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    for _ in range(epochs):
        opt.zero_grad()
        pred = model(X)
        loss = loss_fn(pred, y)
        loss.backward()
        opt.step()
    model.eval()
    return model


def save_model(model: nn.Module, path: str) -> None:
    cfg = {
        "cls": type(model).__name__,
        "input_dim": getattr(model, "input_dim", 4),
        "hidden_dim": getattr(model, "hidden_dim", 32),
        "num_layers": getattr(model, "num_layers", 2),
        "seq_len": getattr(model, "seq_len", 30),
    }
    torch.save({"cfg": cfg, "state": model.state_dict()}, path)


def load_model(path: str) -> nn.Module:
    obj = torch.load(path, map_location="cpu")
    if isinstance(obj, (LSTMForecaster, TransformerForecaster)):
        obj.eval()
        return obj
    if isinstance(obj, dict) and "state" in obj:
        cfg = obj.get("cfg", {})
        cls_name = cfg.pop("cls", "LSTMForecaster")
        if cls_name == "TransformerForecaster":
            cls = TransformerForecaster
        else:
            cls = LSTMForecaster
        model = cls(**cfg)
        model.load_state_dict(obj["state"])
        model.eval()
        return model
    raise TypeError("Invalid model file")


_MODEL_CACHE: dict[str, tuple[float, nn.Module]] = {}


def get_model(path: str | None, *, reload: bool = False) -> nn.Module | None:
    if not path or not os.path.exists(path):
        return None
    if reload and path in _MODEL_CACHE:
        _MODEL_CACHE.pop(path, None)
    mtime = os.path.getmtime(path)
    entry = _MODEL_CACHE.get(path)
    if entry and entry[0] == mtime:
        return entry[1]
    model = load_model(path)
    _MODEL_CACHE[path] = (mtime, model)
    return model

