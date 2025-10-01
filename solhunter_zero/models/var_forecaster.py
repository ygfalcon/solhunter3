from __future__ import annotations

import os
from typing import Sequence, Tuple

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
                    "torch is required for var_forecaster"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for var_forecaster"
            )

    torch = nn = _TorchStub()  # type: ignore
import numpy as np

from ..risk import value_at_risk


class VaRForecaster(nn.Module):
    """LSTM network forecasting next-period VaR."""

    def __init__(self, seq_len: int = 30, hidden_dim: int = 32, num_layers: int = 2) -> None:
        super().__init__()
        self.seq_len = seq_len
        self.lstm = nn.LSTM(1, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1]
        return torch.relu(self.fc(out)).squeeze(-1)

    def predict(self, prices: Sequence[float]) -> float:
        self.eval()
        with torch.no_grad():
            arr = torch.tensor(prices[-self.seq_len:], dtype=torch.float32).unsqueeze(0).unsqueeze(-1)
            pred = self(arr)
            return float(pred.item())


def make_var_dataset(prices: Sequence[float], seq_len: int = 30, confidence: float = 0.95) -> Tuple[torch.Tensor, torch.Tensor]:
    arr = torch.tensor(prices, dtype=torch.float32)
    n = len(arr) - seq_len - 1
    if n <= 0:
        raise ValueError("not enough history for seq_len")

    seqs = []
    targets = []
    for i in range(n):
        seq = arr[i : i + seq_len].unsqueeze(-1)
        target_prices = arr[i + 1 : i + seq_len + 2].tolist()
        var = value_at_risk(target_prices, confidence)
        seqs.append(seq)
        targets.append(var)
    X = torch.stack(seqs)
    y = torch.tensor(targets, dtype=torch.float32)
    return X, y


def train_var_forecaster(
    prices: Sequence[float],
    *,
    seq_len: int = 30,
    epochs: int = 20,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
    confidence: float = 0.95,
) -> VaRForecaster:
    X, y = make_var_dataset(prices, seq_len, confidence)
    model = VaRForecaster(seq_len, hidden_dim=hidden_dim, num_layers=num_layers)
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


def save_var_model(model: VaRForecaster, path: str) -> None:
    cfg = {
        "seq_len": model.seq_len,
        "hidden_dim": model.lstm.hidden_size,
        "num_layers": model.lstm.num_layers,
    }
    torch.save({"cfg": cfg, "state": model.state_dict()}, path)


def load_var_model(path: str) -> VaRForecaster:
    obj = torch.load(path, map_location="cpu")
    cfg = obj.get("cfg", {})
    model = VaRForecaster(**cfg)
    model.load_state_dict(obj["state"])
    model.eval()
    return model


_MODEL_CACHE: dict[str, tuple[float, VaRForecaster]] = {}


def get_model(path: str | None, *, reload: bool = False) -> VaRForecaster | None:
    if not path or not os.path.exists(path):
        return None
    if reload and path in _MODEL_CACHE:
        _MODEL_CACHE.pop(path, None)
    mtime = os.path.getmtime(path)
    entry = _MODEL_CACHE.get(path)
    if entry and entry[0] == mtime:
        return entry[1]
    model = load_var_model(path)
    _MODEL_CACHE[path] = (mtime, model)
    return model

