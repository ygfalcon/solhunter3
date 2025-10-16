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
                    "torch is required for regime_classifier"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for regime_classifier"
            )

    torch = nn = _TorchStub()  # type: ignore

LABELS = ["bear", "sideways", "bull"]
LABEL_TO_IDX = {lbl: i for i, lbl in enumerate(LABELS)}


class RegimeLSTM(nn.Module):
    """Simple LSTM based regime classifier."""

    def __init__(
        self, seq_len: int = 30, hidden_dim: int = 32, num_layers: int = 2
    ) -> None:
        super().__init__()
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.lstm = nn.LSTM(1, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, len(LABELS))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1]
        return self.fc(out)

    def predict(self, prices: Sequence[float]) -> str:
        self.eval()
        with torch.no_grad():
            seq = (
                torch.tensor(prices[-self.seq_len:], dtype=torch.float32)
                .unsqueeze(0)
                .unsqueeze(-1)
            )
            logits = self(seq)
            idx = int(torch.argmax(logits, dim=-1).item())
            return LABELS[idx]


class RegimeTransformer(nn.Module):
    """Transformer encoder based regime classifier."""

    def __init__(
        self, seq_len: int = 30, hidden_dim: int = 32, num_layers: int = 2
    ) -> None:
        super().__init__()
        self.seq_len = seq_len
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        encoder_layer = nn.TransformerEncoderLayer(
            1, nhead=1, dim_feedforward=hidden_dim
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers)
        self.fc = nn.Linear(1, len(LABELS))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        t = self.encoder(x)
        out = t[:, -1]
        return self.fc(out)

    def predict(self, prices: Sequence[float]) -> str:
        self.eval()
        with torch.no_grad():
            seq = (
                torch.tensor(prices[-self.seq_len:], dtype=torch.float32)
                .unsqueeze(0)
                .unsqueeze(-1)
            )
            logits = self(seq)
            idx = int(torch.argmax(logits, dim=-1).item())
            return LABELS[idx]


def make_training_data(
    prices: Sequence[float], seq_len: int = 30, threshold: float = 0.02
) -> Tuple[torch.Tensor, torch.Tensor]:
    arr = torch.tensor(prices, dtype=torch.float32)
    n = len(arr) - seq_len - 1
    if n <= 0:
        raise ValueError("not enough history for seq_len")
    seqs = []
    labels = []
    for i in range(n):
        seq = arr[i:i + seq_len].unsqueeze(-1)
        p0 = arr[i + seq_len - 1]
        p1 = arr[i + seq_len]
        change = (p1 - p0) / p0
        if change > threshold:
            lbl = LABEL_TO_IDX["bull"]
        elif change < -threshold:
            lbl = LABEL_TO_IDX["bear"]
        else:
            lbl = LABEL_TO_IDX["sideways"]
        seqs.append(seq)
        labels.append(lbl)
    X = torch.stack(seqs)
    y = torch.tensor(labels, dtype=torch.long)
    return X, y


def train_regime_classifier(
    prices: Sequence[float],
    *,
    seq_len: int = 30,
    epochs: int = 20,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
    threshold: float = 0.02,
    model_type: str = "lstm",
) -> nn.Module:
    X, y = make_training_data(prices, seq_len, threshold)
    if model_type == "transformer":
        model = RegimeTransformer(
            seq_len, hidden_dim=hidden_dim, num_layers=num_layers
        )
    else:
        model = RegimeLSTM(
            seq_len, hidden_dim=hidden_dim, num_layers=num_layers
        )
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.CrossEntropyLoss()
    for _ in range(max(1, epochs)):
        opt.zero_grad()
        logits = model(X)
        loss = loss_fn(logits, y)
        loss.backward()
        opt.step()
    model.eval()
    return model


def save_regime_model(model: nn.Module, path: str) -> None:
    if isinstance(model, RegimeTransformer):
        cfg = {
            "cls": "RegimeTransformer",
            "seq_len": model.seq_len,
            "hidden_dim": model.hidden_dim,
            "num_layers": model.num_layers,
        }
    else:
        cfg = {
            "cls": "RegimeLSTM",
            "seq_len": model.seq_len,
            "hidden_dim": model.hidden_dim,
            "num_layers": model.num_layers,
        }
    torch.save({"cfg": cfg, "state": model.state_dict()}, path)


def load_regime_model(path: str) -> nn.Module:
    obj = torch.load(path, map_location="cpu")
    cfg = obj.get("cfg", {})
    cls = cfg.get("cls", "RegimeLSTM")
    if cls == "RegimeTransformer":
        model = RegimeTransformer(**cfg)
    else:
        model = RegimeLSTM(**cfg)
    model.load_state_dict(obj["state"])
    model.eval()
    return model


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
    model = load_regime_model(path)
    _MODEL_CACHE[path] = (mtime, model)
    return model
