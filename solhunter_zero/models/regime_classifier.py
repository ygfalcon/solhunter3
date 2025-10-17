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
        self.lstm = nn.LSTM(
            1,
            hidden_dim,
            num_layers,
            batch_first=True,
            dropout=0.1 if num_layers > 1 else 0.0,
        )
        self.fc = nn.Linear(hidden_dim, len(LABELS))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1]
        return self.fc(out)

    def predict(self, prices: Sequence[float]) -> str:
        self.eval()
        with torch.no_grad():
            tail = list(prices[-self.seq_len:])
            if not tail:
                tail = [0.0] * self.seq_len
            elif len(tail) < self.seq_len:
                tail = [tail[0]] * (self.seq_len - len(tail)) + tail
            seq = torch.tensor(tail, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)
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
        # Project scalar input → model dim, then Transformer, then head
        self.model_dim = hidden_dim
        self.input_proj = nn.Linear(1, self.model_dim)
        encoder_layer = nn.TransformerEncoderLayer(
            d_model=self.model_dim,
            nhead=max(1, min(4, self.model_dim)),
            dim_feedforward=max(hidden_dim * 2, 64),
            batch_first=True,
            dropout=0.1 if num_layers > 1 else 0.0,
            activation="gelu",
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers)
        self.fc = nn.Linear(self.model_dim, len(LABELS))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        t = self.input_proj(x)
        t = self.encoder(t)
        out = t[:, -1]  # CLS-by-convention (use last time step)
        return self.fc(out)

    def predict(self, prices: Sequence[float]) -> str:
        self.eval()
        with torch.no_grad():
            tail = list(prices[-self.seq_len:])
            if not tail:
                tail = [0.0] * self.seq_len
            elif len(tail) < self.seq_len:
                tail = [tail[0]] * (self.seq_len - len(tail)) + tail
            seq = torch.tensor(tail, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)
            logits = self(seq)
            idx = int(torch.argmax(logits, dim=-1).item())
            return LABELS[idx]


def _to_returns(arr: torch.Tensor, *, log: bool = True) -> torch.Tensor:
    if arr.numel() < 2:
        return torch.zeros_like(arr)
    prev = arr[:-1]
    curr = arr[1:]
    if log:
        r = torch.log(curr.clamp_min(1e-12)) - torch.log(prev.clamp_min(1e-12))
    else:
        r = (curr - prev) / prev.clamp_min(1e-12)
    return torch.nn.functional.pad(r, (1, 0))


def make_training_data(
    prices: Sequence[float],
    seq_len: int = 30,
    threshold: float = 0.02,
    *,
    use_returns: bool = True,
) -> Tuple[torch.Tensor, torch.Tensor]:
    arr = torch.tensor(prices, dtype=torch.float32)
    if use_returns:
        arr = _to_returns(arr, log=True)
    n = len(arr) - seq_len - 1
    if n <= 0:
        raise ValueError("not enough history for seq_len")
    seqs = []
    labels = []
    for i in range(n):
        seq = arr[i:i + seq_len].unsqueeze(-1)
        p0 = arr[i + seq_len - 1]
        p1 = arr[i + seq_len]
        change = p1 - p0 if use_returns else (p1 - p0) / (p0 + 1e-12)
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
    batch_size: int = 64,
    grad_clip: float = 1.0,
    use_returns: bool = True,
) -> nn.Module:
    X, y = make_training_data(prices, seq_len, threshold, use_returns=use_returns)
    if model_type == "transformer":
        model = RegimeTransformer(
            seq_len, hidden_dim=hidden_dim, num_layers=num_layers
        )
    else:
        model = RegimeLSTM(
            seq_len, hidden_dim=hidden_dim, num_layers=num_layers
        )
    device = torch.device(
        "cuda"
        if torch.cuda.is_available()
        else (
            "mps"
            if hasattr(torch.backends, "mps") and torch.backends.mps.is_available()
            else "cpu"
        )
    )
    model.to(device)
    counts = torch.bincount(y, minlength=len(LABELS)).float()
    weights = counts.sum() / (counts + 1e-6)
    weights = weights / weights.mean()
    loss_fn = nn.CrossEntropyLoss(weight=weights.to(device))
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    idx = torch.randperm(X.size(0))
    X, y = X[idx], y[idx]
    for _ in range(max(1, epochs)):
        for start in range(0, X.size(0), batch_size):
            xb = X[start:start + batch_size].to(device)
            yb = y[start:start + batch_size].to(device)
            opt.zero_grad(set_to_none=True)
            logits = model(xb)
            loss = loss_fn(logits, yb)
            loss.backward()
            if grad_clip and grad_clip > 0:
                nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
            opt.step()
    model.to("cpu")
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
    cfg_no_cls = {k: v for k, v in cfg.items() if k != "cls"}
    if cls == "RegimeTransformer":
        model = RegimeTransformer(**cfg_no_cls)
    else:
        model = RegimeLSTM(**cfg_no_cls)
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
