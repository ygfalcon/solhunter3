from __future__ import annotations

import os
from typing import Sequence, Tuple, Optional

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
        # running normalization stats for the input feature (returns)
        self.register_buffer("_feat_mean", torch.zeros(1))
        self.register_buffer("_feat_std", torch.ones(1))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # normalize last dim if shape matches (..., 1)
        if x.size(-1) == 1:
            x = (x - self._feat_mean) / self._feat_std.clamp_min(1e-8)
        out, _ = self.lstm(x)
        out = out[:, -1]
        # VaR is non-negative (magnitude of potential loss)
        return torch.relu(self.fc(out)).squeeze(-1)

    def predict(self, prices: Sequence[float]) -> float:
        self.eval()
        with torch.no_grad():
            seq = _prices_to_returns(prices)[-self.seq_len:]
            if len(seq) < self.seq_len:
                # pad with zeros if extremely short (edge-case)
                seq = ([0.0] * (self.seq_len - len(seq))) + list(seq)
            arr = torch.tensor(seq, dtype=torch.float32).unsqueeze(0).unsqueeze(-1)
            pred = self(arr)  # normalization applied in forward
            return float(pred.item())

    def set_normalizer(self, mean: torch.Tensor, std: torch.Tensor) -> None:
        with torch.no_grad():
            self._feat_mean.copy_(mean.view(1).to(self._feat_mean.device))
            self._feat_std.copy_(std.view(1).to(self._feat_std.device))


def _prices_to_returns(prices: Sequence[float]) -> np.ndarray:
    """Compute log-returns from price series with guards."""
    p = np.asarray(list(prices), dtype=np.float64)
    p = p[np.isfinite(p)]
    if p.size < 2:
        return np.asarray([], dtype=np.float32)
    # prevent invalid logs
    p = np.clip(p, 1e-12, np.inf)
    r = np.diff(np.log(p))
    return r.astype(np.float32, copy=False)


def make_var_dataset(
    prices: Sequence[float],
    seq_len: int = 30,
    confidence: float = 0.95,
) -> Tuple[torch.Tensor, torch.Tensor]:
    # Model input is returns; target remains VaR of *price* changes next step
    rets = _prices_to_returns(prices)
    if rets.size == 0:
        raise ValueError("not enough history for seq_len")
    # Rebuild an aligned price series from returns for VaR target windows
    arr = torch.tensor(prices, dtype=torch.float32)
    n = len(arr) - seq_len - 1
    if n <= 0:
        raise ValueError("not enough history for seq_len")

    seqs = []
    targets = []
    for i in range(n):
        # feed returns to the model
        r = _prices_to_returns(arr[i : i + seq_len + 1].tolist())  # length seq_len
        if r.size < seq_len:
            continue
        seq = torch.from_numpy(r).float().unsqueeze(-1)
        # target VaR from the following horizon (same horizon as inputs)
        target_prices = arr[i + 1 : i + seq_len + 2].tolist()
        var = value_at_risk(target_prices, confidence)
        seqs.append(seq)
        # ensure non-negative numeric target
        targets.append(float(max(var, 0.0)))
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
    # feature normalization (per-column; here just returns with dim=1)
    mean = X.mean(dim=(0, 1))
    std = X.std(dim=(0, 1)).clamp_min(1e-8)

    device = _default_device()
    model = VaRForecaster(seq_len, hidden_dim=hidden_dim, num_layers=num_layers).to(device)
    model.set_normalizer(mean, std)

    X = X.to(device)
    y = y.to(device)

    # split for validation
    n = X.size(0)
    idx = torch.randperm(n, device=device)
    n_val = max(1, int(0.1 * n)) if n > 10 else 0
    val_idx = idx[:n_val] if n_val > 0 else None
    tr_idx = idx[n_val:] if n_val > 0 else idx
    Xtr, ytr = X[tr_idx], y[tr_idx]
    Xval, yval = (X[val_idx], y[val_idx]) if val_idx is not None else (None, None)

    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    scaler = torch.cuda.amp.GradScaler(enabled=(device.type == "cuda"))

    best = float("inf")
    best_state: Optional[dict[str, torch.Tensor]] = None
    patience = 5
    bad = 0
    batch_size = 256
    grad_clip = 1.0

    for _ in range(max(1, int(epochs))):
        model.train()
        perm = torch.randperm(Xtr.size(0), device=device)
        for start in range(0, Xtr.size(0), batch_size):
            sl = perm[start : start + batch_size]
            xb, yb = Xtr[sl], ytr[sl]
            opt.zero_grad(set_to_none=True)
            if scaler.is_enabled():
                with torch.cuda.amp.autocast():
                    pred = model(xb)
                    loss = loss_fn(pred, yb)
                scaler.scale(loss).backward()
                scaler.unscale_(opt)
                if grad_clip > 0:
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                scaler.step(opt)
                scaler.update()
            else:
                pred = model(xb)
                loss = loss_fn(pred, yb)
                loss.backward()
                if grad_clip > 0:
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                opt.step()

        if Xval is not None:
            model.eval()
            with torch.no_grad():
                v = float(loss_fn(model(Xval), yval).item())
            if v + 1e-12 < best:
                best = v
                best_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}
                bad = 0
            else:
                bad += 1
                if bad >= patience:
                    break

    if best_state is not None:
        model.load_state_dict(best_state)
    model.eval()
    return model


def save_var_model(model: VaRForecaster, path: str) -> None:
    cfg = {
        "seq_len": model.seq_len,
        "hidden_dim": model.lstm.hidden_size,
        "num_layers": model.lstm.num_layers,
    }
    torch.save(
        {
            "cfg": cfg,
            "state": model.state_dict(),
            # persist normalizer explicitly for safety
            "norm": {
                "mean": getattr(model, "_feat_mean", torch.zeros(1)).detach().cpu(),
                "std": getattr(model, "_feat_std", torch.ones(1)).detach().cpu(),
            },
        },
        path,
    )


def load_var_model(path: str) -> VaRForecaster:
    obj = torch.load(path, map_location="cpu")
    cfg = obj.get("cfg", {})
    model = VaRForecaster(**cfg)
    model.load_state_dict(obj["state"])
    try:
        n = obj.get("norm", {})
        if isinstance(n, dict) and "mean" in n and "std" in n:
            model.set_normalizer(n["mean"], n["std"])
    except Exception:
        pass
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


def _default_device() -> "torch.device":
    if hasattr(torch, "cuda") and torch.cuda.is_available():
        return torch.device("cuda")
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")

