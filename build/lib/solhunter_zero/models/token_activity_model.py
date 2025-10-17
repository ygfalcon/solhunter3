from __future__ import annotations

from typing import Sequence, Iterable, Tuple, Any, Optional

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
                    "torch is required for token_activity_model"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for token_activity_model"
            )

    torch = nn = _TorchStub()  # type: ignore


class ActivityModel(nn.Module):
    """Simple feed-forward network for token activity scoring."""

    def __init__(self, input_dim: int = 4, hidden_dim: int = 16) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, 1)
        # running feature normalization (mean/std) stored with the model
        self.register_buffer("_feat_mean", torch.zeros(input_dim))
        self.register_buffer("_feat_std", torch.ones(input_dim))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # normalize if the last dim matches input_dim
        if x.dim() >= 2 and x.size(-1) == self.input_dim:
            # avoid div-by-zero with clamp
            x = (x - self._feat_mean) / self._feat_std.clamp_min(1e-8)
        x = torch.relu(self.fc1(x))
        return self.fc2(x).squeeze(-1)

    def predict(self, features: Sequence[float]) -> float:
        self.eval()
        with torch.no_grad():
            t = torch.tensor(features, dtype=torch.float32).unsqueeze(0)
            pred = self(t)
            return float(pred.item())

    def predict_many(self, features: Iterable[Sequence[float]]) -> list[float]:
        """Vectorized prediction for a batch of feature rows."""
        self.eval()
        with torch.no_grad():
            t = torch.tensor(list(features), dtype=torch.float32)
            y = self(t)
            return [float(v) for v in y.detach().cpu().flatten()]

    def set_normalizer(self, mean: torch.Tensor, std: torch.Tensor) -> None:
        """Attach feature normalizer statistics to the model."""
        with torch.no_grad():
            self._feat_mean.copy_(mean.to(self._feat_mean.device))
            self._feat_std.copy_(std.to(self._feat_std.device))


def make_training_data(
    snaps: Iterable[Any],
    trades: Iterable[Any],
) -> Tuple[torch.Tensor, torch.Tensor]:
    """Construct dataset from offline snapshots and trades."""

    snaps_by_token: dict[str, list[Any]] = {}
    for s in snaps:
        snaps_by_token.setdefault(s.token, []).append(s)

    trades_by_token: dict[str, list[Any]] = {}
    for t in trades:
        trades_by_token.setdefault(t.token, []).append(t)

    feats = []
    targets = []

    for tok, s_list in snaps_by_token.items():
        s_list.sort(key=lambda s: s.timestamp)
        t_list = sorted(trades_by_token.get(tok, []), key=lambda t: t.timestamp)
        for prev, curr in zip(s_list[:-1], s_list[1:]):
            trade_sizes = [
                float(tr.amount)
                for tr in t_list
                if prev.timestamp <= tr.timestamp < curr.timestamp
            ]
            avg_size = sum(trade_sizes) / len(trade_sizes) if trade_sizes else 0.0
            depth_change = float(curr.depth - prev.depth)
            tx_rate = float(curr.tx_rate)
            whale = float(getattr(curr, "whale_share", getattr(curr, "whale_activity", 0.0)))
            feats.append([depth_change, tx_rate, whale, avg_size])
            prev_price = float(prev.price)
            # guard against zero/invalid previous price
            targets.append(((float(curr.price) - prev_price) / prev_price) if prev_price > 0 else 0.0)

    if not feats:
        raise ValueError("not enough data")

    X = torch.tensor(feats, dtype=torch.float32)
    y = torch.tensor(targets, dtype=torch.float32)
    return X, y


def _default_device() -> "torch.device":
    try:
        if hasattr(torch, "cuda") and torch.cuda.is_available():
            return torch.device("cuda")
    except (AttributeError, ImportError):
        pass
    try:
        if hasattr(torch, "backends") and torch.backends.mps.is_available():
            return torch.device("mps")
    except (AttributeError, ImportError):
        pass
    return torch.device("cpu")


def train_activity_model(
    snaps: Iterable[Any],
    trades: Iterable[Any],
    *,
    epochs: int = 20,
    lr: float = 1e-3,
    batch_size: int = 256,
    grad_clip: float = 1.0,
    val_split: float = 0.1,
    early_stopping: bool = True,
    patience: int = 5,
    use_amp: bool = False,
    seed: Optional[int] = 1337,
    device: Optional["torch.device"] = None,
    loss: str = "mse",
) -> ActivityModel:
    """Train :class:`ActivityModel` on offline data."""

    if seed is not None:
        try:
            torch.manual_seed(seed)
        except Exception:
            pass

    X, y = make_training_data(snaps, trades)

    mean = X.mean(dim=0)
    std = X.std(dim=0).clamp_min(1e-8)

    device = device or _default_device()
    model = ActivityModel(X.size(-1)).to(device)
    model.set_normalizer(mean, std)

    X = X.to(device)
    y = y.to(device)

    n = X.size(0)
    idx = torch.randperm(n, device=device)
    n_val = int(n * max(0.0, min(0.5, val_split)))
    val_idx = idx[:n_val] if n_val > 0 else None
    tr_idx = idx[n_val:] if n_val > 0 else idx
    Xtr, ytr = X[tr_idx], y[tr_idx]
    Xval, yval = (X[val_idx], y[val_idx]) if val_idx is not None else (None, None)

    opt = torch.optim.Adam(model.parameters(), lr=lr)
    if loss == "huber":
        loss_fn = nn.SmoothL1Loss(beta=0.1)
    else:
        loss_fn = nn.MSELoss()
    try:
        scaler = torch.cuda.amp.GradScaler(enabled=use_amp and device.type == "cuda")
    except (AttributeError, ImportError):  # pragma: no cover - optional CUDA amp
        class _NoOpScaler:
            def is_enabled(self) -> bool:
                return False

            def scale(self, loss: torch.Tensor) -> torch.Tensor:
                return loss

            def step(self, optimizer: torch.optim.Optimizer) -> None:
                optimizer.step()

            def update(self) -> None:
                pass

            def unscale_(self, optimizer: torch.optim.Optimizer) -> None:
                pass

        scaler = _NoOpScaler()

    best_val = float("inf")
    best_state = None
    stagnant = 0

    for _ in range(max(1, int(epochs))):
        model.train()
        perm = torch.randperm(Xtr.size(0), device=device)
        for start in range(0, Xtr.size(0), max(1, int(batch_size))):
            sl = perm[start : start + int(batch_size)]
            xb, yb = Xtr[sl], ytr[sl]
            opt.zero_grad(set_to_none=True)
            if scaler.is_enabled():
                with torch.cuda.amp.autocast():
                    pred = model(xb)
                    l = loss_fn(pred, yb)
                scaler.scale(l).backward()
                if grad_clip and grad_clip > 0:
                    scaler.unscale_(opt)
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                scaler.step(opt)
                scaler.update()
            else:
                pred = model(xb)
                l = loss_fn(pred, yb)
                l.backward()
                if grad_clip and grad_clip > 0:
                    nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
                opt.step()

        if Xval is not None and early_stopping:
            model.eval()
            with torch.no_grad():
                val_pred = model(Xval)
                v = float(loss_fn(val_pred, yval).item())
            if v + 1e-12 < best_val:
                best_val = v
                best_state = {k: v.detach().cpu() for k, v in model.state_dict().items()}
                stagnant = 0
            else:
                stagnant += 1
                if stagnant >= max(1, int(patience)):
                    break

    if best_state is not None:
        model.load_state_dict(best_state)

    model.eval()
    return model

