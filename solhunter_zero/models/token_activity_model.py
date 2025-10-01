from __future__ import annotations

from typing import Sequence, Iterable, Tuple, Any

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

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = torch.relu(self.fc1(x))
        return self.fc2(x).squeeze(-1)

    def predict(self, features: Sequence[float]) -> float:
        self.eval()
        with torch.no_grad():
            t = torch.tensor(features, dtype=torch.float32).unsqueeze(0)
            pred = self(t)
            return float(pred.item())


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
            targets.append((float(curr.price) - float(prev.price)) / float(prev.price))

    if not feats:
        raise ValueError("not enough data")

    X = torch.tensor(feats, dtype=torch.float32)
    y = torch.tensor(targets, dtype=torch.float32)
    return X, y


def train_activity_model(
    snaps: Iterable[Any],
    trades: Iterable[Any],
    *,
    epochs: int = 20,
    lr: float = 1e-3,
) -> ActivityModel:
    """Train :class:`ActivityModel` on offline data."""

    X, y = make_training_data(snaps, trades)
    model = ActivityModel(X.size(-1))
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

