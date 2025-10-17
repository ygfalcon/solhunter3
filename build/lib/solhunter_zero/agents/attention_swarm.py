from __future__ import annotations

from typing import Sequence, Dict, Any

from ..optional_imports import try_import
import numpy as np

class _TorchStub:
    def __getattr__(self, name: str) -> Any:  # pragma: no cover - simple
        raise ImportError("torch is required for AttentionSwarm")


_torch = try_import("torch", stub=_TorchStub())
if isinstance(_torch, _TorchStub):  # pragma: no cover - optional dependency
    torch = None

    class _NNStub:
        def __getattr__(self, name: str) -> Any:  # pragma: no cover - simple
            raise ImportError("torch is required for AttentionSwarm")

    nn = _NNStub()  # type: ignore
    ModuleBase = object
else:  # pragma: no cover - optional dependency
    torch = _torch  # type: ignore[assignment]
    import torch.nn as nn  # type: ignore
    ModuleBase = nn.Module  # type: ignore[attr-defined]

from ..regime import detect_regime
from ..advanced_memory import AdvancedMemory
from ..device import get_default_device


def _require_torch() -> None:
    if torch is None:  # pragma: no cover - optional dependency
        raise ImportError("torch is required for AttentionSwarm")


class AttentionSwarm(ModuleBase):
    """Tiny transformer predicting agent weights from ROI history."""

    def __init__(
        self,
        num_agents: int,
        seq_len: int = 5,
        hidden_dim: int = 32,
        num_layers: int = 2,
        *,
        device: str | torch.device | None = None,
    ) -> None:
        _require_torch()
        super().__init__()
        self.num_agents = int(num_agents)
        self.seq_len = int(seq_len)
        self.hidden_dim = int(hidden_dim)
        self.num_layers = int(num_layers)

        self.device = get_default_device(device)

        input_dim = self.num_agents + 2
        candidates = [h for h in (4, 3, 2, 1) if input_dim % h == 0]
        nhead = candidates[0] if candidates else 1
        layer = nn.TransformerEncoderLayer(
            d_model=input_dim,
            nhead=nhead,
            dim_feedforward=self.hidden_dim,
            batch_first=True,
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers)
        self.fc = nn.Linear(input_dim, num_agents)
        self.softmax = nn.Softmax(dim=-1)
        self.to(self.device)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        if x.device != self.device:
            x = x.to(self.device)
        enc = self.encoder(x)
        out = enc[:, -1, :]
        return self.softmax(self.fc(out))

    def predict(self, seq: Sequence[Sequence[float]]) -> Sequence[float]:
        self.eval()
        with torch.no_grad():
            t = torch.tensor(seq, dtype=torch.float32, device=self.device).unsqueeze(0)
            w = self.forward(t)[0].detach().cpu()
            return [float(v) for v in w]


def _window_rois(trades: Sequence[Any], agents: Sequence[str]) -> Dict[str, float]:
    summary = {a: {"buy": 0.0, "sell": 0.0} for a in agents}
    for t in trades:
        name = getattr(t, "reason", None) or ""
        if name not in summary:
            continue
        side = getattr(t, "direction", "")
        amt = float(getattr(t, "amount", 0.0)) * float(getattr(t, "price", 0.0))
        if side == "buy":
            summary[name]["buy"] += amt
        elif side == "sell":
            summary[name]["sell"] += amt
    rois = {}
    for a in agents:
        spent = summary[a]["buy"]
        revenue = summary[a]["sell"]
        rois[a] = (revenue - spent) / spent if spent > 0 else 0.0
    return rois


def make_training_data(
    memory: AdvancedMemory,
    agents: Sequence[str],
    *,
    window: int = 50,
    seq_len: int = 5,
) -> tuple[torch.Tensor, torch.Tensor]:
    """Construct dataset tensors from ``memory`` trades."""

    _require_torch()
    trades = memory.list_trades()
    trades.sort(key=lambda t: t.timestamp)
    if not trades:
        raise ValueError("no trades in memory")
    if len(trades) < window + seq_len:
        raise ValueError(
            f"not enough trades for window={window} and seq_len={seq_len}"
        )

    windows = [trades[i:i + window] for i in range(0, len(trades) - window, window)]
    feats: list[list[float]] = []
    for w in windows:
        rois = _window_rois(w, agents)
        prices = [float(t.price) for t in w]
        regime = detect_regime(prices)
        reg_val = 1.0 if regime == "bull" else -1.0 if regime == "bear" else 0.0
        vol = float(np.std(prices) / (np.mean(prices) or 1.0)) if len(prices) > 1 else 0.0
        row = [rois[a] for a in agents] + [reg_val, vol]
        feats.append(row)

    X_seq = []
    y = []
    for i in range(len(feats) - seq_len):
        X_seq.append(feats[i:i + seq_len])
        target = feats[i + seq_len][: len(agents)]
        t = np.array(target, dtype=np.float32)
        max_t = float(np.max(t))
        exp_t = np.exp(t - max_t)
        soft = exp_t / np.sum(exp_t)
        y.append(soft.tolist())

    if not X_seq or not y:
        raise ValueError(
            "not enough sequences after windowing; collect more trades or adjust parameters"
        )

    X = torch.tensor(X_seq, dtype=torch.float32)
    y_t = torch.tensor(y, dtype=torch.float32)
    return X, y_t


def train_attention_swarm(
    memory: AdvancedMemory,
    agents: Sequence[str],
    *,
    window: int = 50,
    seq_len: int = 5,
    epochs: int = 10,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
    device: str | None = None,
) -> AttentionSwarm:
    """Fit an :class:`AttentionSwarm` from ``memory`` trades."""
    _require_torch()
    dev = get_default_device(device)

    X, y = make_training_data(memory, agents, window=window, seq_len=seq_len)
    X = X.to(dev)
    y = y.to(dev)
    model = AttentionSwarm(
        len(agents), seq_len=seq_len, hidden_dim=hidden_dim, num_layers=num_layers, device=dev
    )
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    for _ in range(int(epochs)):
        opt.zero_grad()
        pred = model(X)
        loss = loss_fn(pred, y)
        loss.backward()
        opt.step()
    model.eval()
    return model


def save_model(model: AttentionSwarm, path: str) -> None:
    cfg = {
        "num_agents": model.num_agents,
        "seq_len": model.seq_len,
        "hidden_dim": model.hidden_dim,
        "num_layers": model.num_layers,
    }
    _require_torch()
    torch.save({"cfg": cfg, "state": model.state_dict()}, path)


def load_model(path: str, *, device: str | None = None) -> AttentionSwarm:
    _require_torch()
    dev = get_default_device(device)
    obj = torch.load(path, map_location=dev)
    cfg = obj.get("cfg", {})
    model = AttentionSwarm(**cfg, device=dev)
    model.load_state_dict(obj["state"])
    model.eval()
    return model

