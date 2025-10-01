from __future__ import annotations

import os
from typing import Iterable, Sequence, Tuple, Any
from pathlib import Path

import solhunter_zero.device as device_module

try:
    import torch
    import torch.nn as nn
except ImportError as exc:  # pragma: no cover - optional dependency
    class _TorchStub:
        class Tensor:  # pragma: no cover - typing placeholder
            pass

        class device:  # pragma: no cover - typing placeholder
            def __init__(self, *a, **k) -> None:
                pass

        class Module:
            def __init__(self, *a, **k) -> None:
                raise ImportError(
                    "torch is required for model operations"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for model operations"
            )

    torch = nn = _TorchStub()  # type: ignore

from .token_activity_model import ActivityModel
from .graph_price_model import GraphPriceModel, load_graph_model
from .gnn import (
    RouteGNN,
    GATRouteGNN,
    train_route_gnn,
    save_route_gnn,
    load_route_gnn,
    rank_routes,
)
from .route_generator import (
    RouteGenerator,
    train_route_generator,
    save_route_generator,
    load_route_generator,
)


class PriceModel(nn.Module):
    """Simple LSTM based predictor."""

    def __init__(self, input_dim: int, hidden_dim: int = 32, num_layers: int = 2) -> None:
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1]
        return self.fc(out).squeeze(-1)

    def predict(self, seq: Sequence[Sequence[float]]) -> float:
        """Return model prediction for a single sequence."""
        self.eval()
        with torch.no_grad():
            t = torch.tensor(seq, dtype=torch.float32).unsqueeze(0)
            pred = self(t)
            return float(pred.item())


class TransformerModel(nn.Module):
    """Simple transformer encoder based predictor."""

    def __init__(self, input_dim: int, hidden_dim: int = 32, num_layers: int = 2) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        nhead = max(1, min(4, input_dim))
        if input_dim % nhead != 0:
            nhead = 1
        encoder_layer = nn.TransformerEncoderLayer(
            input_dim, nhead=nhead, dim_feedforward=hidden_dim
        )
        self.encoder = nn.TransformerEncoder(encoder_layer, num_layers)
        self.fc = nn.Linear(input_dim, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        # x: (batch, seq, features)
        t = self.encoder(x)
        out = t[:, -1]
        return self.fc(out).squeeze(-1)

    def predict(self, seq: Sequence[Sequence[float]]) -> float:
        self.eval()
        with torch.no_grad():
            t = torch.tensor(seq, dtype=torch.float32).unsqueeze(0)
            pred = self(t)
            return float(pred.item())


class DeepLSTMModel(nn.Module):
    """Deeper LSTM network for price prediction."""

    def __init__(self, input_dim: int, hidden_dim: int = 64, num_layers: int = 4) -> None:
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        out = out[:, -1]
        return self.fc(out).squeeze(-1)


class DeepTransformerModel(nn.Module):
    """Transformer with additional layers."""

    def __init__(self, input_dim: int, hidden_dim: int = 64, num_layers: int = 4) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        layer = nn.TransformerEncoderLayer(input_dim, nhead=8, dim_feedforward=hidden_dim)
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


class XLTransformerModel(nn.Module):
    """Very deep transformer encoder for complex patterns."""

    def __init__(self, input_dim: int, hidden_dim: int = 128, num_layers: int = 8) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        layer = nn.TransformerEncoderLayer(input_dim, nhead=8, dim_feedforward=hidden_dim)
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


def save_model(model: nn.Module, path: str) -> None:
    """Save ``model`` to ``path`` using a portable format."""
    if isinstance(model, PriceModel):
        cfg = {
            "cls": "PriceModel",
            "input_dim": model.lstm.input_size,
            "hidden_dim": model.lstm.hidden_size,
            "num_layers": model.lstm.num_layers,
        }
    elif isinstance(model, TransformerModel):
        cfg = {
            "cls": "TransformerModel",
            "input_dim": model.input_dim,
            "hidden_dim": model.hidden_dim,
            "num_layers": model.num_layers,
        }
    elif isinstance(model, DeepLSTMModel):
        cfg = {
            "cls": "DeepLSTMModel",
            "input_dim": model.lstm.input_size,
            "hidden_dim": model.lstm.hidden_size,
            "num_layers": model.lstm.num_layers,
        }
    elif isinstance(model, DeepTransformerModel):
        cfg = {
            "cls": "DeepTransformerModel",
            "input_dim": model.input_dim,
            "hidden_dim": model.hidden_dim,
            "num_layers": model.num_layers,
        }
    elif isinstance(model, XLTransformerModel):
        cfg = {
            "cls": "XLTransformerModel",
            "input_dim": model.input_dim,
            "hidden_dim": model.hidden_dim,
            "num_layers": model.num_layers,
        }
    elif isinstance(model, GraphPriceModel):
        cfg = {
            "cls": "GraphPriceModel",
            "input_dim": model.embed.in_features,
            "hidden_dim": model.embed.out_features,
        }
    elif isinstance(model, ActivityModel):
        cfg = {
            "cls": "ActivityModel",
            "input_dim": model.input_dim,
            "hidden_dim": model.hidden_dim,
        }
    else:
        cfg = {"cls": type(model).__name__}
    torch.save({"cfg": cfg, "state": model.state_dict()}, path)


def load_model(path: str) -> nn.Module:
    """Load a saved ML model from ``path``."""
    obj = torch.load(path, map_location="cpu")
    if isinstance(obj, (PriceModel, TransformerModel, DeepLSTMModel, DeepTransformerModel, XLTransformerModel, GraphPriceModel)):
        obj.eval()
        return obj
    if isinstance(obj, dict) and "state" in obj:
        cfg = obj.get("cfg", {})
        cls_name = cfg.pop("cls", "PriceModel")
        if cls_name == "PriceModel":
            model_cls = PriceModel
        elif cls_name == "TransformerModel":
            model_cls = TransformerModel
        elif cls_name == "DeepLSTMModel":
            model_cls = DeepLSTMModel
        elif cls_name == "DeepTransformerModel":
            model_cls = DeepTransformerModel
        elif cls_name == "XLTransformerModel":
            model_cls = XLTransformerModel
        elif cls_name == "GraphPriceModel":
            model_cls = GraphPriceModel
        elif cls_name == "ActivityModel":
            model_cls = ActivityModel
        else:
            model_cls = PriceModel
        model = model_cls(**cfg)
        model.load_state_dict(obj["state"])
        model.eval()
        return model
    raise TypeError("Invalid model file")


_MODEL_CACHE: dict[str, tuple[float, nn.Module]] = {}


def _cached_load(path: str) -> nn.Module:
    mtime = os.path.getmtime(path)
    entry = _MODEL_CACHE.get(path)
    if entry and entry[0] == mtime:
        return entry[1]
    model = load_model(path)
    _MODEL_CACHE[path] = (mtime, model)
    return model


def get_model(path: str | None, *, reload: bool = False) -> nn.Module | None:
    """Return model from path if it exists, reloading when ``reload`` is True."""
    if not path or not os.path.exists(path):
        return None
    if reload and path in _MODEL_CACHE:
        _MODEL_CACHE.pop(path, None)
    try:
        return _cached_load(path)
    except Exception:
        return None


def export_torchscript(model: nn.Module, path: str) -> str:
    """Compile ``model`` to TorchScript and save to ``path``."""
    out = os.fspath(path)
    try:
        scripted = torch.jit.script(model)
        scripted.save(out)
    except Exception as exc:  # pragma: no cover - optional feature
        raise RuntimeError(f"failed to export TorchScript: {exc}") from exc
    return out


class _ONNXModule(nn.Module):
    """Wrapper executing an ONNX session via :mod:`onnxruntime`."""

    def __init__(self, session: Any) -> None:  # pragma: no cover - simple
        super().__init__()
        self.session = session
        self.input_name = session.get_inputs()[0].name

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # pragma: no cover - simple
        arr = x.detach().cpu().numpy()
        out = self.session.run(None, {self.input_name: arr})[0]
        return torch.tensor(out, device=x.device)


def export_onnx(model: nn.Module, path: str, example_input: torch.Tensor) -> str:
    """Export ``model`` to ONNX format using ``example_input``."""
    out = os.fspath(path)
    try:
        torch.onnx.export(model, example_input, out, opset_version=12)
    except Exception as exc:  # pragma: no cover - optional feature
        raise RuntimeError(f"failed to export ONNX: {exc}") from exc
    return out


def load_compiled_model(
    path: str, device: str | torch.device | None = None
) -> nn.Module | None:
    """Load compiled TorchScript or ONNX model next to ``path`` if available."""

    dev = device_module.get_default_device(device)
    base = Path(path)
    script_path = base.with_suffix(".ptc")
    if script_path.exists():
        try:
            return torch.jit.load(os.fspath(script_path)).to(dev)
        except Exception:
            pass
    onnx_path = base.with_suffix(".onnx")
    if onnx_path.exists():
        try:
            import onnxruntime as ort  # type: ignore

            prov = ["CUDAExecutionProvider", "CPUExecutionProvider"] if dev.type == "cuda" else ["CPUExecutionProvider"]
            session = ort.InferenceSession(os.fspath(onnx_path), providers=prov)
            return _ONNXModule(session).to(dev)
        except Exception:
            return None
    return None


def make_training_data(
    prices: Iterable[float],
    liquidity: Iterable[float],
    depth: Iterable[float],
    total_depth: Iterable[float] | None = None,
    tx_counts: Iterable[float] | None = None,
    slippage: Iterable[float] | None = None,
    volume: Iterable[float] | None = None,
    mempool_rates: Iterable[float] | None = None,
    whale_shares: Iterable[float] | None = None,
    spreads: Iterable[float] | None = None,
    seq_len: int = 30,
) -> Tuple[torch.Tensor, torch.Tensor]:
    """Create model inputs from historical metrics."""

    p = torch.tensor(list(prices), dtype=torch.float32)
    l = torch.tensor(list(liquidity), dtype=torch.float32)
    d = torch.tensor(list(depth), dtype=torch.float32)
    td = torch.tensor(list(total_depth), dtype=torch.float32) if total_depth is not None else torch.zeros_like(p)
    s = torch.tensor(list(slippage), dtype=torch.float32) if slippage is not None else torch.zeros_like(p)
    v = torch.tensor(list(volume), dtype=torch.float32) if volume is not None else torch.zeros_like(p)
    if tx_counts is None:
        t = torch.zeros_like(p)
    else:
        t = torch.tensor(list(tx_counts), dtype=torch.float32)
    m = torch.tensor(list(mempool_rates), dtype=torch.float32) if mempool_rates is not None else torch.zeros_like(p)
    w = torch.tensor(list(whale_shares), dtype=torch.float32) if whale_shares is not None else torch.zeros_like(p)
    sp = torch.tensor(list(spreads), dtype=torch.float32) if spreads is not None else torch.zeros_like(p)

    n = len(p) - seq_len
    if n <= 0:
        raise ValueError("not enough history for seq_len")

    seqs = []
    targets = []
    for i in range(n):
        seq = torch.stack(
            [
                p[i : i + seq_len],
                l[i : i + seq_len],
                d[i : i + seq_len],
                td[i : i + seq_len],
                s[i : i + seq_len],
                v[i : i + seq_len],
                t[i : i + seq_len],
                m[i : i + seq_len],
                w[i : i + seq_len],
                sp[i : i + seq_len],
            ],
            dim=1,
        )
        seqs.append(seq)
        p0 = p[i + seq_len - 1]
        p1 = p[i + seq_len]
        targets.append((p1 - p0) / p0)

    X = torch.stack(seqs)
    y = torch.tensor(targets, dtype=torch.float32)
    return X, y


def train_price_model(
    prices: Iterable[float],
    liquidity: Iterable[float],
    depth: Iterable[float],
    total_depth: Iterable[float] | None = None,
    tx_counts: Iterable[float] | None = None,
    slippage: Iterable[float] | None = None,
    volume: Iterable[float] | None = None,
    mempool_rates: Iterable[float] | None = None,
    whale_shares: Iterable[float] | None = None,
    spreads: Iterable[float] | None = None,
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
) -> PriceModel:
    """Train a :class:`PriceModel` on historical data."""

    X, y = make_training_data(
        prices,
        liquidity,
        depth,
        total_depth,
        tx_counts,
        slippage,
        volume,
        mempool_rates,
        whale_shares,
        spreads,
        seq_len,
    )
    model = PriceModel(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers)
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


def train_transformer_model(
    prices: Iterable[float],
    liquidity: Iterable[float],
    depth: Iterable[float],
    total_depth: Iterable[float] | None = None,
    tx_counts: Iterable[float] | None = None,
    slippage: Iterable[float] | None = None,
    volume: Iterable[float] | None = None,
    mempool_rates: Iterable[float] | None = None,
    whale_shares: Iterable[float] | None = None,
    spreads: Iterable[float] | None = None,
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    num_layers: int = 2,
) -> TransformerModel:
    """Train a :class:`TransformerModel` on historical data."""

    X, y = make_training_data(
        prices,
        liquidity,
        depth,
        total_depth,
        tx_counts,
        slippage,
        volume,
        mempool_rates,
        whale_shares,
        spreads,
        seq_len,
    )
    model = TransformerModel(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers)
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


def train_deep_transformer_model(
    prices: Iterable[float],
    liquidity: Iterable[float],
    depth: Iterable[float],
    total_depth: Iterable[float] | None = None,
    tx_counts: Iterable[float] | None = None,
    slippage: Iterable[float] | None = None,
    volume: Iterable[float] | None = None,
    mempool_rates: Iterable[float] | None = None,
    whale_shares: Iterable[float] | None = None,
    spreads: Iterable[float] | None = None,
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
    hidden_dim: int = 64,
    num_layers: int = 4,
) -> DeepTransformerModel:
    """Train a :class:`DeepTransformerModel` on historical data."""

    X, y = make_training_data(
        prices,
        liquidity,
        depth,
        total_depth,
        tx_counts,
        slippage,
        volume,
        mempool_rates,
        whale_shares,
        spreads,
        seq_len,
    )
    model = DeepTransformerModel(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers)
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


def train_xl_transformer_model(
    prices: Iterable[float],
    liquidity: Iterable[float],
    depth: Iterable[float],
    total_depth: Iterable[float] | None = None,
    tx_counts: Iterable[float] | None = None,
    slippage: Iterable[float] | None = None,
    volume: Iterable[float] | None = None,
    mempool_rates: Iterable[float] | None = None,
    whale_shares: Iterable[float] | None = None,
    spreads: Iterable[float] | None = None,
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
    hidden_dim: int = 128,
    num_layers: int = 8,
) -> XLTransformerModel:
    """Train a :class:`XLTransformerModel` on historical data."""

    X, y = make_training_data(
        prices,
        liquidity,
        depth,
        total_depth,
        tx_counts,
        slippage,
        volume,
        mempool_rates,
        whale_shares,
        spreads,
        seq_len,
    )
    model = XLTransformerModel(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers)
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


def make_snapshot_training_data(snaps: Sequence[Any], seq_len: int = 30) -> Tuple[torch.Tensor, torch.Tensor]:
    """Construct dataset tensors from :class:`OfflineData` snapshots."""
    prices = torch.tensor([float(s.price) for s in snaps], dtype=torch.float32)
    depth = torch.tensor([float(s.depth) for s in snaps], dtype=torch.float32)
    agg_depth = torch.tensor([float(getattr(s, "total_depth", 0.0)) for s in snaps], dtype=torch.float32)
    slip = torch.tensor([float(getattr(s, "slippage", 0.0)) for s in snaps], dtype=torch.float32)
    vol = torch.tensor([float(getattr(s, "volume", 0.0)) for s in snaps], dtype=torch.float32)
    imb = torch.tensor([float(getattr(s, "imbalance", 0.0)) for s in snaps], dtype=torch.float32)
    rate = torch.tensor([float(getattr(s, "tx_rate", 0.0)) for s in snaps], dtype=torch.float32)
    whales = torch.tensor([float(getattr(s, "whale_share", getattr(s, "whale_activity", 0.0))) for s in snaps], dtype=torch.float32)
    spread = torch.tensor([float(getattr(s, "spread", 0.0)) for s in snaps], dtype=torch.float32)
    sent = torch.tensor([float(getattr(s, "sentiment", 0.0)) for s in snaps], dtype=torch.float32)

    n = len(prices) - seq_len
    if n <= 0:
        raise ValueError("not enough history for seq_len")

    seqs = []
    targets = []
    for i in range(n):
        seq = torch.stack([
            prices[i:i+seq_len],
            depth[i:i+seq_len],
            agg_depth[i:i+seq_len],
            slip[i:i+seq_len],
            vol[i:i+seq_len],
            imb[i:i+seq_len],
            rate[i:i+seq_len],
            whales[i:i+seq_len],
            spread[i:i+seq_len],
            sent[i:i+seq_len],
        ], dim=1)
        seqs.append(seq)
        p0 = prices[i + seq_len - 1]
        p1 = prices[i + seq_len]
        targets.append((p1 - p0) / p0)

    X = torch.stack(seqs)
    y = torch.tensor(targets, dtype=torch.float32)
    return X, y


def train_deep_lstm(
    snaps: Sequence[Any],
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
) -> DeepLSTMModel:
    """Train :class:`DeepLSTMModel` using stored snapshots."""
    X, y = make_snapshot_training_data(snaps, seq_len)
    model = DeepLSTMModel(X.size(-1))
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


def train_deep_transformer(
    snaps: Sequence[Any],
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
) -> DeepTransformerModel:
    """Train :class:`DeepTransformerModel` using stored snapshots."""
    X, y = make_snapshot_training_data(snaps, seq_len)
    model = DeepTransformerModel(X.size(-1))
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


def train_xl_transformer(
    snaps: Sequence[Any],
    *,
    seq_len: int = 30,
    epochs: int = 10,
    lr: float = 1e-3,
    hidden_dim: int = 128,
    num_layers: int = 8,
) -> XLTransformerModel:
    """Train :class:`XLTransformerModel` using stored snapshots."""
    X, y = make_snapshot_training_data(snaps, seq_len)
    model = XLTransformerModel(X.size(-1), hidden_dim=hidden_dim, num_layers=num_layers)
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
