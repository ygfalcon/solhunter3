try:
    import torch
    from torch import nn
    from torch.utils.data import Dataset, DataLoader
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
                    "torch is required for graph_price_model"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for graph_price_model"
            )

    class _DatasetStub:
        def __init__(self, *a, **k) -> None:
            raise ImportError(
                "torch is required for graph_price_model"
            )

    torch = nn = _TorchStub()  # type: ignore
    Dataset = DataLoader = _DatasetStub  # type: ignore
from collections import defaultdict
from typing import Sequence, Any

try:  # optional dependency
    from torch_geometric.nn import SAGEConv
    HAS_PYG = True
except Exception:  # pragma: no cover - optional
    HAS_PYG = False


def _collate(batch: Sequence[tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]]):
    nodes_list: list[torch.Tensor] = []
    edge_list: list[torch.Tensor] = []
    batch_vec: list[torch.Tensor] = []
    targets: list[torch.Tensor] = []
    offset = 0
    for i, (nodes, edges, bvec, y) in enumerate(batch):
        nodes_list.append(nodes)
        if edges.numel() > 0:
            edge_list.append(edges + offset)
        batch_vec.append(bvec + i)
        targets.append(y)
        offset += nodes.size(0)
    x = torch.cat(nodes_list, dim=0) if nodes_list else torch.empty(0, 0)
    if edge_list:
        edge_index = torch.cat(edge_list, dim=1)
    else:
        edge_index = torch.empty(2, 0, dtype=torch.long)
    batch_tensor = torch.cat(batch_vec, dim=0) if batch_vec else torch.empty(0, dtype=torch.long)
    y = torch.cat(targets) if targets else torch.empty(0)
    return x, edge_index, batch_tensor, y


class GraphPriceDataset(Dataset):
    """Dataset of token graphs built from MarketSnapshot rows."""

    def __init__(self, snaps: Sequence[Any]) -> None:
        groups: dict[str, list] = defaultdict(list)
        for s in snaps:
            groups[s.token].append(s)
        self.tokens = sorted(groups)
        for lst in groups.values():
            lst.sort(key=lambda s: s.timestamp)
        if not groups:
            raise ValueError("no snapshots provided")
        min_len = min(len(lst) for lst in groups.values())
        if min_len < 2:
            raise ValueError("need at least two snapshots per token")
        self.nodes: list[torch.Tensor] = []
        self.targets: list[torch.Tensor] = []
        for i in range(min_len - 1):
            feats = []
            rois = []
            for tok in self.tokens:
                s0 = groups[tok][i]
                s1 = groups[tok][i + 1]
                feats.append([
                    float(s0.price),
                    float(s0.depth),
                    float(getattr(s0, "total_depth", 0.0)),
                    float(getattr(s0, "slippage", 0.0)),
                    float(getattr(s0, "volume", 0.0)),
                    float(getattr(s0, "imbalance", 0.0)),
                    float(getattr(s0, "tx_rate", 0.0)),
                    float(getattr(s0, "whale_share", getattr(s0, "whale_activity", 0.0))),
                    float(getattr(s0, "spread", 0.0)),
                    float(getattr(s0, "sentiment", 0.0)),
                ])
                rois.append((float(s1.price) - float(s0.price)) / float(s0.price))
            self.nodes.append(torch.tensor(feats, dtype=torch.float32))
            self.targets.append(torch.tensor(rois, dtype=torch.float32))
        self.edge_index = self._full_edges(len(self.tokens))
        # Precompute normalization (feature-wise across all nodes/steps)
        mat = torch.cat(self.nodes, dim=0)  # (steps*n_tokens, feat)
        self.feat_mean = mat.mean(dim=0)
        self.feat_std = mat.std(dim=0).clamp_min(1e-6)

    @staticmethod
    def _full_edges(n: int) -> torch.Tensor:
        if n <= 1:
            return torch.empty(2, 0, dtype=torch.long)
        src = []
        dst = []
        for i in range(n):
            for j in range(n):
                if i != j:
                    src.append(i)
                    dst.append(j)
        return torch.tensor([src, dst], dtype=torch.long)

    def __len__(self) -> int:
        return len(self.nodes)

    def __getitem__(self, idx: int):
        x = self.nodes[idx]
        y = self.targets[idx]
        batch = torch.zeros(x.size(0), dtype=torch.long)
        return x, self.edge_index, batch, y


class GraphPriceModel(nn.Module):
    """Simple graph convolution model forecasting price movement."""

    def __init__(self, input_dim: int, hidden_dim: int = 32) -> None:
        super().__init__()
        self.input_dim = input_dim
        self.hidden_dim = hidden_dim
        self.embed = nn.Linear(input_dim, hidden_dim)
        if HAS_PYG:
            self.conv1 = SAGEConv(hidden_dim, hidden_dim)
            self.conv2 = SAGEConv(hidden_dim, hidden_dim)
        else:
            self.fc1 = nn.Linear(hidden_dim, hidden_dim)
        self.out = nn.Linear(hidden_dim, 1)
        # normalization buffers (populated by trainer/loader)
        self.register_buffer("feat_mean", torch.zeros(input_dim))
        self.register_buffer("feat_std", torch.ones(input_dim))

    def forward(self, nodes: torch.Tensor, edge_index: torch.Tensor, batch: torch.Tensor) -> torch.Tensor:
        # normalize if stats available
        if self.feat_mean.numel() == nodes.size(-1) and self.feat_std.numel() == nodes.size(-1):
            nodes = (nodes - self.feat_mean) / self.feat_std
        x = torch.relu(self.embed(nodes))
        if HAS_PYG:
            x = torch.relu(self.conv1(x, edge_index))
            x = torch.relu(self.conv2(x, edge_index))
        else:
            x = torch.relu(self.fc1(x))
        return self.out(x).squeeze(-1)


def train_graph_model(
    snaps: Sequence[Any],
    *,
    epochs: int = 20,
    lr: float = 1e-3,
    hidden_dim: int = 32,
    batch_size: int = 32,
    grad_clip: float | None = 1.0,
) -> GraphPriceModel:
    """Train :class:`GraphPriceModel` using ``MarketSnapshot`` rows."""

    dataset = GraphPriceDataset(snaps)
    loader = DataLoader(
        dataset,
        batch_size=min(batch_size, len(dataset)) if len(dataset) > 0 else 1,
        shuffle=True,
        collate_fn=_collate,
    )
    model = GraphPriceModel(dataset.nodes[0].size(1), hidden_dim=hidden_dim)
    # attach normalization learned from dataset
    model.feat_mean = dataset.feat_mean.clone()
    model.feat_std = dataset.feat_std.clone()
    device = torch.device("cpu")
    try:
        if torch.cuda.is_available():
            device = torch.device("cuda")
        else:
            backends = getattr(torch, "backends", None)
            if backends is not None and hasattr(backends, "mps") and backends.mps.is_available():
                device = torch.device("mps")
    except Exception:
        device = torch.device("cpu")
    model.to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    for _ in range(max(1, epochs)):
        for nodes, edges, batch, y in loader:
            nodes = nodes.to(device)
            edges = edges.to(device)
            batch = batch.to(device)
            y = y.to(device)
            opt.zero_grad(set_to_none=True)
            pred = model(nodes, edges, batch)
            loss = loss_fn(pred, y)
            loss.backward()
            if grad_clip and grad_clip > 0:
                nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
            opt.step()
    model.eval()
    return model


def save_graph_model(model: GraphPriceModel, path: str) -> None:
    cfg = {
        "cls": "GraphPriceModel",
        "input_dim": model.input_dim if hasattr(model, "input_dim") else model.embed.in_features,
        "hidden_dim": model.hidden_dim if hasattr(model, "hidden_dim") else model.embed.out_features,
    }
    # state_dict contains feat_mean/std buffers, so normalization persists
    torch.save({"cfg": cfg, "state": model.state_dict()}, path)


def load_graph_model(path: str) -> GraphPriceModel:
    obj = torch.load(path, map_location="cpu")
    if isinstance(obj, GraphPriceModel):
        obj.eval()
        return obj
    if isinstance(obj, dict) and "state" in obj:
        cfg = obj.get("cfg", {})
        model = GraphPriceModel(cfg.get("input_dim", 10), hidden_dim=cfg.get("hidden_dim", 32))
        model.load_state_dict(obj["state"])
        model.eval()
        return model
    raise TypeError("Invalid model file")


# -------- Convenience inference for live snapshots ----------
def predict_rois(model: GraphPriceModel, latest_snaps: Sequence[Any]) -> torch.Tensor:
    """
    Predict per-token next-period ROI from a single synchronized snapshot set.
    Expects one latest MarketSnapshot per token, all for the same timestamp window.
    """
    if not latest_snaps:
        return torch.empty(0)
    # Build a single-step graph in the same token order as training isn’t guaranteed,
    # so we sort tokens alphabetically for determinism.
    feats = []
    tokens = sorted({s.token for s in latest_snaps})
    snap_by_tok = {s.token: s for s in latest_snaps}
    for tok in tokens:
        s0 = snap_by_tok[tok]
        feats.append([
            float(s0.price),
            float(s0.depth),
            float(getattr(s0, "total_depth", 0.0)),
            float(getattr(s0, "slippage", 0.0)),
            float(getattr(s0, "volume", 0.0)),
            float(getattr(s0, "imbalance", 0.0)),
            float(getattr(s0, "tx_rate", 0.0)),
            float(getattr(s0, "whale_share", getattr(s0, "whale_activity", 0.0))),
            float(getattr(s0, "spread", 0.0)),
            float(getattr(s0, "sentiment", 0.0)),
        ])
    nodes = torch.tensor(feats, dtype=torch.float32)
    n = nodes.size(0)
    if n > 1:
        # fully-connected (no self)
        src, dst = zip(*[(i, j) for i in range(n) for j in range(n) if i != j])
        edge_index = torch.tensor([src, dst], dtype=torch.long)
    else:
        edge_index = torch.empty(2, 0, dtype=torch.long)
    batch = torch.zeros(n, dtype=torch.long)
    device = next(model.parameters()).device
    with torch.no_grad():
        return model(nodes.to(device), edge_index.to(device), batch.to(device)).cpu()
