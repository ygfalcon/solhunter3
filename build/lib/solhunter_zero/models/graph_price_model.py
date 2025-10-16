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
    nodes_list = []
    edge_list = []
    batch_vec = []
    targets = []
    offset = 0
    for i, (nodes, edges, bvec, y) in enumerate(batch):
        nodes_list.append(nodes)
        if edges.numel() > 0:
            edge_list.append(edges + offset)
        batch_vec.append(bvec + i)
        targets.append(y)
        offset += nodes.size(0)
    x = torch.cat(nodes_list, dim=0)
    if edge_list:
        edge_index = torch.cat(edge_list, dim=1)
    else:
        edge_index = torch.empty(2, 0, dtype=torch.long)
    batch_tensor = torch.cat(batch_vec, dim=0)
    y = torch.cat(targets)
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
        min_len = min(len(lst) for lst in groups.values())
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
        self.embed = nn.Linear(input_dim, hidden_dim)
        if HAS_PYG:
            self.conv1 = SAGEConv(hidden_dim, hidden_dim)
            self.conv2 = SAGEConv(hidden_dim, hidden_dim)
        else:
            self.fc1 = nn.Linear(hidden_dim, hidden_dim)
        self.out = nn.Linear(hidden_dim, 1)

    def forward(self, nodes: torch.Tensor, edge_index: torch.Tensor, batch: torch.Tensor) -> torch.Tensor:
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
) -> GraphPriceModel:
    """Train :class:`GraphPriceModel` using ``MarketSnapshot`` rows."""

    dataset = GraphPriceDataset(snaps)
    loader = DataLoader(
        dataset, batch_size=min(32, len(dataset)), shuffle=True, collate_fn=_collate
    )
    model = GraphPriceModel(dataset.nodes[0].size(1), hidden_dim=hidden_dim)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    for _ in range(max(1, epochs)):
        for nodes, edges, batch, y in loader:
            opt.zero_grad()
            pred = model(nodes, edges, batch)
            loss = loss_fn(pred, y)
            loss.backward()
            opt.step()
    model.eval()
    return model


def save_graph_model(model: GraphPriceModel, path: str) -> None:
    cfg = {
        "cls": "GraphPriceModel",
        "input_dim": model.embed.in_features,
        "hidden_dim": model.embed.out_features,
    }
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
