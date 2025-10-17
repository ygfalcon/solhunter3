import os
from typing import Sequence, Dict, Tuple

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
                raise ImportError("torch is required for gnn module")

        def __getattr__(self, name):
            raise ImportError("torch is required for gnn module")

    class _DatasetStub:
        def __init__(self, *a, **k) -> None:
            raise ImportError("torch is required for gnn module")

    torch = nn = _TorchStub()  # type: ignore
    Dataset = DataLoader = _DatasetStub  # type: ignore

try:
    from torch_geometric.nn import GCNConv, GATConv, global_mean_pool
    HAS_PYG = True
except Exception:  # pragma: no cover - optional dependency
    HAS_PYG = False


def _collate(batch: Sequence[Tuple[torch.Tensor, torch.Tensor, torch.Tensor, torch.Tensor]]):
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
    device = nodes_list[0].device if nodes_list else torch.device("cpu")
    x = torch.cat(nodes_list, dim=0) if nodes_list else torch.empty(0, dtype=torch.long, device=device)
    if edge_list:
        edge_index = torch.cat(edge_list, dim=1)
    else:
        edge_index = torch.empty(2, 0, dtype=torch.long, device=device)
    batch_tensor = torch.cat(batch_vec, dim=0) if batch_vec else torch.empty(0, dtype=torch.long, device=device)
    y = torch.stack(targets) if targets else torch.empty(0, dtype=torch.float32, device=device)
    return x, edge_index, batch_tensor, y


class RouteDataset(Dataset):
    """Dataset of arbitrage routes and profits."""

    def __init__(self, routes: Sequence[Sequence[str]], profits: Sequence[float], venue_map: Dict[str, int] | None = None) -> None:
        # Reserve a stable UNK index at 0 so unseen venues at inference don’t crash
        UNK = "<UNK>"
        if venue_map is None:
            uniq = sorted({v for r in routes for v in r})
            self.venue_map = {UNK: 0, **{v: i + 1 for i, v in enumerate(uniq)}}
        else:
            self.venue_map = dict(venue_map)
            if UNK not in self.venue_map:
                # keep existing ids stable; just ensure UNK exists
                self.venue_map[UNK] = 0
        self._unk = self.venue_map[UNK]
        # Map unknowns to UNK (safe for inference with new venues)
        self.routes = [[self.venue_map.get(v, self._unk) for v in r] for r in routes]
        self.profits = torch.tensor(list(profits), dtype=torch.float32)

    def __len__(self) -> int:
        return len(self.routes)

    def __getitem__(self, idx: int):
        nodes = torch.tensor(self.routes[idx], dtype=torch.long)
        if len(nodes) > 1:
            edges = torch.stack([
                torch.arange(len(nodes) - 1, dtype=torch.long),
                torch.arange(1, len(nodes), dtype=torch.long),
            ])
        else:
            edges = torch.empty(2, 0, dtype=torch.long)
        batch = torch.zeros(len(nodes), dtype=torch.long)
        y = self.profits[idx]
        return nodes, edges, batch, y


class RouteGNN(nn.Module):
    """Simple graph network for ranking arbitrage routes."""

    def __init__(self, num_venues: int, embed_dim: int = 8) -> None:
        super().__init__()
        self.embed = nn.Embedding(num_venues, embed_dim)
        if HAS_PYG:
            self.conv1 = GCNConv(embed_dim, embed_dim)
            self.conv2 = GCNConv(embed_dim, embed_dim)
        else:
            self.fc1 = nn.Linear(embed_dim, embed_dim)
        self.out = nn.Linear(embed_dim, 1)
        self.venue_map: Dict[str, int] = {}
        self.embed_dim = embed_dim

    def forward(self, nodes: torch.Tensor, edge_index: torch.Tensor, batch: torch.Tensor) -> torch.Tensor:
        x = self.embed(nodes)
        if HAS_PYG:
            x = torch.relu(self.conv1(x, edge_index))
            x = torch.relu(self.conv2(x, edge_index))
            x = global_mean_pool(x, batch)
        else:
            bsz = int(batch.max().item() + 1) if batch.numel() > 0 else 1
            agg = []
            for i in range(bsz):
                mask = batch == i
                if mask.any():
                    agg.append(x[mask].mean(0))
                else:
                    agg.append(torch.zeros(x.size(-1), device=x.device))
            x = torch.stack(agg, dim=0)
            x = torch.relu(self.fc1(x))
        out = self.out(x).squeeze(-1)
        return out


class GATRouteGNN(nn.Module):
    """Graph attention network for ranking arbitrage routes."""

    def __init__(self, num_venues: int, embed_dim: int = 8, heads: int = 2) -> None:
        super().__init__()
        self.embed = nn.Embedding(num_venues, embed_dim)
        self.heads = heads
        if HAS_PYG:
            self.conv1 = GATConv(embed_dim, embed_dim, heads=heads, concat=False)
            self.conv2 = GATConv(embed_dim, embed_dim, heads=heads, concat=False)
        else:
            self.fc1 = nn.Linear(embed_dim, embed_dim)
        self.out = nn.Linear(embed_dim, 1)
        self.venue_map: Dict[str, int] = {}
        self.embed_dim = embed_dim

    def forward(self, nodes: torch.Tensor, edge_index: torch.Tensor, batch: torch.Tensor) -> torch.Tensor:
        x = self.embed(nodes)
        if HAS_PYG:
            x = torch.relu(self.conv1(x, edge_index))
            x = torch.relu(self.conv2(x, edge_index))
            x = global_mean_pool(x, batch)
        else:
            bsz = int(batch.max().item() + 1) if batch.numel() > 0 else 1
            agg = []
            for i in range(bsz):
                mask = batch == i
                if mask.any():
                    agg.append(x[mask].mean(0))
                else:
                    agg.append(torch.zeros(x.size(-1), device=x.device))
            x = torch.stack(agg, dim=0)
            x = torch.relu(self.fc1(x))
        out = self.out(x).squeeze(-1)
        return out


def train_route_gnn(
    routes: Sequence[Sequence[str]],
    profits: Sequence[float],
    *,
    epochs: int = 20,
    lr: float = 1e-3,
    embed_dim: int = 8,
    gat: bool = False,
    heads: int = 2,
    batch_size: int = 32,
    grad_clip: float | None = 1.0,
) -> RouteGNN:
    """Train :class:`RouteGNN` on historical routes."""

    dataset = RouteDataset(routes, profits)
    loader = DataLoader(
        dataset,
        batch_size=min(batch_size, len(dataset)) if len(dataset) > 0 else 1,
        shuffle=True,
        collate_fn=_collate,
    )
    model_cls = GATRouteGNN if gat else RouteGNN
    model = model_cls(len(dataset.venue_map), embed_dim=embed_dim, **({"heads": heads} if gat else {}))
    model.venue_map = dataset.venue_map
    device = torch.device(
        "cuda" if torch.cuda.is_available()
        else ("mps" if hasattr(torch.backends, "mps") and torch.backends.mps.is_available() else "cpu")
    )
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


def save_route_gnn(model: RouteGNN, path: str) -> None:
    torch.save(
        {
            "state": model.state_dict(),
            "venue_map": model.venue_map,
            "gat": isinstance(model, GATRouteGNN),
            "embed_dim": getattr(model, "embed_dim", 8),
            "heads": getattr(model, "heads", 2),
        },
        path,
    )


def load_route_gnn(path: str | None) -> RouteGNN | None:
    if not path or not os.path.exists(path):
        return None
    obj = torch.load(path, map_location="cpu")
    venue_map = obj.get("venue_map", {})
    embed_dim = int(obj.get("embed_dim", 8))
    heads = int(obj.get("heads", 2))
    if obj.get("gat"):
        model = GATRouteGNN(len(venue_map), embed_dim=embed_dim, heads=heads)
    else:
        model = RouteGNN(len(venue_map), embed_dim=embed_dim)
    model.load_state_dict(obj["state"])
    model.venue_map = venue_map
    model.eval()
    return model


def rank_routes(model: RouteGNN | GATRouteGNN, routes: Sequence[Sequence[str]]) -> int:
    """Return index of the route with highest predicted profit."""
    dataset = RouteDataset(routes, [0.0] * len(routes), venue_map=model.venue_map)
    nodes, edges, batch, _ = _collate([dataset[i] for i in range(len(dataset))])
    with torch.no_grad():
        device = next(model.parameters()).device
        pred = model(nodes.to(device), edges.to(device), batch.to(device))
    return int(torch.argmax(pred).item())
