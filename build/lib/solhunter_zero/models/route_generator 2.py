from __future__ import annotations

import os
from typing import Sequence, Dict, List

try:
    import torch
    from torch import nn
    from torch.utils.data import Dataset, DataLoader
    from torch.nn.utils.rnn import pad_sequence
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
                    "torch is required for route_generator"
                )

        def __getattr__(self, name):
            raise ImportError(
                "torch is required for route_generator"
            )

    class _DatasetStub:
        def __init__(self, *a, **k) -> None:
            raise ImportError(
                "torch is required for route_generator"
            )

    torch = nn = _TorchStub()  # type: ignore
    Dataset = DataLoader = _DatasetStub  # type: ignore

    def pad_sequence(*a, **k):  # type: ignore
        raise ImportError(
            "torch is required for route_generator"
        )


class RouteDataset(Dataset):
    """Dataset of profitable arbitrage routes."""

    def __init__(self, routes: Sequence[Sequence[str]], venue_map: Dict[str, int] | None = None) -> None:
        uniq = sorted({v for r in routes for v in r})
        if venue_map is None:
            self.venue_map = {v: i + 2 for i, v in enumerate(uniq)}
        else:
            self.venue_map = {v: i + 2 for v, i in venue_map.items()}
        self.start_idx = 0
        self.end_idx = 1
        self.routes = [[self.venue_map[v] for v in r] for r in routes]

    def __len__(self) -> int:
        return len(self.routes)

    def __getitem__(self, idx: int):
        seq = self.routes[idx]
        x = torch.tensor([self.start_idx] + seq, dtype=torch.long)
        y = torch.tensor(seq + [self.end_idx], dtype=torch.long)
        return x, y

    def collate(self, batch):
        xs, ys = zip(*batch)
        x_pad = pad_sequence(xs, batch_first=True, padding_value=self.end_idx)
        y_pad = pad_sequence(ys, batch_first=True, padding_value=self.end_idx)
        return x_pad, y_pad


class RouteGenerator(nn.Module):
    """Simple LSTM route generator."""

    def __init__(self, vocab_size: int, embed_dim: int = 16, hidden_dim: int = 32, num_layers: int = 1) -> None:
        super().__init__()
        self.embed = nn.Embedding(vocab_size, embed_dim)
        self.lstm = nn.LSTM(embed_dim, hidden_dim, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_dim, vocab_size)
        self.start_idx = 0
        self.end_idx = 1
        self.venue_map: Dict[str, int] = {}

    def forward(self, x: torch.Tensor, state=None):
        x = self.embed(x)
        out, state = self.lstm(x, state)
        out = self.fc(out)
        return out, state

    def generate(self, num_routes: int = 5, max_hops: int = 4) -> List[List[str]]:
        inv = {i: v for v, i in self.venue_map.items()}
        routes: List[List[str]] = []
        device = next(self.parameters()).device
        for _ in range(num_routes):
            seq_idx: List[int] = []
            inp = torch.tensor([[self.start_idx]], dtype=torch.long, device=device)
            state = None
            for _ in range(max_hops):
                logits, state = self.forward(inp, state)
                probs = torch.softmax(logits[0, -1], dim=-1)
                idx = int(torch.argmax(probs).item())
                if idx == self.end_idx:
                    break
                seq_idx.append(idx)
                inp = torch.tensor([[idx]], dtype=torch.long, device=device)
            if seq_idx:
                routes.append([inv.get(i, str(i)) for i in seq_idx])
        return routes


def train_route_generator(
    routes: Sequence[Sequence[str]],
    *,
    epochs: int = 20,
    lr: float = 1e-3,
    embed_dim: int = 16,
    hidden_dim: int = 32,
    num_layers: int = 1,
) -> RouteGenerator:
    dataset = RouteDataset(routes)
    loader = DataLoader(dataset, batch_size=min(32, len(dataset)), shuffle=True, collate_fn=dataset.collate)
    model = RouteGenerator(len(dataset.venue_map) + 2, embed_dim, hidden_dim, num_layers)
    model.venue_map = dataset.venue_map
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.CrossEntropyLoss(ignore_index=dataset.end_idx)
    for _ in range(max(1, epochs)):
        for x, y in loader:
            opt.zero_grad()
            out, _ = model(x)
            loss = loss_fn(out.view(-1, out.size(-1)), y.view(-1))
            loss.backward()
            opt.step()
    model.eval()
    return model


def save_route_generator(model: RouteGenerator, path: str) -> None:
    torch.save({"state": model.state_dict(), "venue_map": model.venue_map}, path)


def load_route_generator(path: str | None) -> RouteGenerator | None:
    if not path or not os.path.exists(path):
        return None
    obj = torch.load(path, map_location="cpu")
    venue_map = obj.get("venue_map", {})
    model = RouteGenerator(len(venue_map) + 2)
    model.load_state_dict(obj["state"])
    model.venue_map = venue_map
    model.eval()
    return model
