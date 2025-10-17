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

    PAD_IDX = 0
    SOS_IDX = 1
    EOS_IDX = 2

    def __init__(self, routes: Sequence[Sequence[str]], venue_map: Dict[str, int] | None = None) -> None:
        uniq = sorted({v for r in routes for v in r})
        if venue_map is None:
            # Reserve 0,1,2 for PAD,SOS,EOS
            self.venue_map = {v: i + 3 for i, v in enumerate(uniq)}
        else:
            # Respect provided mapping but ensure indices start at 3
            offset = 0 if all(i >= 3 for i in venue_map.values()) else 3
            v_sorted = sorted(venue_map.keys())
            self.venue_map = {v: (venue_map[v] if offset == 0 else (i + 3)) for i, v in enumerate(v_sorted)}
        self.routes = [[self.venue_map[v] for v in r] for r in routes]

    def __len__(self) -> int:
        return len(self.routes)

    def __getitem__(self, idx: int):
        seq = self.routes[idx]
        x = torch.tensor([self.SOS_IDX] + seq, dtype=torch.long)
        y = torch.tensor(seq + [self.EOS_IDX], dtype=torch.long)
        return x, y

    def collate(self, batch):
        xs, ys = zip(*batch)
        x_pad = pad_sequence(xs, batch_first=True, padding_value=self.PAD_IDX)
        y_pad = pad_sequence(ys, batch_first=True, padding_value=self.PAD_IDX)
        return x_pad, y_pad


class RouteGenerator(nn.Module):
    """Simple LSTM route generator."""

    def __init__(self, vocab_size: int, embed_dim: int = 16, hidden_dim: int = 32, num_layers: int = 1) -> None:
        super().__init__()
        self.PAD_IDX = 0
        self.SOS_IDX = 1
        self.EOS_IDX = 2
        self.embed = nn.Embedding(vocab_size, embed_dim, padding_idx=self.PAD_IDX)
        self.lstm = nn.LSTM(
            embed_dim,
            hidden_dim,
            num_layers,
            batch_first=True,
            dropout=0.1 if num_layers > 1 else 0.0,
        )
        self.fc = nn.Linear(hidden_dim, vocab_size)
        self.venue_map: Dict[str, int] = {}

    def forward(self, x: torch.Tensor, state=None):
        x = self.embed(x)
        out, state = self.lstm(x, state)
        out = self.fc(out)
        return out, state

    def _sample_logits(
        self,
        logits: torch.Tensor,
        *,
        temperature: float = 1.0,
        top_k: int = 0,
        top_p: float = 0.0,
    ) -> int:
        if temperature <= 0:
            temperature = 1.0
        l = logits / temperature
        if top_k > 0:
            kth = torch.topk(l, k=min(top_k, l.numel())).values[-1]
            l = torch.where(l < kth, torch.full_like(l, float("-inf")), l)
        if top_p > 0.0 and top_p < 1.0:
            probs = torch.softmax(l, dim=-1)
            sorted_probs, sorted_idx = torch.sort(probs, descending=True)
            cumsum = torch.cumsum(sorted_probs, dim=-1)
            mask = cumsum > top_p
            mask[..., 0] = False
            filtered = l.clone().fill_(float("-inf"))
            filtered[sorted_idx[~mask]] = l[sorted_idx[~mask]]
            l = filtered
        probs = torch.softmax(l, dim=-1)
        if torch.isnan(probs).any() or torch.isinf(probs).any() or probs.sum() <= 0:
            return int(torch.argmax(l, dim=-1).item())
        return int(torch.multinomial(probs, 1).item())

    def generate(
        self,
        num_routes: int = 5,
        max_hops: int = 4,
        *,
        temperature: float = 1.0,
        top_k: int = 5,
        top_p: float = 0.0,
        avoid_repeats: bool = True,
    ) -> List[List[str]]:
        inv = {i: v for v, i in self.venue_map.items()}
        routes: List[List[str]] = []
        device = next(self.parameters()).device
        for _ in range(num_routes):
            seq_idx: List[int] = []
            inp = torch.tensor([[self.SOS_IDX]], dtype=torch.long, device=device)
            state = None
            for _ in range(max_hops):
                logits, state = self.forward(inp, state)
                last = logits[0, -1].clone()
                last[self.PAD_IDX] = float("-inf")
                last[self.SOS_IDX] = float("-inf")
                if not seq_idx:
                    last[self.EOS_IDX] = float("-inf")
                idx = self._sample_logits(last, temperature=temperature, top_k=top_k, top_p=top_p)
                if idx == self.EOS_IDX:
                    break
                if avoid_repeats and idx in seq_idx:
                    last[idx] = float("-inf")
                    idx = self._sample_logits(last, temperature=temperature, top_k=top_k, top_p=top_p)
                    if idx == self.EOS_IDX:
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
    batch_size = min(64, max(1, len(dataset)))
    loader = DataLoader(dataset, batch_size=batch_size, shuffle=True, collate_fn=dataset.collate)
    vocab_size = max(dataset.venue_map.values(), default=2) + 1
    model = RouteGenerator(vocab_size, embed_dim, hidden_dim, num_layers)
    model.venue_map = dataset.venue_map
    try:
        if torch.cuda.is_available():
            device = torch.device("cuda")
        elif hasattr(torch, "backends") and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
            device = torch.device("mps")
        else:
            device = torch.device("cpu")
    except (AttributeError, ImportError):  # pragma: no cover - torch stubs
        device = torch.device("cpu")
    model.to(device)
    opt = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.CrossEntropyLoss(ignore_index=dataset.PAD_IDX)
    grad_clip = 1.0
    for _ in range(max(1, epochs)):
        for x, y in loader:
            x = x.to(device)
            y = y.to(device)
            opt.zero_grad(set_to_none=True)
            out, _ = model(x)
            loss = loss_fn(out.view(-1, out.size(-1)), y.view(-1))
            loss.backward()
            if grad_clip > 0:
                nn.utils.clip_grad_norm_(model.parameters(), grad_clip)
            opt.step()
    model.eval()
    return model


def save_route_generator(model: RouteGenerator, path: str) -> None:
    torch.save(
        {
            "state": model.state_dict(),
            "venue_map": model.venue_map,
            "special": {"PAD": model.PAD_IDX, "SOS": model.SOS_IDX, "EOS": model.EOS_IDX},
        },
        path,
    )


def load_route_generator(path: str | None) -> RouteGenerator | None:
    if not path or not os.path.exists(path):
        return None
    obj = torch.load(path, map_location="cpu")
    venue_map = obj.get("venue_map", {})
    vocab_size = max(venue_map.values(), default=2) + 1
    model = RouteGenerator(vocab_size)
    model.load_state_dict(obj["state"])
    model.venue_map = venue_map
    sp = obj.get("special", {})
    model.PAD_IDX = int(sp.get("PAD", 0))
    model.SOS_IDX = int(sp.get("SOS", 1))
    model.EOS_IDX = int(sp.get("EOS", 2))
    model.eval()
    return model
