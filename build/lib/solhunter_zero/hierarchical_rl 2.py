from __future__ import annotations

import json
import os
from typing import Iterable, Dict, Any, List

from .agents import BaseAgent

try:  # pragma: no cover - optional
    import torch
    import torch.nn as nn
except Exception:  # pragma: no cover - optional
    torch = None
    nn = None

from .models import load_compiled_model
from .device import get_default_device


class HighLevelPolicyNetwork(nn.Module if torch else object):
    """Simple network predicting weights for each agent."""

    def __init__(self, num_agents: int) -> None:
        if torch:
            super().__init__()
            self.weights = nn.Parameter(torch.ones(num_agents, dtype=torch.float32))
        else:  # pragma: no cover - lightweight fallback
            self.weights = [1.0 for _ in range(num_agents)]
        self.num_agents = num_agents

    def predict(self, names: Iterable[str]) -> Dict[str, float]:  # pragma: no cover - simple
        if torch and isinstance(self.weights, torch.Tensor):
            vals: List[float] = self.weights.detach().cpu().tolist()
        else:
            vals = list(self.weights)
        return {n: float(vals[i]) if i < len(vals) else 1.0 for i, n in enumerate(names)}


def roi_by_agent(trades: Iterable[Any], names: Iterable[str]) -> Dict[str, float]:
    """Return ROI for ``names`` based on ``trades``."""
    summary: Dict[str, Dict[str, float]] = {n: {"buy": 0.0, "sell": 0.0} for n in names}
    for t in trades:
        name = getattr(t, "reason", None)
        if name not in summary:
            continue
        direction = getattr(t, "direction", "buy")
        val = float(getattr(t, "amount", 0.0)) * float(getattr(t, "price", 0.0))
        summary[name][direction] += val
    rois = {}
    for n, info in summary.items():
        spent = info.get("buy", 0.0)
        revenue = info.get("sell", 0.0)
        rois[n] = (revenue - spent) / spent if spent > 0 else 0.0
    return rois


def train_policy(
    model: HighLevelPolicyNetwork,
    trades: Iterable[Any],
    names: Iterable[str],
    *,
    lr: float = 0.1,
    epochs: int = 1,
) -> Dict[str, float]:
    """Update ``model`` weights based on ROI derived from ``trades``."""

    names = list(names)
    rois = roi_by_agent(trades, names)
    if torch and isinstance(model.weights, torch.Tensor):
        target = torch.tensor([rois.get(n, 1.0) for n in names], dtype=torch.float32)
        opt = torch.optim.SGD([model.weights], lr=lr)
        for _ in range(max(1, int(epochs))):
            opt.zero_grad()
            loss = ((model.weights - target) ** 2).mean()
            loss.backward()
            opt.step()
    else:  # pragma: no cover - lightweight fallback
        for i, n in enumerate(names):
            target = rois.get(n, 1.0)
            model.weights[i] += lr * (target - model.weights[i])
    return model.predict(names)


def save_policy(model: HighLevelPolicyNetwork, path: str) -> None:
    """Persist ``model`` weights to ``path``."""
    if torch and isinstance(model.weights, torch.Tensor):
        data = model.weights.detach().cpu().tolist()
    else:  # pragma: no cover - lightweight fallback
        data = list(model.weights)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def load_policy(path: str, num_agents: int) -> HighLevelPolicyNetwork:
    """Load policy from ``path`` or create a new one."""
    model = HighLevelPolicyNetwork(num_agents)
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list) and len(data) >= num_agents:
                if torch and isinstance(model.weights, torch.Tensor):
                    model.weights.data = torch.tensor(data[:num_agents], dtype=torch.float32)
                else:
                    model.weights = [float(v) for v in data[:num_agents]]
        except Exception:  # pragma: no cover - bad file
            pass
    return model


class SupervisorAgent(BaseAgent):
    """Load a policy checkpoint to select or weight strategies."""

    name = "supervisor"

    def __init__(self, checkpoint: str = "supervisor.json", device: str | None = None) -> None:
        self.checkpoint = checkpoint
        try:
            self.device = str(get_default_device(device))
        except Exception:
            self.device = "cpu"
        self.policy: Dict[str, float] = {}
        self.model = None
        self._load()

    def _load(self) -> None:
        if not self.checkpoint or not os.path.exists(self.checkpoint):
            return
        if self.checkpoint.endswith(".json"):
            try:
                with open(self.checkpoint, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, dict):
                    self.policy = {str(k): float(v) for k, v in data.items()}
            except Exception:  # pragma: no cover - invalid file
                self.policy = {}
        else:
            try:
                self.model = load_compiled_model(self.checkpoint, self.device)
            except Exception:  # pragma: no cover - load failure
                self.model = None

    def predict_weights(
        self,
        agent_names: Iterable[str],
        token: str | None = None,
        portfolio: Any | None = None,
    ) -> Dict[str, float]:
        names = list(agent_names)
        if self.model is not None and torch is not None:
            try:
                with torch.no_grad():  # pragma: no cover - simple inference
                    x = torch.zeros((1, len(names)), dtype=torch.float32)
                    out = self.model(x)
                vals = out.squeeze().tolist()
                if not isinstance(vals, list):
                    vals = [float(vals)] * len(names)
                return {n: float(vals[i]) for i, n in enumerate(names)}
            except Exception:  # pragma: no cover - inference failure
                pass
        return {n: self.policy.get(n, 1.0) for n in names}

    async def propose_trade(
        self,
        token: str,
        portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> list[Dict[str, Any]]:
        return []
