"""Sequential reinforcement-learning training primitives.

This module replaces the historical Lightning-based training stack with a
compact, single-threaded pipeline that is easier to reason about and debug.
It focuses on three responsibilities:

* converting trade/snapshot history into dense feature tensors
* training light-weight neural policies one algorithm at a time
* persisting checkpoints and broadcasting minimal runtime metadata

The implementation intentionally avoids background threads, process pools, and
event-loop trickery so that each training cycle can be traced from end to end.
All heavy lifting happens on the calling coroutine's event loop.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Dict, Iterable, List, Optional, Tuple

import numpy as np
import torch
from torch import nn
from torch.nn import functional as F
from torch.utils.data import DataLoader, Dataset

from .device import get_default_device
from .event_bus import publish

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dataset construction
# ---------------------------------------------------------------------------


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:  # pragma: no cover - defensive cast
        return default


def _normalize_angle(value: float) -> float:
    """Map any numeric input onto the range [-1, 1]."""

    if value == 0:
        return 0.0
    return math.tanh(value)


def _default_timestamp(obj: Any) -> float:
    # Prefer DB canonical time, then fall back to the legacy timestamp field.
    ts = getattr(obj, "created_at", None)
    if ts is None:
        ts = getattr(obj, "timestamp", None)
    if ts is None:
        return 0.0
    if hasattr(ts, "timestamp"):
        return float(ts.timestamp())
    try:
        return float(ts)
    except Exception:  # pragma: no cover - defensive cast
        return 0.0


def _direction_flag(direction: Any) -> Tuple[int, float]:
    label = str(direction).lower()
    if label in {"sell", "short", "-1"}:
        return 1, -1.0
    return 0, 1.0


def _build_snapshot_index(snapshots: Iterable[Any]) -> Dict[str, List[Any]]:
    index: Dict[str, List[Any]] = {}
    for snap in snapshots:
        token = getattr(snap, "token", None)
        if not token:
            continue
        bucket = index.setdefault(str(token), [])
        bucket.append(snap)
    for bucket in index.values():
        bucket.sort(key=_default_timestamp)
    return index


def _lookup_snapshot(token: str, ts: float, index: Dict[str, List[Any]]) -> Any | None:
    bucket = index.get(token)
    if not bucket:
        return None
    lo, hi = 0, len(bucket) - 1
    best = bucket[hi]
    while lo <= hi:
        mid = (lo + hi) // 2
        snap = bucket[mid]
        snap_ts = _default_timestamp(snap)
        if snap_ts <= ts:
            best = snap
            lo = mid + 1
        else:
            hi = mid - 1
    return best


def _collect_features(
    trades: Iterable[Any],
    snapshots: Iterable[Any],
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    """Convert trades/snapshots into tensors understood by the trainers."""

    snapshot_index = _build_snapshot_index(snapshots)
    states: List[List[float]] = []
    actions: List[int] = []
    rewards: List[float] = []

    price_history: Dict[str, List[float]] = {}

    for trade in trades:
        token = getattr(trade, "token", None)
        if not token:
            continue
        token = str(token)
        ts = _default_timestamp(trade)
        price = _as_float(getattr(trade, "price", 0.0))
        amount = _as_float(getattr(trade, "amount", 0.0))
        action, direction_flag = _direction_flag(getattr(trade, "direction", "buy"))
        reward = amount * price
        if action == 0:  # buys penalise outgoing cash
            reward = -reward

        snap = _lookup_snapshot(token, ts, snapshot_index)
        depth = _as_float(getattr(snap, "depth", 0.0)) if snap else 0.0
        imbalance = _as_float(getattr(snap, "imbalance", 0.0)) if snap else 0.0
        slippage = _as_float(getattr(snap, "slippage", 0.0)) if snap else 0.0
        tx_rate = _as_float(getattr(snap, "tx_rate", 0.0)) if snap else 0.0
        volume = _as_float(getattr(snap, "volume", 0.0)) if snap else 0.0

        history = price_history.setdefault(token, [])
        prev_price = history[-1] if history else price
        change = (price - prev_price) / prev_price if prev_price else 0.0
        change = _normalize_angle(change)
        history.append(price)
        avg = sum(history[-10:]) / min(len(history), 10)
        momentum = (price - avg) / avg if avg else 0.0
        momentum = _normalize_angle(momentum)

        states.append(
            [
                _normalize_angle(price / 1_000.0),
                _normalize_angle(amount),
                direction_flag,
                _normalize_angle(depth),
                _normalize_angle(imbalance),
                _normalize_angle(slippage),
                _normalize_angle(tx_rate),
                _normalize_angle(volume),
                momentum + change,
            ]
        )
        actions.append(action)
        rewards.append(reward)

    if not states:
        return (
            torch.zeros((0, 9), dtype=torch.float32),
            torch.zeros(0, dtype=torch.long),
            torch.zeros(0, dtype=torch.float32),
        )

    state_tensor = torch.tensor(states, dtype=torch.float32)
    action_tensor = torch.tensor(actions, dtype=torch.long)
    reward_tensor = torch.tensor(rewards, dtype=torch.float32)
    return state_tensor, action_tensor, reward_tensor


class _TradeDataset(Dataset):
    """Torch dataset wrapping the sequential feature tensors."""

    def __init__(self, trades: Iterable[Any], snapshots: Iterable[Any]):
        self.states, self.actions, self.rewards = _collect_features(trades, snapshots)

    def __len__(self) -> int:
        return len(self.actions)

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return (
            self.states[idx],
            self.actions[idx],
            self.rewards[idx],
        )


# ---------------------------------------------------------------------------
# Policy models
# ---------------------------------------------------------------------------


class LightningPPO(nn.Module):
    """Minimal PPO-style policy/value pair."""

    def __init__(self, input_size: int = 9, hidden_size: int = 64) -> None:
        super().__init__()
        self.actor = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 2),
        )
        self.critic = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # pragma: no cover - tiny
        return self.actor(x)

    def value(self, x: torch.Tensor) -> torch.Tensor:
        return self.critic(x)


class LightningDQN(nn.Module):
    """Two-action Q-network."""

    def __init__(self, input_size: int = 9, hidden_size: int = 64) -> None:
        super().__init__()
        self.model = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 2),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.model(x)


class LightningA3C(nn.Module):
    """Actor-critic head mirroring PPO but without clip logic."""

    def __init__(self, input_size: int = 9, hidden_size: int = 64) -> None:
        super().__init__()
        self.actor = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 2),
        )
        self.critic = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.actor(x)

    def value(self, x: torch.Tensor) -> torch.Tensor:
        return self.critic(x)


class LightningDDPG(nn.Module):
    """Lightweight deterministic policy network."""

    def __init__(self, input_size: int = 9, hidden_size: int = 64) -> None:
        super().__init__()
        self.actor = nn.Sequential(
            nn.Linear(input_size, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
            nn.Tanh(),
        )
        self.critic = nn.Sequential(
            nn.Linear(input_size + 1, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.actor(x)

    def q_value(self, x: torch.Tensor, action: torch.Tensor) -> torch.Tensor:
        if action.dim() == 1:
            action = action.unsqueeze(1)
        return self.critic(torch.cat([x, action], dim=1))


def _model_for_algo(algo: str) -> nn.Module:
    algo = algo.lower()
    if algo == "dqn":
        return LightningDQN()
    if algo == "a3c":
        return LightningA3C()
    if algo == "ddpg":
        return LightningDDPG()
    return LightningPPO()


# ---------------------------------------------------------------------------
# Training configuration and loops
# ---------------------------------------------------------------------------


@dataclass
class TrainingConfig:
    epochs: int = 3
    batch_size: int = 64
    learning_rate: float = 1e-3
    min_batch: int = 16
    device: str | None = None
    shuffle: bool = True

    def resolve_device(self) -> torch.device:
        if isinstance(self.device, torch.device):
            return self.device
        if self.device:
            try:
                return torch.device(self.device)
            except Exception:  # pragma: no cover - invalid override
                pass
        return get_default_device()


@dataclass
class TrainingSummary:
    algo: str
    samples: int
    epochs: int
    loss: float
    mean_reward: float


def _loss_for_batch(
    algo: str,
    model: nn.Module,
    states: torch.Tensor,
    actions: torch.Tensor,
    rewards: torch.Tensor,
) -> torch.Tensor:
    algo = algo.lower()
    if algo == "ddpg":
        preds = model(states)
        target = torch.tanh(rewards.unsqueeze(1) / 100.0)
        return F.mse_loss(preds, target)

    logits = model(states)
    target = torch.stack([rewards, -rewards], dim=1) / 100.0
    loss = F.mse_loss(logits, target)
    if hasattr(model, "value"):
        value = model.value(states).squeeze(1)
        loss = loss + 0.1 * F.mse_loss(value, rewards / 100.0)
    return loss


def train_model(
    dataset: _TradeDataset,
    *,
    algo: str = "ppo",
    config: TrainingConfig | None = None,
    initial_state: Dict[str, Any] | None = None,
) -> Tuple[Dict[str, Any], TrainingSummary]:
    """Run a synchronous training loop for ``algo`` over ``dataset``."""

    cfg = config or TrainingConfig()
    if len(dataset) < cfg.min_batch:
        return {}, TrainingSummary(algo=algo, samples=len(dataset), epochs=0, loss=0.0, mean_reward=0.0)

    device = cfg.resolve_device()
    model = _model_for_algo(algo).to(device)
    if initial_state:
        try:
            model.load_state_dict(initial_state, strict=False)
        except Exception:  # pragma: no cover - corrupt checkpoint
            logger.warning("Failed to load previous state for %s", algo, exc_info=True)

    loader = DataLoader(
        dataset,
        batch_size=cfg.batch_size,
        shuffle=cfg.shuffle,
    )
    optim = torch.optim.Adam(model.parameters(), lr=cfg.learning_rate)

    last_loss = 0.0
    for _ in range(cfg.epochs):
        for states, actions, rewards in loader:
            states = states.to(device)
            actions = actions.to(device)
            rewards = rewards.to(device)
            optim.zero_grad()
            loss = _loss_for_batch(algo, model, states, actions, rewards)
            loss.backward()
            optim.step()
            last_loss = float(loss.detach().cpu())

    mean_reward = float(dataset.rewards.mean()) if len(dataset) else 0.0
    state_dict = model.state_dict()
    return state_dict, TrainingSummary(
        algo=algo,
        samples=len(dataset),
        epochs=cfg.epochs,
        loss=last_loss,
        mean_reward=mean_reward,
    )


def fit(
    trades: Iterable[Any],
    snapshots: Iterable[Any],
    *,
    model_path: str | Path,
    algo: str = "ppo",
    config: TrainingConfig | None = None,
) -> TrainingSummary:
    """Train a policy on in-memory samples and persist the resulting weights."""

    dataset = _TradeDataset(trades, snapshots)
    path = Path(model_path)
    existing: Dict[str, Any] | None = None
    if path.exists():
        with contextlib.suppress(Exception):
            existing = torch.load(path, map_location="cpu")

    publish(
        "runtime.log",
        {
            "stage": "rl",
            "detail": f"dataset_ready algo={algo} samples={len(dataset)}",
            "ts": time.time(),
            "level": "INFO",
        },
    )

    state_dict, summary = train_model(dataset, algo=algo, config=config, initial_state=existing)
    if not state_dict:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        publish(
            "runtime.log",
            {
                "stage": "rl",
                "detail": f"skipped_training algo={algo} samples={len(dataset)} (< min_batch)",
                "ts": time.time(),
                "level": "INFO",
            },
        )
        return summary

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    torch.save(state_dict, tmp)
    tmp.replace(path)

    publish("rl_checkpoint", {"time": time.time(), "path": str(path)})
    publish(
        "runtime.log",
        {
            "stage": "rl",
            "detail": (
                f"checkpoint_saved algo={summary.algo} samples={summary.samples} "
                f"loss={summary.loss:.6f} mean_reward={summary.mean_reward:.6f}"
            ),
            "ts": time.time(),
            "level": "INFO",
        },
    )
    return summary


# ---------------------------------------------------------------------------
# Periodic trainer helpers
# ---------------------------------------------------------------------------


async def _load_offline_samples(db_url: str) -> Tuple[List[Any], List[Any]]:
    from .offline_data import OfflineData

    data = OfflineData(db_url)
    trades = await data.list_trades()
    snaps = await data.list_snapshots()
    await data.close()
    return list(trades), list(snaps)


@dataclass
class RLTraining:
    """Utility wrapper that performs sequential training from storage."""

    db_url: str = "sqlite:///offline_data.db"
    model_path: Path = Path("ppo_model.pt")
    algo: str = "ppo"
    config: TrainingConfig = field(default_factory=TrainingConfig)

    async def train_once(self) -> TrainingSummary:
        trades, snaps = await _load_offline_samples(self.db_url)
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None,
            lambda: fit(
                trades,
                snaps,
                model_path=self.model_path,
                algo=self.algo,
                config=self.config,
            ),
        )


class MultiAgentRL:
    """Train multiple algorithms and derive portfolio weights."""

    def __init__(
        self,
        *,
        db_url: str = "sqlite:///offline_data.db",
        algos: Iterable[str] | None = None,
        model_base: str = "population_model.pt",
        config: TrainingConfig | None = None,
        controller_path: str | Path | None = None,
    ) -> None:
        self.db_url = db_url
        self.algos = list(algos or ["ppo", "dqn"])
        base = Path(model_base)
        self.model_paths = [base.with_name(f"{base.stem}_{algo}.pt") for algo in self.algos]
        self.config = config or TrainingConfig(epochs=2, batch_size=128)
        self._last_summaries: Dict[str, TrainingSummary] = {}
        self.controller_path = Path(controller_path) if controller_path else base.with_name("rl_controller.json")

    async def train_population(self) -> Dict[str, TrainingSummary]:
        trades, snaps = await _load_offline_samples(self.db_url)
        loop = asyncio.get_running_loop()

        summaries: Dict[str, TrainingSummary] = {}
        publish(
            "runtime.log",
            {
                "stage": "rl",
                "detail": (
                    f"population_start algos={self.algos} "
                    f"samples_trades={len(trades)} samples_snaps={len(snaps)}"
                ),
                "ts": time.time(),
                "level": "INFO",
            },
        )
        for algo, path in zip(self.algos, self.model_paths):
            summary = await loop.run_in_executor(
                None,
                lambda a=algo, p=path: fit(
                    trades,
                    snaps,
                    model_path=p,
                    algo=a,
                    config=self.config,
                ),
            )
            summaries[algo] = summary
        publish(
            "runtime.log",
            {
                "stage": "rl",
                "detail": f"population_done algos={self.algos}",
                "ts": time.time(),
                "level": "INFO",
            },
        )
        self._last_summaries = summaries
        return summaries

    def train_controller(self, agent_names: Iterable[str]) -> Dict[str, float]:
        if not self._last_summaries:
            return {name: 1.0 / max(1, len(list(agent_names))) for name in agent_names}
        rewards = np.array(
            [
                self._last_summaries.get(
                    algo, TrainingSummary(algo, 0, 0, 0.0, 0.0)
                ).mean_reward
                for algo in self.algos
            ],
            dtype=float,
        )
        rewards = np.maximum(rewards, 0.0)
        if rewards.sum() == 0:
            rewards[:] = 1.0
        weights = rewards / rewards.sum()
        mapping: Dict[str, float] = {}
        agents = list(agent_names)
        for idx, name in enumerate(agents):
            mapping[name] = float(weights[idx % len(weights)])
        try:
            import json

            self.controller_path.parent.mkdir(parents=True, exist_ok=True)
            with self.controller_path.open("w", encoding="utf-8") as fh:
                json.dump(mapping, fh, indent=2)
        except Exception:  # pragma: no cover - disk failures
            logger.warning("Failed to persist RL controller", exc_info=True)
        publish("rl_weights", {"weights": mapping})
        return mapping


# ---------------------------------------------------------------------------
# Offline dataset helpers
# ---------------------------------------------------------------------------


def _ensure_mmap_dataset(db_url: str, out_path: Path) -> Awaitable[None] | None:
    """Persist a compact npz archive of the offline dataset if missing."""

    out_path = Path(out_path)
    if out_path.exists():
        return None

    async def _build() -> None:
        from .offline_data import OfflineData

        data = OfflineData(db_url)
        trades = await data.list_trades()
        snaps = await data.list_snapshots()
        await data.close()
        states, actions, rewards = _collect_features(trades, snaps)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            out_path,
            states=states.numpy(),
            actions=actions.numpy(),
            rewards=rewards.numpy(),
        )

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(_build())
        return None
    return loop.create_task(_build())


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------


__all__ = [
    "TrainingConfig",
    "TrainingSummary",
    "_TradeDataset",
    "LightningPPO",
    "LightningDQN",
    "LightningA3C",
    "LightningDDPG",
    "RLTraining",
    "MultiAgentRL",
    "fit",
    "train_model",
    "_ensure_mmap_dataset",
]
