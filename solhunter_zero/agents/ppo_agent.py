from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Any, Dict, List

import solhunter_zero.device as device_module
from ..optional_imports import try_import


class _TorchStub:
    class Tensor:
        pass

    class device:
        def __init__(self, *a, **k) -> None:
            pass

    class Module:
        def __init__(self, *a, **k) -> None:
            raise ImportError("torch is required for PPOAgent")

    def __getattr__(self, name):
        raise ImportError("torch is required for PPOAgent")


_torch = try_import("torch", stub=_TorchStub())
if isinstance(_torch, _TorchStub):  # pragma: no cover - optional dependency
    torch = nn = optim = F = _torch  # type: ignore
else:
    torch = _torch  # type: ignore
    from torch import nn, optim  # type: ignore
    F = try_import("torch.nn.functional", stub=_TorchStub())  # type: ignore

import asyncio
import inspect
import logging

from . import BaseAgent
from .memory import MemoryAgent
from .price_utils import resolve_price
from ..util import parse_bool_env
from ..offline_data import OfflineData
from ..order_book_ws import snapshot
from ..portfolio import Portfolio
from ..replay import ReplayBuffer
from ..regime import detect_regime
from .. import models
from ..simulation import predict_price_movement as _predict_price_movement


def predict_price_movement(token: str, *, model_path: str | None = None) -> float:
    """Wrapper around :func:`simulation.predict_price_movement`."""
    return _predict_price_movement(token, model_path=model_path)


class PPOAgent(BaseAgent):
    """Actor-critic agent trained with PPO."""

    name = "ppo"

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        data_url: str = "sqlite:///offline_data.db",
        hidden_size: int = 32,
        learning_rate: float = 3e-4,
        gamma: float = 0.99,
        clip_epsilon: float = 0.2,
        epochs: int = 5,
        regime_weight: float = 1.0,
        model_path: str | Path = "ppo_model.pt",
        replay_url: str = "sqlite:///replay.db",
        price_model_path: str | None = None,
        device: str | torch.device | None = device_module.get_default_device(),
    ) -> None:
        self.memory_agent = memory_agent or MemoryAgent()
        self.offline_data = OfflineData(data_url)
        self.gamma = gamma
        self.clip_epsilon = clip_epsilon
        self.epochs = int(epochs)
        self.model_path = Path(model_path)
        self.replay = ReplayBuffer(replay_url)
        self.regime_weight = float(regime_weight)
        self.price_model_path = price_model_path or os.getenv("PRICE_MODEL_PATH")
        self.device = device_module.get_default_device(device)
        self._last_mtime = 0.0
        self._seen_ids: set[int] = set()
        self._last_id: int = 0
        self._task: asyncio.Task | None = None
        self._logger = logging.getLogger(__name__)

        self.actor = nn.Sequential(
            nn.Linear(4, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 2),
        )
        self.critic = nn.Sequential(
            nn.Linear(4, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )
        self.actor.to(self.device)
        self.critic.to(self.device)
        use_compile = parse_bool_env("USE_TORCH_COMPILE", True)
        if use_compile:
            try:
                if getattr(torch, "compile", None) and int(torch.__version__.split(".")[0]) >= 2:
                    self.actor = torch.compile(self.actor)
                    self.critic = torch.compile(self.critic)
            except Exception:
                pass
        self.optimizer = optim.Adam(
            list(self.actor.parameters()) + list(self.critic.parameters()),
            lr=learning_rate,
        )
        self._fitted = False
        self._jit = None

        if self.model_path.exists():
            self._load_weights()

        from ..event_bus import subscription
        self._rl_sub = subscription("rl_weights", lambda _p: self.reload_weights())
        self._rl_sub.__enter__()

    # ------------------------------------------------------------------
    def _load_weights(self) -> None:
        script_path = self.model_path.with_suffix(".ptc")
        if script_path.exists():
            try:
                self._jit = torch.jit.load(script_path, map_location=self.device)
                self._last_mtime = os.path.getmtime(script_path)
                self._fitted = True
                return
            except Exception:
                self._jit = None
        data = torch.load(self.model_path, map_location=self.device)
        self.actor.load_state_dict(data.get("actor_state", {}))
        self.critic.load_state_dict(data.get("critic_state", {}))
        opt_state = data.get("optim_state")
        if opt_state:
            self.optimizer.load_state_dict(opt_state)
        self._last_mtime = os.path.getmtime(self.model_path)
        self._fitted = True
        self._jit = None

    def reload_weights(self) -> None:
        """Public method for reloading weights from disk."""
        self._load_weights()

    def _maybe_reload(self) -> None:
        if not self.model_path.exists():
            return
        script = self.model_path.with_suffix(".ptc")
        mtime = os.path.getmtime(script if script.exists() else self.model_path)
        if mtime > self._last_mtime:
            self._load_weights()

    # ------------------------------------------------------------------
    def _state(self, token: str, portfolio: Portfolio) -> List[float]:
        pos = portfolio.balances.get(token)
        amt = float(pos.amount) if pos else 0.0
        depth, imb, _ = snapshot(token)
        regime = detect_regime(portfolio.price_history.get(token, []))
        r = {"bull": 1.0, "bear": -1.0}.get(regime, 0.0) * self.regime_weight
        return [amt, depth, imb, r]

    def _predict_return(self, token: str) -> float:
        return predict_price_movement(token, model_path=self.price_model_path)

    async def _log_trades(self) -> None:
        loader = getattr(self.memory_agent.memory, "list_trades", None)
        if loader is None:
            return
        trades = loader(since_id=self._last_id)
        if inspect.isawaitable(trades):
            trades = await trades
        prices: dict[str, list[float]] = {}
        for t in trades:
            tid = getattr(t, "id", None)
            if tid is not None and tid in self._seen_ids:
                continue
            if tid is not None:
                self._seen_ids.add(tid)
                if tid > self._last_id:
                    self._last_id = tid
            reward = float(t.amount) * float(t.price)
            if t.direction == "buy":
                reward = -reward
            seq = prices.setdefault(t.token, [])
            regime_label = detect_regime(seq)
            r = {"bull": 1.0, "bear": -1.0}.get(regime_label, 0.0) * self.regime_weight
            state = [float(t.amount), float(t.price), 0.0, r]
            self.replay.add(
                state,
                t.direction,
                reward,
                getattr(t, "emotion", ""),
                regime_label,
            )
            seq.append(float(t.price))

    async def train(self, regime: str | None = None) -> None:
        await self._log_trades()
        batch = self.replay.sample(64, regime=regime)
        if not batch:
            return

        states = torch.tensor([b[0] for b in batch], dtype=torch.float32, device=self.device)
        actions = torch.tensor([0 if b[1] == "buy" else 1 for b in batch], device=self.device)
        rewards = torch.tensor([b[2] for b in batch], dtype=torch.float32, device=self.device)

        with torch.no_grad():
            old_log_probs = (
                torch.distributions.Categorical(logits=self.actor(states))
                .log_prob(actions)
            )
            values = self.critic(states).squeeze()
            advantages = rewards - values
            returns = rewards

        for _ in range(self.epochs):
            dist = torch.distributions.Categorical(logits=self.actor(states))
            log_probs = dist.log_prob(actions)
            ratio = torch.exp(log_probs - old_log_probs)
            s1 = ratio * advantages
            s2 = torch.clamp(ratio, 1 - self.clip_epsilon, 1 + self.clip_epsilon) * advantages
            actor_loss = -torch.min(s1, s2).mean()

            value_pred = self.critic(states).squeeze()
            critic_loss = F.mse_loss(value_pred, returns)

            loss = actor_loss + 0.5 * critic_loss

            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()

        self._fitted = True
        torch.save(
            {
                "actor_state": self.actor.cpu().state_dict(),
                "critic_state": self.critic.cpu().state_dict(),
                "optim_state": self.optimizer.state_dict(),
            },
            self.model_path,
        )
        self.actor.to(self.device)
        self.critic.to(self.device)
        self._last_mtime = os.path.getmtime(self.model_path)

    async def _online_loop(self, interval: float = 60.0) -> None:
        """Continuously train the agent from new replay data."""
        while True:
            try:
                await self.train()
            except Exception as exc:  # pragma: no cover - logging
                self._logger.error("online train failed: %s", exc)
            await asyncio.sleep(interval)

    def start_online_learning(self, interval: float = 60.0) -> None:
        """Start background task that updates the model periodically."""
        if self._task is None:
            self._task = asyncio.create_task(self._online_loop(interval))

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        self.start_online_learning()
        self._maybe_reload()
        regime = detect_regime(portfolio.price_history.get(token, []))
        await self.train(regime)
        state = torch.tensor([self._state(token, portfolio)], dtype=torch.float32, device=self.device)
        with torch.no_grad():
            if self._jit is not None and hasattr(self._jit, "actor"):
                logits = self._jit.actor(state)[0]
            else:
                logits = self.actor(state)[0]
        pred = self._predict_return(token)
        logits = logits + torch.tensor([pred, -pred], device=self.device)
        action = "buy" if logits[0] >= logits[1] else "sell"
        if action == "buy":
            price, context = await resolve_price(token, portfolio)
            if price <= 0:
                self._logger.info(
                    "%s agent skipping buy for %s due to missing price: %s",
                    self.name,
                    token,
                    context,
                )
                return []
            return [
                {
                    "token": token,
                    "side": "buy",
                    "amount": 1.0,
                    "price": price,
                    "agent": self.name,
                }
            ]
        position = portfolio.balances.get(token)
        if position:
            price, context = await resolve_price(token, portfolio)
            if price <= 0:
                self._logger.info(
                    "%s agent skipping sell for %s due to missing price: %s",
                    self.name,
                    token,
                    context,
                )
                return []
            return [
                {
                    "token": token,
                    "side": "sell",
                    "amount": position.amount,
                    "price": price,
                    "agent": self.name,
                }
            ]
        return []

    def close(self) -> None:
        if getattr(self, "_rl_sub", None):
            self._rl_sub.__exit__(None, None, None)
