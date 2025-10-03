from __future__ import annotations

import inspect
import os
import random
from typing import List, Dict, Any

import asyncio
import logging

from pathlib import Path

import numpy as np
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
            raise ImportError("torch is required for DQN")

    def manual_seed(self, *a, **k):  # pragma: no cover - stub
        raise ImportError("torch is required for DQN")

    def __getattr__(self, name):
        raise ImportError("torch is required for DQN")


_torch = try_import("torch", stub=_TorchStub())
if isinstance(_torch, _TorchStub):  # pragma: no cover - optional dependency
    torch = nn = optim = _torch  # type: ignore
else:
    torch = _torch  # type: ignore
    from torch import nn, optim  # type: ignore

from . import BaseAgent
from .memory import MemoryAgent
from .price_utils import resolve_price
from ..portfolio import Portfolio
from ..replay import ReplayBuffer
from ..regime import detect_regime
from .. import models
from ..simulation import predict_price_movement as _predict_price_movement


def predict_price_movement(token: str, *, model_path: str | None = None) -> float:
    """Wrapper around :func:`simulation.predict_price_movement`."""
    return _predict_price_movement(token, model_path=model_path)


class DQNAgent(BaseAgent):
    """Deep Q-Network agent that learns from trade history."""

    name = "dqn"

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        hidden_size: int = 8,
        learning_rate: float = 0.001,
        epsilon: float = 0.1,
        discount: float = 0.95,
        model_path: str | Path = "dqn_model.pt",
        replay_url: str = "sqlite:///replay.db",
        price_model_path: str | None = None,
        device: str | torch.device | None = device_module.get_default_device(),
    ) -> None:
        self.memory_agent = memory_agent or MemoryAgent()
        self.epsilon = epsilon
        self.discount = discount
        self.model_path = Path(model_path)
        self.replay = ReplayBuffer(replay_url)
        self.price_model_path = price_model_path or os.getenv("PRICE_MODEL_PATH")
        self.device = device_module.get_default_device(device)
        self._last_mtime = 0.0
        self._seen_ids: set[int] = set()
        self._last_id: int = 0
        self._task: asyncio.Task | None = None
        self._logger = logging.getLogger(__name__)

        self.model = nn.Sequential(
            nn.Linear(1, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 2),
        )
        self.model.to(self.device)
        self.optimizer = optim.Adam(self.model.parameters(), lr=learning_rate)
        self.loss_fn = nn.MSELoss()
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
        self.model.load_state_dict(data.get("model_state", {}))
        opt_state = data.get("optimizer_state")
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
        return [amt]

    def _predict_return(self, token: str) -> float:
        return predict_price_movement(token, model_path=self.price_model_path)

    async def train(self, portfolio: Portfolio, regime: str | None = None) -> None:
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
            emotion = getattr(t, "emotion", "")
            if emotion == "regret":
                continue
            seq = prices.setdefault(t.token, [])
            regime_label = detect_regime(seq)
            self.replay.add(
                [float(t.amount)],
                t.direction,
                reward,
                emotion,
                regime_label,
            )
            seq.append(float(t.price))

        batch = self.replay.sample(32, regime=regime)
        if not batch:
            return

        X: List[List[float]] = []
        y: List[List[float]] = []
        for state, action, reward, _, _ in batch:
            X.append(list(state))
            y.append([reward, -reward])

        X_arr = torch.tensor(np.array(X), dtype=torch.float32, device=self.device)
        y_arr = torch.tensor(np.array(y), dtype=torch.float32, device=self.device)

        self.model.train()
        for _ in range(100):
            self.optimizer.zero_grad()
            pred = self.model(X_arr)
            loss = self.loss_fn(pred, y_arr)
            loss.backward()
            self.optimizer.step()
        self._fitted = True

        if self.model_path:
            torch.save(
                {
                    "model_state": self.model.cpu().state_dict(),
                    "optimizer_state": self.optimizer.state_dict(),
                },
                self.model_path,
            )
            self.model.to(self.device)
            self._last_mtime = os.path.getmtime(self.model_path)

    async def _online_loop(self, interval: float = 60.0) -> None:
        """Continuously train the agent from new replay data."""
        portfolio = Portfolio()
        while True:
            try:
                await self.train(portfolio)
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
        await self.train(portfolio, regime)
        state = torch.tensor([self._state(token, portfolio)], dtype=torch.float32, device=self.device)
        if self._fitted:
            with torch.no_grad():
                if self._jit is not None:
                    q = self._jit(state)[0].cpu().numpy()
                else:
                    q = self.model(state)[0].cpu().numpy()
        else:
            q = [0.0, 0.0]
        pred = self._predict_return(token)
        q[0] += pred
        q[1] -= pred
        if random.random() < self.epsilon:
            action = random.choice(["buy", "sell"])
        else:
            action = "buy" if q[0] >= q[1] else "sell"
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
