from __future__ import annotations

import asyncio
import inspect
import logging
import os
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
            raise ImportError("torch is required for SACAgent")

    def __getattr__(self, name):
        raise ImportError("torch is required for SACAgent")


_torch = try_import("torch", stub=_TorchStub())
if isinstance(_torch, _TorchStub):  # pragma: no cover - optional dependency
    torch = nn = optim = F = _torch  # type: ignore
else:
    torch = _torch  # type: ignore
    from torch import nn, optim  # type: ignore
    F = try_import("torch.nn.functional", stub=_TorchStub())  # type: ignore

from . import BaseAgent
from .memory import MemoryAgent
from .price_utils import resolve_price
from ..offline_data import OfflineData
from ..order_book_ws import snapshot
from ..portfolio import Portfolio
from ..replay import ReplayBuffer
from ..regime import detect_regime
from ..simulation import predict_price_movement as _predict_price_movement


def predict_price_movement(token: str, *, model_path: str | None = None) -> float:
    """Wrapper around :func:`simulation.predict_price_movement`."""
    return _predict_price_movement(token, model_path=model_path)


class GaussianPolicy(nn.Module):
    def __init__(self, state_dim: int, hidden_size: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, hidden_size),
            nn.ReLU(),
        )
        self.mean = nn.Linear(hidden_size, 1)
        self.log_std = nn.Linear(hidden_size, 1)

    def forward(self, x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        h = self.net(x)
        return self.mean(h), self.log_std(h)


class QNetwork(nn.Module):
    def __init__(self, state_dim: int, hidden_size: int) -> None:
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim + 1, hidden_size),
            nn.ReLU(),
            nn.Linear(hidden_size, 1),
        )

    def forward(self, state: torch.Tensor, action: torch.Tensor) -> torch.Tensor:
        x = torch.cat([state, action.unsqueeze(-1)], dim=-1)
        return self.net(x).squeeze(-1)


class SACAgent(BaseAgent):
    """Minimal soft actor-critic trading agent."""

    name = "sac"

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        data_url: str = "sqlite:///offline_data.db",
        hidden_size: int = 32,
        learning_rate: float = 3e-4,
        gamma: float = 0.99,
        tau: float = 0.005,
        alpha: float = 0.2,
        regime_weight: float = 1.0,
        model_path: str | Path = "sac_model.pt",
        replay_url: str = "sqlite:///replay.db",
        price_model_path: str | None = None,
        device: str | torch.device | None = device_module.get_default_device(),
    ) -> None:
        self.memory_agent = memory_agent or MemoryAgent()
        self.offline_data = OfflineData(data_url)
        self.gamma = gamma
        self.tau = tau
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
        self._missing_logged = False

        self.actor = GaussianPolicy(4, hidden_size)
        self.q1 = QNetwork(4, hidden_size)
        self.q2 = QNetwork(4, hidden_size)
        self.tq1 = QNetwork(4, hidden_size)
        self.tq2 = QNetwork(4, hidden_size)
        self.tq1.load_state_dict(self.q1.state_dict())
        self.tq2.load_state_dict(self.q2.state_dict())

        self.actor.to(self.device)
        self.q1.to(self.device)
        self.q2.to(self.device)
        self.tq1.to(self.device)
        self.tq2.to(self.device)

        self.actor_opt = optim.Adam(self.actor.parameters(), lr=learning_rate)
        self.q1_opt = optim.Adam(self.q1.parameters(), lr=learning_rate)
        self.q2_opt = optim.Adam(self.q2.parameters(), lr=learning_rate)
        self.log_alpha = torch.log(torch.tensor(float(alpha), device=self.device))
        self.log_alpha.requires_grad = True
        self.alpha_opt = optim.Adam([self.log_alpha], lr=learning_rate)
        self.target_entropy = -1.0
        self._alpha = float(alpha)
        self._fitted = False
        self._jit = None
        if self.model_path.exists():
            self._load_weights()

        from ..event_bus import subscription
        self._rl_sub = subscription("rl_weights", lambda _p: self.reload_weights())
        self._rl_sub.__enter__()

    # --------------------------------------------------------------
    def _load_weights(self) -> None:
        if not self.model_path.exists():
            if not self._missing_logged:
                level = self._logger.warning if self._fitted else self._logger.info
                level("SAC model missing; skipping load (%s)", self.model_path)
                self._missing_logged = True
            self._fitted = False
            self._jit = None
            return
        script_path = self.model_path.with_suffix(".ptc")
        if script_path.exists():
            try:
                self._jit = torch.jit.load(script_path, map_location=self.device)
                self._last_mtime = os.path.getmtime(script_path)
                self._fitted = True
                self._missing_logged = False
                return
            except Exception:
                self._jit = None
        if not self.model_path.exists():
            return
        data = torch.load(self.model_path, map_location=self.device)
        self.actor.load_state_dict(data.get("actor", {}))
        self.q1.load_state_dict(data.get("q1", {}))
        self.q2.load_state_dict(data.get("q2", {}))
        self.tq1.load_state_dict(data.get("tq1", {}))
        self.tq2.load_state_dict(data.get("tq2", {}))
        self.actor_opt.load_state_dict(data.get("actor_opt", {}))
        self.q1_opt.load_state_dict(data.get("q1_opt", {}))
        self.q2_opt.load_state_dict(data.get("q2_opt", {}))
        self.alpha_opt.load_state_dict(data.get("alpha_opt", {}))
        self.log_alpha.data[:] = data.get("log_alpha", self.log_alpha).to(self.device)
        self._alpha = float(self.log_alpha.exp())
        self._last_mtime = os.path.getmtime(self.model_path)
        self._fitted = True
        self._jit = None
        self._missing_logged = False

    def reload_weights(self) -> None:
        self._load_weights()

    def _maybe_reload(self) -> None:
        if not self.model_path.exists():
            return
        script = self.model_path.with_suffix(".ptc")
        mtime = os.path.getmtime(script if script.exists() else self.model_path)
        if mtime > self._last_mtime:
            self._load_weights()

    # --------------------------------------------------------------
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
                action_val = float(t.amount)
            else:
                action_val = -float(t.amount)
            seq = prices.setdefault(t.token, [])
            regime_label = detect_regime(seq)
            r = {"bull": 1.0, "bear": -1.0}.get(regime_label, 0.0) * self.regime_weight
            state = [float(t.amount), float(t.price), 0.0, r]
            self.replay.add(
                state,
                str(action_val),
                reward,
                getattr(t, "emotion", ""),
                regime_label,
            )
            seq.append(float(t.price))

    def _sample_action(self, states: torch.Tensor, deterministic: bool = False) -> tuple[torch.Tensor, torch.Tensor]:
        actor_net = self._jit.actor if self._jit is not None and hasattr(self._jit, "actor") else self.actor
        mean, log_std = actor_net(states)
        log_std = torch.clamp(log_std, -20, 2)
        std = log_std.exp()
        if deterministic:
            z = mean
        else:
            z = mean + std * torch.randn_like(mean)
        action = torch.tanh(z)
        log_prob = -0.5 * ((z - mean) / (std + 1e-6)) ** 2 - log_std - torch.log(2 - 2 * action.pow(2) + 1e-6)
        log_prob = log_prob.sum(-1)
        return action.squeeze(-1), log_prob

    # --------------------------------------------------------------
    async def train(self, regime: str | None = None) -> None:
        await self._log_trades()
        batch = self.replay.sample(64, regime=regime)
        if not batch:
            return
        states = torch.tensor([b[0] for b in batch], dtype=torch.float32, device=self.device)
        actions = torch.tensor([float(b[1]) for b in batch], dtype=torch.float32, device=self.device)
        rewards = torch.tensor([b[2] for b in batch], dtype=torch.float32, device=self.device)
        with torch.no_grad():
            next_states = states
            next_actions, next_logp = self._sample_action(next_states)
            tq1 = self.tq1(next_states, next_actions)
            tq2 = self.tq2(next_states, next_actions)
            target_v = torch.min(tq1, tq2) - self._alpha * next_logp
            target_q = rewards + self.gamma * target_v
        q1 = self.q1(states, actions)
        q2 = self.q2(states, actions)
        q1_loss = F.mse_loss(q1, target_q)
        q2_loss = F.mse_loss(q2, target_q)
        self.q1_opt.zero_grad(); q1_loss.backward(); self.q1_opt.step()
        self.q2_opt.zero_grad(); q2_loss.backward(); self.q2_opt.step()

        new_actions, logp = self._sample_action(states)
        q1_pi = self.q1(states, new_actions)
        q2_pi = self.q2(states, new_actions)
        q_pi = torch.min(q1_pi, q2_pi)
        actor_loss = (self._alpha * logp - q_pi).mean()
        self.actor_opt.zero_grad(); actor_loss.backward(); self.actor_opt.step()

        alpha_loss = -(self.log_alpha * (logp + self.target_entropy).detach()).mean()
        self.alpha_opt.zero_grad(); alpha_loss.backward(); self.alpha_opt.step()
        self._alpha = float(self.log_alpha.exp())

        for tp, p in zip(self.tq1.parameters(), self.q1.parameters()):
            tp.data.mul_(1 - self.tau); tp.data.add_(self.tau * p.data)
        for tp, p in zip(self.tq2.parameters(), self.q2.parameters()):
            tp.data.mul_(1 - self.tau); tp.data.add_(self.tau * p.data)

        self._fitted = True
        torch.save(
            {
                "actor": self.actor.cpu().state_dict(),
                "q1": self.q1.cpu().state_dict(),
                "q2": self.q2.cpu().state_dict(),
                "tq1": self.tq1.cpu().state_dict(),
                "tq2": self.tq2.cpu().state_dict(),
                "actor_opt": self.actor_opt.state_dict(),
                "q1_opt": self.q1_opt.state_dict(),
                "q2_opt": self.q2_opt.state_dict(),
                "alpha_opt": self.alpha_opt.state_dict(),
                "log_alpha": self.log_alpha.detach().cpu(),
            },
            self.model_path,
        )
        self.actor.to(self.device)
        self.q1.to(self.device)
        self.q2.to(self.device)
        self.tq1.to(self.device)
        self.tq2.to(self.device)
        self._last_mtime = os.path.getmtime(self.model_path)

    async def _online_loop(self, interval: float = 60.0) -> None:
        while True:
            try:
                await self.train()
            except Exception as exc:  # pragma: no cover - logging
                self._logger.error("online train failed: %s", exc)
            await asyncio.sleep(interval)

    def start_online_learning(self, interval: float = 60.0) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._online_loop(interval))

    # --------------------------------------------------------------
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
        action, _ = self._sample_action(state, deterministic=True)
        val = float(action.item())
        val = max(min(val, 1.0), -1.0)
        side = "buy" if val >= 0 else "sell"
        amount = abs(val)
        if side == "buy":
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
                    "amount": amount,
                    "price": price,
                    "agent": self.name,
                }
            ]
        position = portfolio.balances.get(token)
        if position:
            amt = min(amount, float(position.amount))
            if amt > 0:
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
                        "amount": amt,
                        "price": price,
                        "agent": self.name,
                    }
                ]
        return []

    def close(self) -> None:
        if getattr(self, "_rl_sub", None):
            self._rl_sub.__exit__(None, None, None)
