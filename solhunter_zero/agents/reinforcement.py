from __future__ import annotations

import inspect
import random
import time
from collections import defaultdict
from typing import List, Dict, Any

import logging

from . import BaseAgent
from .memory import MemoryAgent
from .price_utils import resolve_price
from ..portfolio import Portfolio

logger = logging.getLogger(__name__)


class ReinforcementAgent(BaseAgent):
    """Context-free (bandit-style) Q learner over actions {'buy','sell'} per token.

    Upgrades vs. baseline:
      - Trains at most every `train_interval_s` seconds (prevents hammering storage).
      - ε-greedy with multiplicative decay toward `epsilon_floor`.
      - Normalizes reward by notional and clips extreme updates (stability).
      - Safer sizing: % of cash/position with min/max clamps.
      - Ignores malformed trades, unknown directions, and zeros.
      - Persists last processed trade id across runs (optional path).
    """

    name = "reinforcement"

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        learning_rate: float = 0.1,
        epsilon: float = 0.2,
        epsilon_decay: float = 0.995,
        epsilon_floor: float = 0.02,
        discount: float = 0.95,  # kept for compatibility; unused in bandit
        train_interval_s: float = 1.0,  # throttle training calls
        min_notional: float = 10.0,  # smallest order value
        buy_cash_frac: float = 0.05,  # allocate 5% of free cash per buy
        sell_pos_frac: float = 1.0,  # sell up to 100% of position
        q_clip: float = 5.0,  # clip per-update target to +/- q_clip
        state_path: str | None = None,  # optional file to persist _last_id & ε
    ) -> None:
        self.memory_agent = memory_agent or MemoryAgent()
        self.learning_rate = float(learning_rate)
        self.epsilon = float(epsilon)
        self.epsilon_decay = float(epsilon_decay)
        self.epsilon_floor = float(epsilon_floor)
        self.discount = float(discount)
        self.train_interval_s = float(train_interval_s)
        self.min_notional = float(min_notional)
        self.buy_cash_frac = float(buy_cash_frac)
        self.sell_pos_frac = float(sell_pos_frac)
        self.q_clip = float(q_clip)
        self.state_path = state_path

        # Q[token] = {'buy': q, 'sell': q}
        self.q: Dict[str, Dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
        self._last_id: int = 0
        self._last_train_ts: float = 0.0

        self._load_state()

    # ---------- internal persistence ----------

    def _load_state(self) -> None:
        if not self.state_path:
            return
        try:
            import json, os

            if os.path.exists(self.state_path):
                with open(self.state_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                self._last_id = int(data.get("last_id", 0))
                self.epsilon = float(data.get("epsilon", self.epsilon))
        except Exception:
            logger.debug("ReinforcementAgent: failed to load state", exc_info=True)

    def _save_state(self) -> None:
        if not self.state_path:
            return
        try:
            import json, os

            tmp = f"{self.state_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump({"last_id": self._last_id, "epsilon": self.epsilon}, fh)
            os.replace(tmp, self.state_path)
        except Exception:
            logger.debug("ReinforcementAgent: failed to save state", exc_info=True)

    # ---------- learning ----------

    async def train(self) -> None:
        """Update Q-values from new trades since `_last_id`, at most once per interval."""
        now = time.time()
        if now - self._last_train_ts < self.train_interval_s:
            return

        loader = getattr(self.memory_agent.memory, "list_trades", None)
        if loader is None:
            return

        trades = loader(since_id=self._last_id)
        if inspect.isawaitable(trades):  # type: ignore[attr-defined]
            trades = await trades

        # Aggregate realized PnL per token (USD) and total notional to normalize
        pnl: Dict[str, float] = defaultdict(float)
        notional: Dict[str, float] = defaultdict(float)
        max_id = self._last_id

        for t in trades or []:
            try:
                direction = str(getattr(t, "direction", "")).lower()
                token = str(getattr(t, "token", ""))
                amount = float(getattr(t, "amount", 0.0))
                price = float(getattr(t, "price", 0.0))
                tid = int(getattr(t, "id", max_id))
            except Exception:
                continue

            if not token or amount <= 0 or price <= 0:
                continue
            if direction not in {"buy", "sell"}:
                continue

            value = amount * price
            notional[token] += value
            pnl[token] += (-value if direction == "buy" else value)
            if tid > max_id:
                max_id = tid

        # Apply bandit-style update per token
        for token, reward_abs in pnl.items():
            # Normalize by notional so different tokens update comparably
            denom = max(1e-9, notional.get(token, 0.0))
            reward = reward_abs / denom
            # Clip extreme updates (defensive)
            reward = max(-self.q_clip, min(self.q_clip, reward))
            q = self.q[token]
            # Move 'buy' Q toward +reward, 'sell' Q toward -reward
            q["buy"] += self.learning_rate * (reward - q["buy"])
            q["sell"] += self.learning_rate * (-reward - q["sell"])

        if max_id > self._last_id:
            self._last_id = max_id
            self._save_state()

        # ε-decay (bounded)
        self.epsilon = max(self.epsilon_floor, self.epsilon * self.epsilon_decay)
        self._last_train_ts = now

    # ---------- trading ----------

    def _size_buy(self, price: float, portfolio: Portfolio) -> float:
        # Best-effort: use free cash if portfolio exposes it; else fixed notional / price
        free_cash = float(getattr(portfolio, "free_cash", 0.0) or 0.0)
        if free_cash > 0:
            notional = max(self.min_notional, free_cash * self.buy_cash_frac)
        else:
            notional = self.min_notional
        qty = notional / max(price, 1e-9)
        return max(0.0, qty)

    def _size_sell(self, token: str, portfolio: Portfolio) -> float:
        pos = getattr(portfolio, "balances", {}).get(token)
        amount = float(getattr(pos, "amount", 0.0) or 0.0)
        return max(0.0, amount * self.sell_pos_frac)

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        await self.train()
        q = self.q[token]
        # ε-greedy choose action
        if random.random() < self.epsilon:
            action = random.choice(["buy", "sell"])
        else:
            action = "buy" if q["buy"] >= q["sell"] else "sell"

        # Resolve price once; abort on missing price
        price, context = await resolve_price(token, portfolio)
        if price <= 0:
            logger.info(
                "%s: skipping %s for %s (no price) ctx=%s",
                self.name,
                action,
                token,
                context,
            )
            return []

        if action == "buy":
            qty = self._size_buy(price, portfolio)
            if qty <= 0:
                return []
            return [
                {
                    "token": token,
                    "side": "buy",
                    "amount": qty,
                    "price": price,
                    "reason": self.name,
                    "context": {"q": q, "epsilon": self.epsilon},
                }
            ]

        # sell path
        qty = self._size_sell(token, portfolio)
        if qty <= 0:
            return []
        return [
            {
                "token": token,
                "side": "sell",
                "amount": qty,
                "price": price,
                "reason": self.name,
                "context": {"q": q, "epsilon": self.epsilon},
            }
        ]
