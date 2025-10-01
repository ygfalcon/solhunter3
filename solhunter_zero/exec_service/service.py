from __future__ import annotations

"""Execution service that listens for action decisions and submits orders.

This integrates with the existing exchange/portfolio/memory via the
`solhunter_zero.loop.place_order_async` and follows the same logging paths.
Enabled when EVENT_DRIVEN=1.
"""

import asyncio
import logging
import os
from typing import Any, Dict

from .. import event_bus
from ..loop import place_order_async
from ..memory import Memory
from ..portfolio import Portfolio

log = logging.getLogger(__name__)


class TradeExecutor:
    def __init__(self, memory: Memory, portfolio: Portfolio) -> None:
        self.memory = memory
        self.portfolio = portfolio
        self._task: asyncio.Task | None = None
        self._n = 0

    def start(self) -> None:
        event_bus.subscribe("action_decision", self._on_decision)

    def stop(self) -> None:
        pass

    def _on_decision(self, payload: Dict[str, Any]) -> None:
        side = str(payload.get("side", "hold"))
        if side not in {"buy", "sell"}:
            return
        token = str(payload.get("token"))
        size = float(payload.get("size", 0.0))
        if not token or size <= 0:
            return
        price = payload.get("price")
        if price is None:
            return
        price = float(price)

        async def _exec():
            try:
                fraction_limit = float(os.getenv("MAX_TRADE_FRACTION", "0.25") or 0.25)
                ramp = int(os.getenv("RAMP_TRADES_COUNT", "10") or 10)
                total_val = 0.0
                try:
                    prices = {
                        t: (
                            self.portfolio.price_history.get(t, [p.entry_price])[-1]
                            if self.portfolio.price_history.get(t)
                            else p.entry_price
                        )
                        for t, p in self.portfolio.balances.items()
                    }
                    prices[token] = price
                    total_val = self.portfolio.total_value(prices)
                except Exception:
                    total_val = 0.0
                notional = size * price
                if total_val > 0 and notional > fraction_limit * total_val:
                    log.warning(
                        "circuit breaker: notional %.4f exceeds fraction %.2f of portfolio %.4f; skipping",
                        notional,
                        fraction_limit,
                        total_val,
                    )
                    return
                scale = 1.0
                if ramp > 0 and self._n < ramp:
                    scale = max(0.1, (self._n + 1) / ramp)
                adj_size = size * scale
                self._n += 1
                live_drill = str(os.getenv("LIVE_DRILL", "")).lower() in {"1", "true", "yes"}
                action = {"agent": "swarm", "token": token, "side": side, "amount": adj_size, "price": price}
                if live_drill:
                    await self.memory.log_trade(token=token, direction=side, amount=adj_size, price=price)
                    await self.portfolio.update_async(token, adj_size if side == "buy" else -adj_size, price)
                    try:
                        event_bus.publish("action_executed", {"action": action, "result": {"status": "simulated"}})
                    except Exception:
                        pass
                else:
                    await place_order_async(token, side, adj_size, price, testnet=False, dry_run=False, keypair=None)
                    await self.memory.log_trade(token=token, direction=side, amount=adj_size, price=price)
                    await self.portfolio.update_async(token, adj_size if side == "buy" else -adj_size, price)
                    try:
                        event_bus.publish("action_executed", {"action": action, "result": {"status": "ok"}})
                    except Exception:
                        pass
            except Exception as exc:
                log.warning("execution failed for %s %s: %s", side, token, exc)

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(_exec())
            else:
                loop.run_until_complete(_exec())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_exec())
            loop.close()

