from __future__ import annotations

"""Execution service that listens for action decisions and submits orders.

This integrates with the existing exchange/portfolio/memory via the
`solhunter_zero.loop.place_order_async` and follows the same logging paths.
Enabled when EVENT_DRIVEN=1.
"""

import asyncio
import logging
import os
import random
from typing import Any, Dict, List

from .. import event_bus
from ..loop import place_order_async
from ..memory import Memory
from ..portfolio import Portfolio
from ..exit_management import DEPTH_FRACTION_CAP

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
        side = str(payload.get("side", "hold")).lower()
        if side not in {"buy", "sell"}:
            return
        token = str(payload.get("token"))
        price = payload.get("price")
        expected_price = payload.get("expected_price", price)
        max_slippage_bps = float(payload.get("max_slippage_bps", 100.0) or 100.0)
        depth1pct_usd = float(payload.get("depth1pct_usd") or payload.get("depth_1pct_usd") or 0.0)
        must_exit = bool(payload.get("must"))
        notional = float(payload.get("notional_usd") or 0.0)
        size = float(payload.get("size") or 0.0)
        if expected_price is None:
            expected_price = price
        if price is None or expected_price is None:
            return
        price = float(price)
        expected_price = float(expected_price)
        if notional <= 0 and size > 0 and expected_price > 0:
            notional = size * expected_price
        if size <= 0 and notional > 0 and expected_price > 0:
            size = notional / expected_price
        if not token or size <= 0:
            return
        notional = notional or size * expected_price

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
                notional = size * expected_price
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
                inv = self.portfolio.balances.get(token)
                if side == "sell" and inv is not None:
                    adj_size = min(adj_size, max(inv.amount, 0.0))
                    if adj_size <= 0:
                        log.debug("skip exit for %s; no inventory", token)
                        return
                slices = self._build_slices(
                    adj_size,
                    expected_price,
                    depth1pct_usd,
                    must_exit,
                )
                self._n += 1
                live_drill = str(os.getenv("LIVE_DRILL", "")).lower() in {"1", "true", "yes"}
                for idx, slice_size in enumerate(slices):
                    if slice_size <= 0:
                        continue
                    action = {
                        "agent": "swarm",
                        "token": token,
                        "side": side,
                        "amount": slice_size,
                        "price": expected_price,
                        "slice": idx,
                        "must": must_exit,
                    }
                    limit_price = self._apply_slippage_guard(
                        expected_price, max_slippage_bps, side
                    )
                    if not live_drill:
                        await place_order_async(
                            token,
                            side,
                            slice_size,
                            limit_price,
                            testnet=False,
                            dry_run=False,
                            keypair=None,
                        )
                    await self.memory.log_trade(
                        token=token,
                        direction=side,
                        amount=slice_size,
                        price=limit_price,
                    )
                    await self.portfolio.update_async(
                        token,
                        slice_size if side == "buy" else -slice_size,
                        limit_price,
                    )
                    try:
                        event_bus.publish(
                            "action_executed",
                            {"action": action, "result": {"status": "simulated" if live_drill else "ok"}},
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(random.uniform(1.0, 3.0))
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

    @staticmethod
    def _apply_slippage_guard(price: float, max_slippage_bps: float, side: str) -> float:
        if price <= 0:
            return price
        if side == "buy":
            return price * (1 + max_slippage_bps / 10_000)
        return price * (1 - max_slippage_bps / 10_000)

    def _build_slices(
        self,
        qty: float,
        price: float,
        depth1pct_usd: float,
        must_exit: bool,
    ) -> List[float]:
        if qty <= 0:
            return []
        depth_capacity = max(0.0, depth1pct_usd * DEPTH_FRACTION_CAP)
        remaining = qty
        slices: List[float] = []
        first_fraction = 0.60 if must_exit else 0.35
        default_fraction = 0.35
        idx = 0
        while remaining > 1e-9:
            fraction = first_fraction if idx == 0 else default_fraction
            target_notional = remaining * price * fraction
            allowed_notional = depth_capacity if depth_capacity > 0 else remaining * price
            notional = min(target_notional, allowed_notional, remaining * price)
            if notional <= 0:
                slices.append(remaining)
                break
            slice_qty = max(min(remaining, notional / max(price, 1e-9)), 0.0)
            if slice_qty <= 0:
                break
            slices.append(slice_qty)
            remaining -= slice_qty
            idx += 1
            if remaining * price <= 1e-6:
                break
        if remaining > 1e-9:
            slices.append(remaining)
        return slices

