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
from typing import Any, Callable, Dict, List

from .. import event_bus
try:
    from ..loop import place_order_async
except ModuleNotFoundError:  # pragma: no cover - optional import for tests
    async def place_order_async(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError('place_order_async unavailable in this environment')
from ..memory import Memory
from ..portfolio import Portfolio

log = logging.getLogger(__name__)

SLICE_FRACTION_DEFAULT = 0.35
SLICE_FRACTION_MUST = 0.60
JITTER_LOW = 1.0
JITTER_HIGH = 3.0


class TradeExecutor:
    def __init__(
        self,
        memory: Memory,
        portfolio: Portfolio,
        *,
        dry_run: bool = False,
        testnet: bool = False,
    ) -> None:
        self.memory = memory
        self.portfolio = portfolio
        self.dry_run = bool(dry_run)
        self.testnet = bool(testnet)
        self._n = 0
        self._subscriptions: List[Callable[[], None]] = []
        self._tasks: List[asyncio.Task] = []

    def start(self) -> None:
        self._subscriptions.append(event_bus.subscribe("action_decision", self._on_decision))

    async def stop(self) -> None:
        for unsub in list(self._subscriptions):
            try:
                unsub()
            except Exception:
                pass
        self._subscriptions.clear()
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    def _build_execution_plan(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        token = str(payload.get("token") or payload.get("mint") or "").strip()
        side = str(payload.get("side", "hold")).lower()
        expected_price = float(payload.get("expected_price") or payload.get("price") or 0.0)
        max_slippage_bps = float(payload.get("max_slippage_bps") or 0.0)
        notional = payload.get("notional_usd")
        if notional is not None:
            try:
                notional = float(notional)
            except (TypeError, ValueError):
                notional = 0.0
        else:
            notional = 0.0
        size_hint = payload.get("size")
        qty = 0.0
        if size_hint is not None:
            try:
                qty = max(float(size_hint), 0.0)
            except (TypeError, ValueError):
                qty = 0.0
        if qty <= 0 and expected_price > 0 and notional:
            qty = max(notional / expected_price, 0.0)
        if side == "sell":
            pos = self.portfolio.get_position(token)
            inventory = pos.amount if pos else 0.0
            if inventory > 0:
                qty = min(qty or inventory, inventory)
        depth = 0.0
        for key in ("depth_1pct_usd", "depth_usd"):
            value = payload.get(key)
            if value is not None:
                try:
                    depth = float(value)
                    break
                except (TypeError, ValueError):
                    continue
        must_exit = bool(payload.get("must_exit"))
        remaining = max(qty, 0.0)
        slices = []
        if remaining > 0 and expected_price > 0 and depth > 0:
            first_fraction = SLICE_FRACTION_MUST if must_exit else SLICE_FRACTION_DEFAULT
            fraction = first_fraction
            while remaining > 1e-9:
                depth_qty = depth * fraction / expected_price if expected_price > 0 else remaining
                take_qty = depth_qty if depth_qty > 0 else remaining
                take_qty = min(remaining, take_qty)
                slices.append(
                    {
                        "qty": take_qty,
                        "fraction": fraction,
                        "jitter_sec": random.uniform(JITTER_LOW, JITTER_HIGH),
                    }
                )
                remaining -= take_qty
                fraction = SLICE_FRACTION_DEFAULT
        if not slices and qty > 0:
            slices.append(
                {
                    "qty": qty,
                    "fraction": 1.0,
                    "jitter_sec": random.uniform(JITTER_LOW, JITTER_HIGH),
                }
            )
        plan_notional = qty * expected_price if expected_price > 0 else notional
        return {
            "token": token,
            "side": side,
            "expected_price": expected_price,
            "total_qty": qty,
            "notional_usd": plan_notional,
            "max_slippage_bps": max_slippage_bps,
            "slices": slices,
            "must_exit": must_exit,
        }

    def _on_decision(self, payload: Dict[str, Any]) -> None:
        side = str(payload.get("side", "hold")).lower()
        if side not in {"buy", "sell"}:
            return
        token = str(payload.get("token") or payload.get("mint") or "").strip()
        if not token:
            return
        plan_payload = dict(payload)
        plan_payload["token"] = token
        plan_payload["side"] = side
        plan = self._build_execution_plan(plan_payload)
        price = plan.get("expected_price", 0.0)
        if plan["total_qty"] <= 0 or price <= 0:
            return

        async def _exec() -> None:
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
                scale = 1.0
                if ramp > 0 and self._n < ramp:
                    scale = max(0.1, (self._n + 1) / ramp)
                self._n += 1
                scaled_qty = plan["total_qty"] * scale
                notional = scaled_qty * price
                if total_val > 0 and notional > fraction_limit * total_val:
                    log.warning(
                        "circuit breaker: notional %.4f exceeds fraction %.2f of portfolio %.4f; skipping",
                        notional,
                        fraction_limit,
                        total_val,
                    )
                    return
                live_drill = str(os.getenv("LIVE_DRILL", "")).lower() in {"1", "true", "yes"}
                executed = 0.0
                scale_factor = (scaled_qty / plan["total_qty"]) if plan["total_qty"] > 0 else 0.0
                for slice_plan in plan["slices"]:
                    qty = float(slice_plan.get("qty", 0.0))
                    if qty <= 0:
                        continue
                    qty *= scale_factor if scale_factor > 0 else 1.0
                    if qty <= 0:
                        continue
                    jitter = float(slice_plan.get("jitter_sec", JITTER_LOW))
                    await asyncio.sleep(jitter)
                    action_qty = qty
                    executed += action_qty
                    action = {
                        "agent": "swarm",
                        "token": token,
                        "side": side,
                        "amount": action_qty,
                        "price": price,
                        "max_slippage_bps": plan["max_slippage_bps"],
                    }
                    simulate = live_drill or self.dry_run
                    status = "simulated" if simulate else "ok"
                    if simulate and not live_drill:
                        log.debug(
                            "TradeExecutor[%s]: dry-run skipping live order side=%s qty=%.8f price=%.8f",
                            token,
                            side,
                            action_qty,
                            price,
                        )
                    if not simulate:
                        await place_order_async(
                            token,
                            side,
                            action_qty,
                            price,
                            testnet=self.testnet,
                            dry_run=self.dry_run,
                            keypair=None,
                        )
                    await self.memory.log_trade(token=token, direction=side, amount=action_qty, price=price)
                    await self.portfolio.update_async(
                        token,
                        action_qty if side == "buy" else -action_qty,
                        price,
                    )
                    try:
                        event_bus.publish(
                            "action_executed",
                            {"action": action, "result": {"status": status}},
                        )
                    except Exception:
                        pass
                if executed <= 0:
                    log.info("TradeExecutor[%s]: no slices executed", token)
            except Exception as exc:
                log.warning("execution failed for %s %s: %s", side, token, exc)

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                task = asyncio.create_task(_exec())
                self._tasks.append(task)
                task.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)
            else:
                loop.run_until_complete(_exec())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(_exec())
            loop.close()
