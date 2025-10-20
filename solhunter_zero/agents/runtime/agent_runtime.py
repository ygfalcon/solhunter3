from __future__ import annotations

"""Event-driven agent runtime scaffolding.

This is optional and enabled when EVENT_DRIVEN=1. It wraps the existing
AgentManager so agents can propose actions via the event bus and a swarm
coordinator can consolidate them into decisions.
"""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from ... import event_bus
from ...agent_manager import AgentManager
from ...portfolio import Portfolio
from ...prices import fetch_token_prices_async

log = logging.getLogger(__name__)


@dataclass
class Proposal:
    token: str
    side: str
    size: float
    score: float
    agent: str


class AgentRuntime:
    def __init__(self, manager: AgentManager, portfolio: Portfolio) -> None:
        self.manager = manager
        self.portfolio = portfolio
        self._tasks: list[asyncio.Task] = []
        self._running = False
        self._tokens: set[str] = set()
        self._ewma: dict[str, float] = {}
        self._subscriptions: list[Callable[[], None]] = []

    async def start(self) -> None:
        self._running = True
        # Subscribe to token discovery and price updates for decision generation
        self._subscriptions.append(event_bus.subscribe("token_discovered", self._on_tokens))
        self._subscriptions.append(event_bus.subscribe("price_update", self._on_price))
        # Start price backfill loop (optional)
        if os.getenv("PRICE_BACKFILL", "1").lower() in {"1", "true", "yes"}:
            task = asyncio.create_task(self._price_backfill_loop())
            self._tasks.append(task)
            task.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)

    async def stop(self) -> None:
        self._running = False
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

    def _emit(self, p: Proposal) -> None:
        try:
            payload = {"token": p.token, "side": p.side, "size": p.size, "score": p.score, "agent": p.agent}
            event_bus.publish("action_proposal", payload)
        except Exception:
            log.exception("failed to publish action_proposal")

    def _on_tokens(self, payload: Any) -> None:
        if not self._running:
            return
        tokens = payload if isinstance(payload, (list, tuple)) else []
        for token in tokens:
            try:
                self._tokens.add(str(token))
            except Exception:
                pass
            task = asyncio.create_task(self._evaluate_and_publish(str(token)))
            self._tasks.append(task)
            task.add_done_callback(lambda t: self._tasks.remove(t) if t in self._tasks else None)

    def _on_price(self, payload: Any) -> None:
        # Update portfolio price history so agents can use volatility/correlation
        try:
            token = str(payload.get("token"))
            price = float(payload.get("price"))
            if token:
                alpha = float(os.getenv("PRICE_EWMA_ALPHA", "0.3") or 0.3)
                prev = self._ewma.get(token, price)
                # Clip outliers to 2x previous EWMA
                clipped = min(price, 2.0 * prev)
                ewma = alpha * clipped + (1 - alpha) * prev
                self._ewma[token] = ewma
                self.portfolio.record_prices({token: ewma})
        except Exception:
            pass

    async def _evaluate_and_publish(self, token: str) -> None:
        try:
            actions: List[Dict[str, Any]] = await self.manager.evaluate(token, self.portfolio)
        except Exception:
            return
        for act in actions:
            # Map evaluated actions directly to decisions; includes price/amount
            price = act.get("price")
            try:
                price = float(price)
            except (TypeError, ValueError):
                price = None

            if price is None or price <= 0:
                hist = self.portfolio.price_history.get(token, [])
                if hist:
                    try:
                        price = float(hist[-1])
                    except Exception:
                        price = None

            if (price is None or price <= 0) and token not in self._ewma:
                try:
                    prices = await fetch_token_prices_async([token])
                    fetched = float(prices.get(token, 0.0) or 0.0)
                    if fetched > 0:
                        price = fetched
                except Exception:
                    price = None

            # Prefer EWMA if available (smoother decisions)
            if token in self._ewma:
                ewma_price = float(self._ewma[token])
                if ewma_price > 0:
                    price = ewma_price

            if price is None or price <= 0:
                log.warning("dropping action due to missing price", extra={"token": token})
                continue

            decision = {
                "token": act.get("token"),
                "side": act.get("side"),
                "size": float(act.get("amount", 0.0)),
                "price": float(price),
                "rationale": {"agent": act.get("agent"), "conviction_delta": act.get("conviction_delta")},
            }
            try:
                event_bus.publish("action_decision", decision)
            except Exception:
                continue
        # Emit a small summary for UI/metrics
        try:
            buys = sum(1 for a in actions if a.get("side") == "buy")
            sells = sum(1 for a in actions if a.get("side") == "sell")
            event_bus.publish("decision_summary", {"token": token, "count": len(actions), "buys": buys, "sells": sells})
        except Exception:
            pass

    async def _price_backfill_loop(self) -> None:
        interval = float(os.getenv("PRICE_BACKFILL_INTERVAL", "10") or 10)
        while self._running:
            try:
                tokens = set(self._tokens) | set(self.portfolio.balances.keys())
                if tokens:
                    prices = await fetch_token_prices_async(list(tokens))
                    # publish price_update events for tokens we have no recent history for
                    for tok, pr in prices.items():
                        if pr:
                            try:
                                self.portfolio.record_prices({tok: float(pr)})
                                event_bus.publish("price_update", {"venue": "backfill", "token": tok, "price": float(pr)})
                            except Exception:
                                continue
            except Exception:
                pass
            await asyncio.sleep(max(1.0, interval))
