from __future__ import annotations

import asyncio
import os
from typing import List, Dict, Any, Sequence, Callable, Awaitable, Mapping

from . import BaseAgent
from .. import arbitrage
from ..arbitrage import _best_route, refresh_costs
from ..event_bus import subscribe
from ..depth_client import snapshot as depth_snapshot
from ..portfolio import Portfolio

PriceFeed = Callable[[str], Awaitable[float]]


class ArbitrageAgent(BaseAgent):
    """Detect arbitrage opportunities between DEX price feeds."""

    name = "arbitrage"

    def __init__(
        self,
        threshold: float = 0.0,
        amount: float = 1.0,
        feeds: Mapping[str, PriceFeed] | Sequence[PriceFeed] | None = None,
        backup_feeds: Mapping[str, PriceFeed] | Sequence[PriceFeed] | None = None,
        *,
        fees: Mapping[str, float] | None = None,
        gas: Mapping[str, float] | None = None,
        latency: Mapping[str, float] | None = None,
        gas_multiplier: float | None = None,
    ):
        self.threshold = threshold
        self.amount = amount
        if feeds is None:
            self.feeds: Dict[str, PriceFeed] = {
                "orca": arbitrage.fetch_orca_price_async,
                "raydium": arbitrage.fetch_raydium_price_async,
                "jupiter": arbitrage.fetch_jupiter_price_async,
            }
        elif isinstance(feeds, Mapping):
            self.feeds = dict(feeds)
        else:
            self.feeds = {
                getattr(f, "__name__", f"feed{i}"): f for i, f in enumerate(feeds)
            }
        env_fees, env_gas, env_lat = refresh_costs()
        self.fees = dict(env_fees)
        if fees:
            self.fees.update(fees)
        self.gas = dict(env_gas)
        if gas:
            self.gas.update(gas)
        self.latency = dict(env_lat)
        if latency:
            self.latency.update(latency)
        if gas_multiplier is not None:
            self.gas_multiplier = float(gas_multiplier)
        else:
            self.gas_multiplier = float(os.getenv("GAS_MULTIPLIER", "1.0"))
        if backup_feeds is None:
            self.backup_feeds: Dict[str, PriceFeed] | None = None
        elif isinstance(backup_feeds, Mapping):
            self.backup_feeds = dict(backup_feeds)
        else:
            self.backup_feeds = {
                getattr(f, "__name__", f"backup{i}"): f
                for i, f in enumerate(backup_feeds)
            }
        # Cache of latest known prices per token and feed
        self.price_cache: Dict[str, Dict[str, float]] = {}
        self._unsub_price = subscribe("price_update", self._handle_price)

    # ------------------------------------------------------------------
    def _handle_price(self, payload: Mapping[str, Any]) -> None:
        token = payload.get("token")
        venue = payload.get("venue")
        price = payload.get("price")
        if not token or not venue:
            return
        try:
            value = float(price)
        except Exception:
            return
        self.price_cache.setdefault(str(token), {})[str(venue)] = value

    def close(self) -> None:
        if self._unsub_price:
            self._unsub_price()

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        if depth is not None and depth <= 0:
            return []
        token_cache = self.price_cache.setdefault(token, {})

        valid: Dict[str, float] = {
            n: p for n, p in token_cache.items() if isinstance(p, (int, float)) and p > 0
        }

        if len(valid) < 2 and self.feeds:
            names = list(self.feeds.keys())
            prices = await asyncio.gather(*(f(token) for f in self.feeds.values()))
            for name, price in zip(names, prices):
                if price > 0:
                    token_cache[name] = price
                    valid[name] = price
                elif name in token_cache:
                    valid[name] = token_cache[name]

        # If not enough data, try backup feeds
        if len(valid) < 2 and self.backup_feeds:
            b_names = list(self.backup_feeds.keys())
            b_prices = await asyncio.gather(
                *(f(token) for f in self.backup_feeds.values())
            )
            for b_name, b_price in zip(b_names, b_prices):
                if b_price > 0:
                    token_cache[b_name] = b_price
                    valid[b_name] = b_price
                elif b_name in token_cache:
                    valid[b_name] = token_cache[b_name]

        # If still not enough data, give up
        if len(valid) < 2:
            return []

        env_fees, env_gas, env_lat = refresh_costs()
        self.fees.update(env_fees)
        self.gas.update(env_gas)
        self.latency.update(env_lat)

        gas_costs = {k: v * self.gas_multiplier for k, v in self.gas.items()}
        depth_map, _ = depth_snapshot(token)
        path, profit = _best_route(
            valid,
            self.amount,
            token=token,
            fees=self.fees,
            gas=gas_costs,
            latency=self.latency,
            depth=depth_map,
        )

        if not path or profit <= 0:
            return []

        actions = []
        for i in range(len(path) - 1):
            buy = path[i]
            sell = path[i + 1]
            actions.append(
                {
                    "token": token,
                    "side": "buy",
                    "amount": self.amount,
                    "price": valid[buy],
                    "venue": buy,
                }
            )
            actions.append(
                {
                    "token": token,
                    "side": "sell",
                    "amount": self.amount,
                    "price": valid[sell],
                    "venue": sell,
                }
            )
        return actions
