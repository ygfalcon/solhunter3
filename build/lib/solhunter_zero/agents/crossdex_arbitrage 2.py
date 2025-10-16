from __future__ import annotations

import asyncio
import time
from typing import (
    List,
    Dict,
    Any,
    Mapping,
    Sequence,
    Callable,
    Awaitable,
    AsyncGenerator,
)

from . import BaseAgent
from ..arbitrage import (
    DEX_FEES,
    DEX_GAS,
    DEX_LATENCY,
    VENUE_URLS,
    stream_orca_prices,
    stream_raydium_prices,
    stream_jupiter_prices,
    stream_phoenix_prices,
    stream_meteora_prices,
)
from ..event_bus import subscribe
from .. import routeffi as _routeffi
from ..portfolio import Portfolio
from ..util import parse_bool_env

PriceFeed = Callable[[str], Awaitable[float]]


class CrossDEXArbitrage(BaseAgent):
    """Search multi-hop arbitrage paths across DEX venues."""

    name = "crossdex_arbitrage"

    def __init__(
        self,
        threshold: float = 0.0,
        amount: float = 1.0,
        feeds: Mapping[str, PriceFeed] | Sequence[PriceFeed] | None = None,
        *,
        max_hops: int = 4,
        use_price_streams: bool | None = None,
    ) -> None:
        self.threshold = float(threshold)
        self.amount = float(amount)
        if feeds is None:
            from .. import arbitrage as mod

            self.feeds: Dict[str, PriceFeed] = {
                "orca": mod.fetch_orca_price_async,
                "raydium": mod.fetch_raydium_price_async,
                "jupiter": mod.fetch_jupiter_price_async,
            }
        elif isinstance(feeds, Mapping):
            self.feeds = dict(feeds)
        else:
            self.feeds = {
                getattr(f, "__name__", f"feed{i}"): f for i, f in enumerate(feeds)
            }
        self.max_hops = int(max_hops)
        self._fees: Dict[str, float] = dict(DEX_FEES)
        self._gas: Dict[str, float] = dict(DEX_GAS)
        self._latency: Dict[str, float] = dict(DEX_LATENCY)
        self._latency_updates: Dict[str, float] = {}
        self._latency_unsub = subscribe("dex_latency_update", self._handle_latency)

        stream_env = parse_bool_env("USE_PRICE_STREAMS", False)
        if use_price_streams is None:
            use_price_streams = stream_env
        else:
            use_price_streams = bool(use_price_streams) or stream_env
        self.use_price_streams = bool(use_price_streams)

        self._price_cache: Dict[str, Dict[str, float]] = {}
        self._stream_tasks: Dict[tuple[str, str], asyncio.Task] = {}
        self._stream_stats: Dict[tuple[str, str], Dict[str, float]] = {}
        self._stream_funcs: Dict[str, Callable[[str], AsyncGenerator[float, None]]] = {
            "orca": stream_orca_prices,
            "raydium": stream_raydium_prices,
            "jupiter": stream_jupiter_prices,
            "phoenix": stream_phoenix_prices,
            "meteora": stream_meteora_prices,
        }

    def close(self) -> None:
        for task in list(self._stream_tasks.values()):
            task.cancel()
        if self._latency_unsub:
            self._latency_unsub()

    def _handle_latency(self, payload: Mapping[str, Any]) -> None:
        for venue, val in payload.items():
            try:
                self._latency_updates[venue] = float(val)
            except Exception:
                pass

    # ------------------------------------------------------------------
    def _update_price(self, token: str, venue: str, price: float) -> None:
        token_cache = self._price_cache.setdefault(token, {})
        token_cache[venue] = float(price)
        now = time.monotonic()
        key = (token, venue)
        stats = self._stream_stats.setdefault(
            key, {"last": now, "interval_sum": 0.0, "count": 0}
        )
        last = stats.get("last")
        if last is not None:
            interval = now - last
            stats["interval_sum"] += interval
            stats["count"] += 1
        stats["last"] = now

    def _start_stream(self, token: str, venue: str) -> None:
        if not self.use_price_streams:
            return
        key = (token, venue)
        if key in self._stream_tasks:
            return
        fn = self._stream_funcs.get(venue)
        if fn is None:
            return

        async def _runner() -> None:
            try:
                async for price in fn(token):
                    self._update_price(token, venue, price)
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        self._stream_tasks[key] = asyncio.create_task(_runner())

    async def _ensure_latency(self) -> None:
        if self._latency_updates:
            self._latency.update(self._latency_updates)
            self._latency_updates.clear()
        if not self._latency:
            self._latency.update(DEX_LATENCY)

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        await self._ensure_latency()

        token_cache = self._price_cache.setdefault(token, {})
        price_map: Dict[str, float] = {
            n: p for n, p in token_cache.items() if p > 0
        } if self.use_price_streams else {}

        for name in self.feeds.keys():
            if self.use_price_streams:
                self._start_stream(token, name)
            if name not in price_map:
                price = await self.feeds[name](token)
                if price > 0:
                    price_map[name] = price
                    token_cache[name] = price

        if len(price_map) < 2:
            return []

        func = _routeffi.best_route_parallel if _routeffi.parallel_enabled() else _routeffi.best_route
        res = func(
            price_map,
            self.amount,
            fees=self._fees,
            gas=self._gas,
            latency=self._latency,
            max_hops=self.max_hops,
        )
        if not res:
            return []
        path, profit = res
        if not path or profit <= 0:
            return []

        actions: List[Dict[str, Any]] = []
        for i in range(len(path) - 1):
            buy = path[i]
            sell = path[i + 1]
            actions.append({"token": token, "side": "buy", "amount": self.amount, "price": price_map[buy], "venue": buy})
            actions.append({"token": token, "side": "sell", "amount": self.amount, "price": price_map[sell], "venue": sell})
        return actions

    # ------------------------------------------------------------------
    @property
    def metrics(self) -> Dict[str, Dict[str, float]]:
        """Return average update interval per venue and token."""

        data: Dict[str, Dict[str, float]] = {}
        for (token, venue), st in self._stream_stats.items():
            cnt = st.get("count", 0)
            if cnt:
                avg = st["interval_sum"] / cnt
                data.setdefault(token, {})[venue] = avg
        return data
