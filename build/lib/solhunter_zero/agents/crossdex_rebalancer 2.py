from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from typing import List, Dict, Any
import logging

from . import BaseAgent
from .portfolio_optimizer import PortfolioOptimizer
from .execution import ExecutionAgent
from ..depth_client import snapshot
from ..event_bus import subscribe
from ..arbitrage import (
    _prepare_service_tx,
    VENUE_URLS,
    DEX_BASE_URL,
    DEX_FEES,
    DEX_LATENCY,
)
from .. import routeffi as _routeffi
from ..mev_executor import MEVExecutor
from ..portfolio import Portfolio
from ..util import parse_bool_env

logger = logging.getLogger(__name__)


class CrossDEXRebalancer(BaseAgent):
    """Rebalance trades across DEX venues based on order book liquidity."""

    name = "crossdex_rebalancer"

    def __init__(
        self,
        optimizer: PortfolioOptimizer | None = None,
        executor: ExecutionAgent | None = None,
        *,
        rebalance_interval: int = 30,
        slippage_threshold: float = 0.05,
        use_mev_bundles: bool = False,
        use_depth_feed: bool | None = None,
        latency_weight: float = 1.0,
        fee_weight: float = 1.0,
    ) -> None:
        self.optimizer = optimizer or PortfolioOptimizer()
        self.executor = executor or ExecutionAgent(rate_limit=0)
        self.rebalance_interval = int(rebalance_interval)
        self.slippage_threshold = float(slippage_threshold)
        self.use_mev_bundles = bool(use_mev_bundles)
        self.latency_weight = float(latency_weight)
        self.fee_weight = float(fee_weight)
        self._last = 0.0
        stream_enabled = parse_bool_env("USE_DEPTH_STREAM", True)
        if use_depth_feed is None:
            use_depth_feed = stream_enabled or parse_bool_env("USE_DEPTH_FEED", False)
        else:
            use_depth_feed = bool(use_depth_feed) or stream_enabled
        self.use_depth_feed = bool(use_depth_feed)
        self._depth_cache: Dict[str, Dict[str, Dict[str, float]]] = {}
        self._depth_window: Dict[str, Dict[str, Dict[str, deque]]] = {}
        self._unsub = None
        self._fees: Dict[str, float] = dict(DEX_FEES)
        self._latency: Dict[str, float] = {}
        self._latency_updates: Dict[str, float] = {}
        self._latency_unsub = None
        if self.use_depth_feed:
            self._unsub = subscribe("depth_update", self._handle_depth)
        self._latency_unsub = subscribe("dex_latency_update", self._handle_latency)
        if not _routeffi.is_routeffi_available():
            logger.warning(
                "Route FFI library not available; rebalancing will use a Python fallback."
            )

    # ------------------------------------------------------------------
    def _best_order(
        self, side: str, amount: float, depth: Dict[str, Dict[str, float]]
    ) -> List[str] | None:
        """Return an ordered list of venues using the FFI if available."""
        if not _routeffi.is_routeffi_available():
            return None
        key = "asks" if side == "buy" else "bids"
        prices = {
            v: float(info.get(key, 0.0))
            for v, info in depth.items()
            if float(info.get(key, 0.0)) > 0
        }
        if len(prices) < 2:
            return None
        func = (
            _routeffi.best_route_parallel
            if _routeffi.parallel_enabled()
            else _routeffi.best_route
        )
        try:
            res = func(
                prices,
                amount,
                fees=self._fees,
                latency=self._latency,
                max_hops=len(prices),
            )
        except Exception:
            return None
        if not res:
            return None
        path, _ = res
        return path or None

    # ------------------------------------------------------------------
    def _handle_depth(self, payload: Dict[str, Dict[str, Any]]) -> None:
        for token, entry in payload.items():
            dex_map = entry.get("dex") or {
                k: v for k, v in entry.items() if isinstance(v, dict)
            }
            token_cache = self._depth_cache.setdefault(token, {})
            token_window = self._depth_window.setdefault(token, {})
            for venue, info in dex_map.items():
                bids = float(info.get("bids", 0.0))
                asks = float(info.get("asks", 0.0))
                win = token_window.setdefault(
                    venue,
                    {"bids": deque(maxlen=5), "asks": deque(maxlen=5)},
                )
                win["bids"].append(bids)
                win["asks"].append(asks)
                avg_bids = sum(win["bids"]) / len(win["bids"])
                avg_asks = sum(win["asks"]) / len(win["asks"])
                token_cache[venue] = {"bids": avg_bids, "asks": avg_asks}

    def _handle_latency(self, payload: Dict[str, Any]) -> None:
        for venue, val in payload.items():
            try:
                self._latency_updates[venue] = float(val)
            except Exception:
                pass

    def close(self) -> None:
        if self._unsub:
            self._unsub()
        if self._latency_unsub:
            self._latency_unsub()

    async def _ensure_latency(self) -> None:
        if self._latency_updates:
            self._latency.update(self._latency_updates)
            self._latency_updates.clear()
        if not self._latency:
            self._latency.update(DEX_LATENCY)

    # ------------------------------------------------------------------
    async def _split_action(
        self,
        action: Dict[str, Any],
        depth: Dict[str, Dict[str, float]],
        order: List[str] | None = None,
    ) -> List[Dict[str, Any]]:
        side = action.get("side", "buy").lower()
        amount = float(action.get("amount", 0.0))
        if amount <= 0:
            return []

        slip: Dict[str, float] = {}
        key = "asks" if side == "buy" else "bids"
        for venue, info in depth.items():
            liq = float(info.get(key, 0.0))
            if liq > 0:
                slip[venue] = amount / liq
        if not slip:
            return [action]

        valid = {v: s for v, s in slip.items() if s <= self.slippage_threshold}
        if not valid:
            best = min(slip, key=slip.get)
            valid = {best: slip[best]}

        inv: Dict[str, float] = {}
        costs: Dict[str, float] = {}
        for v, s in valid.items():
            cost = s
            cost += self.latency_weight * self._latency.get(v, 0.0)
            cost += self.fee_weight * self._fees.get(v, 0.0)
            inv[v] = 1.0 / max(cost, 1e-9)
            costs[v] = cost

        total = sum(inv.values())
        venues = list(inv.keys())
        if order:
            order = [v for v in order if v in venues]
            order.extend([v for v in venues if v not in order])
            venues = order
        else:
            venues = sorted(venues, key=lambda v: costs[v])

        actions: List[Dict[str, Any]] = []
        for venue in venues:
            inv_w = inv[venue]
            amt = amount * inv_w / total
            new = dict(action)
            new["venue"] = venue
            new["amount"] = amt
            actions.append(new)
        return actions

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        now = asyncio.get_event_loop().time()
        if now - self._last < self.rebalance_interval:
            return []
        self._last = now
        await self._ensure_latency()
        self._fees.update(DEX_FEES)

        base_actions = await self.optimizer.propose_trade(
            token, portfolio, depth=depth, imbalance=imbalance
        )
        if not base_actions:
            return []

        if self.use_depth_feed:
            depth_data = self._depth_cache.get(token)
            if not depth_data:
                depth_data, _ = snapshot(token)
        else:
            depth_data, _ = snapshot(token)
        all_actions: List[Dict[str, Any]] = []
        for act in base_actions:
            order = self._best_order(act.get("side", "buy"), act.get("amount", 0.0), depth_data)
            all_actions.extend(await self._split_action(act, depth_data, order=order))

        if self.use_mev_bundles:
            txs: List[str] = []
            for act in all_actions:
                venue = str(act.get("venue", ""))
                base = VENUE_URLS.get(venue, DEX_BASE_URL)
                tx = await _prepare_service_tx(
                    act["token"],
                    act["side"],
                    act.get("amount", 0.0),
                    act.get("price", 0.0),
                    base,
                )
                if tx:
                    txs.append(tx)
            if txs:
                mev = MEVExecutor(
                    token,
                    priority_rpc=getattr(self.executor, "priority_rpc", None),
                )
                await mev.submit_bundle(txs)
                return [{"bundle": True, "count": len(txs)}]
            return []

        results = []
        for act in all_actions:
            res = await self.executor.execute(act)
            results.append(res)
        return results
