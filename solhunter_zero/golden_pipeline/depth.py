"""Depth aggregation for Golden Snapshots."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Iterable

from .types import DepthSnapshot


class DepthStage:
    """Aggregate venue depth into a per-mint snapshot."""

    def __init__(self, emit: Callable[[DepthSnapshot], Awaitable[None]]) -> None:
        self._emit = emit
        self._by_mint: Dict[str, Dict[str, DepthSnapshot]] = defaultdict(dict)
        self._lock = asyncio.Lock()
        self._last_emit: Dict[str, DepthSnapshot] = {}

    async def submit(self, snapshot: DepthSnapshot) -> None:
        async with self._lock:
            per_mint = self._by_mint[snapshot.mint]
            per_mint[snapshot.venue] = snapshot
            aggregate = self._aggregate(snapshot.mint, per_mint.values())
        if aggregate:
            previous = self._last_emit.get(snapshot.mint)
            if previous is not None and _is_redundant_depth(previous, aggregate):
                return
            self._last_emit[snapshot.mint] = aggregate
            await self._emit(aggregate)

    @staticmethod
    def _aggregate(mint: str, snapshots: Iterable[DepthSnapshot]) -> DepthSnapshot | None:
        snapshots = [snap for snap in snapshots if snap]
        if not snapshots:
            return None
        totals: Dict[str, float] = {}
        weight_sum = 0.0
        weighted_mid = 0.0
        min_spread: float | None = None
        asof = 0.0
        freshest: DepthSnapshot | None = None
        for snap in snapshots:
            asof = max(asof, snap.asof)
            min_spread = snap.spread_bps if min_spread is None else min(min_spread, snap.spread_bps)
            depth_map = snap.depth_pct or {}
            for key, value in depth_map.items():
                totals[key] = totals.get(key, 0.0) + float(value or 0.0)
            depth1 = float(depth_map.get("1", 0.0) or 0.0)
            if depth1 > 0:
                weighted_mid += snap.mid_usd * depth1
                weight_sum += depth1
            if freshest is None or snap.asof >= freshest.asof:
                freshest = snap
        if weight_sum <= 0:
            weighted_mid = sum(snap.mid_usd for snap in snapshots) / len(snapshots)
        else:
            weighted_mid /= weight_sum
        aggregate_depth = {bucket: float(value) for bucket, value in totals.items()}
        depth_bands = dict(freshest.depth_bands_usd) if freshest and freshest.depth_bands_usd else None
        return DepthSnapshot(
            mint=mint,
            venue="aggregated",
            mid_usd=weighted_mid,
            spread_bps=min_spread or 0.0,
            depth_pct=aggregate_depth,
            asof=asof,
            px_bid_usd=freshest.px_bid_usd if freshest else None,
            px_ask_usd=freshest.px_ask_usd if freshest else None,
            depth_bands_usd=depth_bands,
            degraded=freshest.degraded if freshest else False,
            source=freshest.source if freshest else None,
            route_meta=freshest.route_meta if freshest else None,
            staleness_ms=freshest.staleness_ms if freshest else None,
        )


def _depth_signature(snapshot: DepthSnapshot) -> tuple[float, float, float]:
    depth_map = snapshot.depth_pct or {}
    return (
        float(depth_map.get("1", 0.0) or 0.0),
        float(depth_map.get("2", 0.0) or 0.0),
        float(depth_map.get("5", 0.0) or 0.0),
    )


def _is_redundant_depth(previous: DepthSnapshot, current: DepthSnapshot) -> bool:
    if current.spread_bps and current.spread_bps > 0:
        return False
    if abs(current.mid_usd - previous.mid_usd) > 1e-9:
        return False
    return _depth_signature(previous) == _depth_signature(current)
