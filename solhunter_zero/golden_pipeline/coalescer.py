"""Snapshot coalescer producing Golden Snapshots."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Dict

from .contracts import golden_hash_key
from .kv import KeyValueStore
from .types import DepthSnapshot, GoldenSnapshot, OHLCVBar, TokenSnapshot
from .utils import canonical_hash, now_ts


class SnapshotCoalescer:
    """Join token metadata, OHLCV, and depth into Golden Snapshots."""

    def __init__(
        self,
        emit: Callable[[GoldenSnapshot], Awaitable[None]],
        *,
        kv: KeyValueStore | None = None,
        hash_ttl: float = 90.0,
    ) -> None:
        self._emit = emit
        self._meta: Dict[str, TokenSnapshot] = {}
        self._bars: Dict[str, OHLCVBar] = {}
        self._depth: Dict[str, DepthSnapshot] = {}
        self._hash_cache: Dict[str, str] = {}
        self._versions: Dict[str, int] = {}
        self._locks_guard = asyncio.Lock()
        self._mint_locks: Dict[str, asyncio.Lock] = {}
        self._kv = kv
        self._hash_ttl = hash_ttl

    async def update_metadata(self, snapshot: TokenSnapshot) -> None:
        await self._update_and_maybe_emit(snapshot.mint, meta=snapshot)

    async def update_bar(self, bar: OHLCVBar) -> None:
        await self._update_and_maybe_emit(bar.mint, bar=bar)

    async def update_depth(self, depth: DepthSnapshot) -> None:
        await self._update_and_maybe_emit(depth.mint, depth=depth)

    async def _get_mint_lock(self, mint: str) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._mint_locks.get(mint)
            if lock is None:
                lock = asyncio.Lock()
                self._mint_locks[mint] = lock
                self._versions.setdefault(mint, 0)
            return lock

    async def _update_and_maybe_emit(
        self,
        mint: str,
        *,
        meta: TokenSnapshot | None = None,
        bar: OHLCVBar | None = None,
        depth: DepthSnapshot | None = None,
    ) -> None:
        lock = await self._get_mint_lock(mint)
        async with lock:
            if meta is not None:
                self._meta[mint] = meta
            if bar is not None:
                self._bars[mint] = bar
            if depth is not None:
                self._depth[mint] = depth
            current_meta = self._meta.get(mint)
            current_bar = self._bars.get(mint)
            current_depth = self._depth.get(mint)
            version = self._versions.get(mint, 0) + 1
            self._versions[mint] = version
        await self._maybe_emit(
            mint,
            current_meta,
            current_bar,
            current_depth,
            version,
            lock,
        )

    async def _maybe_emit(
        self,
        mint: str,
        meta: TokenSnapshot | None,
        bar: OHLCVBar | None,
        depth: DepthSnapshot | None,
        version: int,
        lock: asyncio.Lock,
    ) -> None:
        if not (meta and bar and depth):
            return
        now = now_ts()
        payload = {
            "mint": mint,
            "meta": {
                "symbol": meta.symbol,
                "decimals": meta.decimals,
                "token_program": meta.token_program,
                "asof": meta.asof,
            },
            "px": {
                "mid_usd": depth.mid_usd,
                "spread_bps": depth.spread_bps,
            },
            "liq": {
                "depth_pct": depth.depth_pct,
                "asof": depth.asof,
            },
            "ohlcv5m": {
                "o": bar.open,
                "h": bar.high,
                "l": bar.low,
                "c": bar.close,
                "vol_usd": bar.vol_usd,
                "buyers": bar.buyers,
                "flow_usd": bar.flow_usd,
                "zret": bar.zret,
                "zvol": bar.zvol,
                "asof_close": bar.asof_close,
            },
        }
        snapshot_hash = canonical_hash(payload)
        async with lock:
            if self._versions.get(mint, 0) != version:
                return
            if self._hash_cache.get(mint) == snapshot_hash:
                return
            self._hash_cache[mint] = snapshot_hash
        if self._kv:
            key = golden_hash_key(mint)
            cached = await self._kv.get(key)
            if cached == snapshot_hash:
                return
            await self._kv.set(key, snapshot_hash, ttl=self._hash_ttl)
        asof = max(meta.asof, depth.asof, bar.asof_close)
        latency_ms = max(0.0, (now - asof) * 1000.0)
        depth_staleness_ms = max(0.0, (now - depth.asof) * 1000.0)
        candle_age_ms = max(0.0, (now - bar.asof_close) * 1000.0)
        payload["liq"]["staleness_ms"] = depth_staleness_ms
        golden = GoldenSnapshot(
            mint=mint,
            asof=asof,
            meta=payload["meta"],
            px=payload["px"],
            liq=payload["liq"],
            ohlcv5m=payload["ohlcv5m"],
            hash=snapshot_hash,
            metrics={
                "emitted_at": now,
                "latency_ms": latency_ms,
                "depth_staleness_ms": depth_staleness_ms,
                "candle_age_ms": candle_age_ms,
            },
        )
        await self._emit(golden)
