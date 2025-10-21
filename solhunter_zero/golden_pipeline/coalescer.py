"""Snapshot coalescer producing Golden Snapshots."""

from __future__ import annotations

import asyncio
import copy
from typing import Awaitable, Callable, Dict

from .contracts import golden_hash_key
from .kv import KeyValueStore
from .types import DepthSnapshot, GoldenSnapshot, OHLCVBar, PriceSnapshot, TokenSnapshot
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
        self._prices: Dict[str, PriceSnapshot] = {}
        self._hash_cache: Dict[str, str] = {}
        self._lock = asyncio.Lock()
        self._kv = kv
        self._hash_ttl = hash_ttl
        self._price_stale_threshold_ms = 2_000.0

    async def update_metadata(self, snapshot: TokenSnapshot) -> None:
        async with self._lock:
            self._meta[snapshot.mint] = snapshot
            await self._maybe_emit(snapshot.mint)

    async def update_bar(self, bar: OHLCVBar) -> None:
        async with self._lock:
            self._bars[bar.mint] = bar
            await self._maybe_emit(bar.mint)

    async def update_depth(self, depth: DepthSnapshot) -> None:
        async with self._lock:
            self._depth[depth.mint] = depth
            await self._maybe_emit(depth.mint)

    async def update_price(self, snapshot: PriceSnapshot) -> None:
        async with self._lock:
            self._prices[snapshot.mint] = snapshot
            await self._maybe_emit(snapshot.mint)

    async def _maybe_emit(self, mint: str) -> None:
        meta = self._meta.get(mint)
        bar = self._bars.get(mint)
        depth = self._depth.get(mint)
        if not (meta and bar and depth):
            return
        now = now_ts()
        price = self._prices.get(mint)
        px_sources: list[dict[str, object]] = []
        px_diag: dict[str, object] | None = None
        px_mid = depth.mid_usd
        px_spread = depth.spread_bps
        px_asof = depth.asof
        if price is not None:
            px_sources = [dict(entry) for entry in price.providers]
            px_diag = dict(price.diagnostics or {})
            alerts = list(px_diag.get("alerts", [])) if isinstance(px_diag.get("alerts"), list) else list()
            staleness_ms = max(0.0, (now - float(price.asof)) * 1000.0)
            px_diag["staleness_ms"] = staleness_ms
            px_asof = price.asof
            if price.mid_usd is not None:
                px_mid = float(price.mid_usd)
            else:
                alerts.append("price.depth_fallback")
                px_diag["used_depth_fallback"] = True
            if price.spread_bps is not None:
                px_spread = float(price.spread_bps)
            if staleness_ms > self._price_stale_threshold_ms:
                alerts.append("price.stale")
            px_diag["alerts"] = sorted({str(alert) for alert in alerts if alert})
        else:
            px_diag = {
                "alerts": ["price.missing", "price.depth_fallback"],
                "used_depth_fallback": True,
                "staleness_ms": None,
            }
        payload = {
            "mint": mint,
            "meta": {
                "symbol": meta.symbol,
                "decimals": meta.decimals,
                "token_program": meta.token_program,
                "asof": meta.asof,
            },
            "px": {
                "mid_usd": px_mid,
                "spread_bps": px_spread,
                "sources": px_sources,
                "diagnostics": px_diag or {},
                "asof": px_asof,
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
        hash_payload = copy.deepcopy(payload)
        diagnostics = hash_payload.get("px", {}).get("diagnostics")
        if isinstance(diagnostics, dict):
            diagnostics.pop("staleness_ms", None)
        snapshot_hash = canonical_hash(hash_payload)
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
