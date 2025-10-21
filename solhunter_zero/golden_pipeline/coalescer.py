"""Snapshot coalescer producing Golden Snapshots."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict

from .contracts import golden_hash_key
from .kv import KeyValueStore
from .types import (
    DepthSnapshot,
    GOLDEN_SNAPSHOT_SCHEMA_VERSION,
    GoldenSnapshot,
    OHLCVBar,
    TokenSnapshot,
)
from .utils import canonical_hash, now_ts
from ..lru import TTLCache


logger = logging.getLogger(__name__)


_GOLDEN_PREFIX = golden_hash_key("")


class SnapshotCoalescer:
    """Join token metadata, OHLCV, and depth into Golden Snapshots."""

    def __init__(
        self,
        emit: Callable[[GoldenSnapshot], Awaitable[None]],
        *,
        kv: KeyValueStore | None = None,
        hash_ttl: float = 90.0,
        hash_cache_size: int = 4096,
    ) -> None:
        self._emit = emit
        self._meta: Dict[str, TokenSnapshot] = {}
        self._bars: Dict[str, OHLCVBar] = {}
        self._depth: Dict[str, DepthSnapshot] = {}
        self._hash_cache: TTLCache[str, str] = TTLCache(
            maxsize=hash_cache_size,
            ttl=hash_ttl,
        )
        self._versions: Dict[str, int] = {}
        self._locks_guard = asyncio.Lock()
        self._mint_locks: Dict[str, asyncio.Lock] = {}
        self._kv = kv
        self._hash_ttl = hash_ttl
        self._prewarm_task: asyncio.Task[None] | None = None
        self._schedule_hash_prewarm()

    def _schedule_hash_prewarm(self) -> None:
        if not self._kv:
            return
        scan = getattr(self._kv, "scan_prefix", None)
        if not callable(scan):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        self._prewarm_task = loop.create_task(self._prewarm_hash_cache(scan))
        self._prewarm_task.add_done_callback(self._handle_prewarm_result)

    def _handle_prewarm_result(self, task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except Exception:  # pragma: no cover - logging safeguard
            logger.exception("failed prewarming golden hash cache")

    async def _prewarm_hash_cache(self, scan: Callable[[str], Awaitable[Any]]) -> None:
        try:
            result = await scan(_GOLDEN_PREFIX)
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("error scanning persisted golden hashes")
            return

        if not result:
            return

        for key, value in result:  # type: ignore[type-var]
            self._store_hash_from_kv(key, value)

    def _store_hash_from_kv(self, key: str, value: str | None) -> None:
        if not key.startswith(_GOLDEN_PREFIX):
            return
        if not value:
            return
        mint = key[len(_GOLDEN_PREFIX) :]
        if not mint:
            return
        self._hash_cache.set(mint, value)

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
        depth_pct = dict(depth.depth_pct)
        ohlcv_payload = {
            "o": bar.open,
            "h": bar.high,
            "l": bar.low,
            "c": bar.close,
            "close": bar.close,
            "vol_usd": bar.vol_usd,
            "volume": bar.vol_usd,
            "volume_usd": bar.vol_usd,
            "vol_base": bar.vol_base,
            "volume_base": bar.vol_base,
            "buyers": bar.buyers,
            "flow_usd": bar.flow_usd,
            "zret": bar.zret,
            "zvol": bar.zvol,
            "asof_close": bar.asof_close,
            "schema_version": bar.schema_version,
        }
        ohlcv_payload["content_hash"] = canonical_hash(dict(ohlcv_payload))
        liq_payload = {
            "depth_pct": depth_pct,
            "depth_usd_by_pct": depth_pct,
            "asof": depth.asof,
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
                "mid_usd": depth.mid_usd,
                "spread_bps": depth.spread_bps,
            },
            "liq": liq_payload,
            "ohlcv5m": ohlcv_payload,
            "schema_version": GOLDEN_SNAPSHOT_SCHEMA_VERSION,
        }
        payload["px_mid_usd"] = depth.mid_usd
        depth_1pct = None
        for key in ("1", "1.0", "100", "100bps"):
            if key in depth_pct:
                try:
                    depth_1pct = float(depth_pct[key])
                except Exception:
                    depth_1pct = None
                else:
                    break
        payload["liq_depth_1pct_usd"] = depth_1pct
        payload["content_hash"] = canonical_hash({k: v for k, v in payload.items() if k != "content_hash"})
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
            content_hash=payload.get("content_hash", snapshot_hash),
            metrics={
                "emitted_at": now,
                "latency_ms": latency_ms,
                "depth_staleness_ms": depth_staleness_ms,
                "candle_age_ms": candle_age_ms,
            },
        )
        await self._emit(golden)
