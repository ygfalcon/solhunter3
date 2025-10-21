"""Price quote aggregation for the Golden pipeline."""

from __future__ import annotations

import asyncio
import math
import statistics
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Iterable, List, Sequence, Tuple

from .types import PriceQuoteUpdate, PriceSnapshot
from .utils import now_ts


class PriceStage:
    """Blend provider quotes into a single price snapshot per mint."""

    def __init__(
        self,
        emit: Callable[[PriceSnapshot], Awaitable[None]],
        *,
        stale_after_sec: float = 2.5,
        sigma_threshold: float = 1.4,
        clamp_sigma: float = 2.8,
        noise_alert_ratio: float = 0.004,
    ) -> None:
        self._emit = emit
        self._stale_after = max(0.1, stale_after_sec)
        self._sigma_threshold = max(0.1, sigma_threshold)
        self._clamp_sigma = max(self._sigma_threshold, clamp_sigma)
        self._noise_alert_ratio = max(0.0, noise_alert_ratio)
        self._quotes: Dict[str, Dict[str, PriceQuoteUpdate]] = defaultdict(dict)
        self._lock = asyncio.Lock()

    async def submit(self, quote: PriceQuoteUpdate) -> None:
        async with self._lock:
            per_mint = self._quotes[quote.mint]
            per_mint[quote.source] = quote
            snapshot = self._aggregate(quote.mint, per_mint.values())
        await self._emit(snapshot)

    def _aggregate(
        self, mint: str, quotes: Iterable[PriceQuoteUpdate]
    ) -> PriceSnapshot:
        now = now_ts()
        entries: List[Dict[str, object]] = []
        active: List[Tuple[PriceQuoteUpdate, Dict[str, object]]] = []
        stale: List[Tuple[PriceQuoteUpdate, Dict[str, object]]] = []

        for quote in quotes:
            mid = float(quote.mid_usd)
            bid = float(quote.bid_usd)
            ask = float(quote.ask_usd)
            liquidity = float(quote.liquidity)
            staleness = max(0.0, now - float(quote.asof))
            entry: Dict[str, object] = {
                "source": quote.source,
                "bid_usd": bid,
                "ask_usd": ask,
                "mid_usd": mid,
                "liquidity": liquidity,
                "staleness_ms": staleness * 1000.0,
                "weight": 0.0,
                "status": "fresh",
            }
            if quote.extras:
                entry["extras"] = dict(quote.extras)
            entries.append(entry)
            if staleness > self._stale_after:
                entry["status"] = "stale"
                stale.append((quote, entry))
            else:
                active.append((quote, entry))

        alerts: List[str] = []
        used_quotes: Sequence[Tuple[PriceQuoteUpdate, Dict[str, object]]]
        if active:
            used_quotes = active
            if stale:
                alerts.append("price.stale")
        elif stale:
            alerts.append("price.stale")
            stale = sorted(stale, key=lambda item: item[0].asof, reverse=True)
            quote, entry = stale[0]
            entry["status"] = "stale_used"
            used_quotes = [stale[0]]
        else:
            diagnostics = {
                "alerts": ["price.missing"],
                "used_quotes": 0,
                "total_quotes": 0,
                "noise_ratio": 0.0,
                "stdev": 0.0,
            }
            return PriceSnapshot(
                mint=mint,
                mid_usd=None,
                spread_bps=None,
                asof=now,
                providers=[dict(entry) for entry in entries],
                diagnostics=diagnostics,
            )

        raw_mids = [float(q.mid_usd) for q, _ in used_quotes]
        mean_mid = sum(raw_mids) / len(raw_mids) if raw_mids else 0.0
        std_all = statistics.pstdev(raw_mids) if len(raw_mids) > 1 else 0.0

        outlier_indexes: set[int] = set()
        if std_all > 0:
            for idx, (quote, entry) in enumerate(used_quotes):
                zscore = (float(quote.mid_usd) - mean_mid) / std_all if std_all else 0.0
                entry["zscore"] = zscore
                if abs(zscore) > self._sigma_threshold:
                    entry["status"] = "outlier"
                    outlier_indexes.add(idx)
        filtered_quotes = [item for idx, item in enumerate(used_quotes) if idx not in outlier_indexes]
        if outlier_indexes:
            alerts.append("price.outlier")

        if not filtered_quotes:
            filtered_quotes = used_quotes

        lower = mean_mid - self._clamp_sigma * std_all if std_all > 0 else None
        upper = mean_mid + self._clamp_sigma * std_all if std_all > 0 else None

        weighted_mid = 0.0
        total_weight = 0.0
        weighted_spread = 0.0
        filtered_mids: List[float] = []

        for quote, entry in filtered_quotes:
            mid = float(quote.mid_usd)
            if lower is not None and upper is not None:
                clamped = min(max(mid, lower), upper)
                if clamped != mid:
                    adjustments = entry.setdefault("adjustments", {})
                    if isinstance(adjustments, dict):
                        adjustments["mid_usd"] = clamped
                    mid = clamped
            bid = float(quote.bid_usd)
            ask = float(quote.ask_usd)
            spread = max(0.0, ask - bid)
            liquidity = float(quote.liquidity)
            weight = liquidity if math.isfinite(liquidity) and liquidity > 0 else 1.0
            entry["weight"] = float(weight)
            weighted_mid += mid * weight
            total_weight += weight
            if mid > 0:
                weighted_spread += (spread / mid) * 10_000.0 * weight
            filtered_mids.append(mid)

        if total_weight <= 0:
            weighted_mid = mean_mid
            weighted_spread = 0.0
        else:
            weighted_mid /= total_weight
            weighted_spread = weighted_spread / total_weight if filtered_mids else 0.0

        std_filtered = statistics.pstdev(filtered_mids) if len(filtered_mids) > 1 else 0.0
        noise_ratio = abs(std_filtered / weighted_mid) if weighted_mid else 0.0
        if noise_ratio > self._noise_alert_ratio and "price.outlier" not in alerts:
            alerts.append("price.noise")

        diagnostics = {
            "alerts": sorted({alert for alert in alerts if alert}),
            "used_quotes": len(filtered_quotes),
            "total_quotes": len(entries),
            "noise_ratio": noise_ratio,
            "stdev": std_filtered,
            "outliers": len(outlier_indexes),
        }

        asof = max((float(q.asof) for q, _ in filtered_quotes), default=now)
        return PriceSnapshot(
            mint=mint,
            mid_usd=weighted_mid if weighted_mid > 0 else None,
            spread_bps=weighted_spread if weighted_spread > 0 else None,
            asof=asof,
            providers=[dict(entry) for entry in entries],
            diagnostics=diagnostics,
        )

