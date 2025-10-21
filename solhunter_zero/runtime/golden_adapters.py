"""Schema adapters for Golden pipeline payloads."""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional, Sequence

from ..event_bus import publish
from ..golden_pipeline.contracts import STREAMS

log = logging.getLogger(__name__)

_WARNING_INTERVAL = 60.0
_LAST_WARNING: dict[tuple[str, str, tuple[str, ...]], float] = {}


@dataclass(frozen=True)
class NormalizedOHLCV:
    """Normalized view over OHLCV payloads."""

    close: Optional[float]
    volume_usd: Optional[float]
    volume_base: Optional[float]
    buyers: Optional[int]
    sellers: Optional[int]
    schema_version: Optional[str]
    content_hash: Optional[str]


@dataclass(frozen=True)
class NormalizedGolden:
    """Normalized view over Golden snapshot payloads."""

    mid_usd: Optional[float]
    depth_1pct_usd: Optional[float]
    schema_version: Optional[str]
    content_hash: Optional[str]


def _coerce_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        result = float(value)
    except Exception:
        return None
    if not math.isfinite(result):
        return None
    return result


def _coerce_int(value: Any) -> Optional[int]:
    numeric = _coerce_float(value)
    if numeric is None:
        return None
    try:
        return int(numeric)
    except Exception:
        return None


def _record_mismatch(stream: str, reader: str, issues: Iterable[str]) -> None:
    unique = tuple(sorted({issue for issue in issues if issue}))
    if not unique:
        return
    try:
        publish(
            "schema_mismatch_total",
            {
                "stream": stream,
                "reader": reader,
                "issues": list(unique),
                "count": 1,
            },
        )
    except Exception:
        pass
    now = time.time()
    key = (stream, reader, unique)
    last = _LAST_WARNING.get(key, 0.0)
    if now - last >= _WARNING_INTERVAL:
        log.warning(
            "schema mismatch for stream %s reader=%s issues=%s",
            stream,
            reader,
            ", ".join(unique),
        )
        _LAST_WARNING[key] = now


def _extract_first_float(payload: Mapping[str, Any], keys: Sequence[str]) -> tuple[Optional[float], bool]:
    for key in keys:
        value = _coerce_float(payload.get(key))
        if value is not None:
            return value, key != keys[0]
    return None, False


def _extract_first_int(payload: Mapping[str, Any], keys: Sequence[str]) -> tuple[Optional[int], bool]:
    for key in keys:
        value = _coerce_int(payload.get(key))
        if value is not None:
            return value, key != keys[0]
    return None, False


def normalize_ohlcv_payload(
    payload: Mapping[str, Any] | None,
    *,
    reader: str,
    stream: str | None = None,
) -> NormalizedOHLCV:
    """Normalize OHLCV payloads across legacy and v2 shapes."""

    if payload is None:
        payload = {}
    stream_name = stream or STREAMS.market_ohlcv_v2
    issues: list[str] = []

    close, used_fallback = _extract_first_float(
        payload,
        ("close", "c", "px_mid_usd", "mid_usd", "price"),
    )
    if used_fallback:
        issues.append("fallback_close")
    if close is None:
        issues.append("missing_close")

    volume_usd, used_fallback = _extract_first_float(
        payload,
        ("volume", "volume_usd", "vol_usd"),
    )
    if used_fallback:
        issues.append("fallback_volume")
    if volume_usd is None:
        issues.append("missing_volume")

    volume_base, used_fallback = _extract_first_float(
        payload,
        ("volume_base", "vol_base", "base_volume"),
    )
    if used_fallback:
        issues.append("fallback_volume_base")

    buyers, used_fallback = _extract_first_int(
        payload,
        ("buyers", "buyer_count", "num_buyers"),
    )
    if used_fallback:
        issues.append("fallback_buyers")

    sellers, used_fallback = _extract_first_int(
        payload,
        ("sellers", "seller_count", "num_sellers"),
    )
    if used_fallback:
        issues.append("fallback_sellers")

    if issues:
        _record_mismatch(stream_name, reader, issues)

    schema_version = None
    raw_version = payload.get("schema_version")
    if isinstance(raw_version, str) and raw_version.strip():
        schema_version = raw_version.strip()
    content_hash = payload.get("content_hash")
    if not isinstance(content_hash, str):
        content_hash = None

    return NormalizedOHLCV(
        close=close,
        volume_usd=volume_usd,
        volume_base=volume_base,
        buyers=buyers,
        sellers=sellers,
        schema_version=schema_version,
        content_hash=content_hash,
    )


def _find_depth_mapping(liq: Mapping[str, Any]) -> Mapping[str, Any] | None:
    for key in ("depth_pct", "depth", "depth_usd_by_pct"):
        value = liq.get(key)
        if isinstance(value, Mapping):
            return value
    return None


def normalize_golden_snapshot(
    payload: Mapping[str, Any] | None,
    *,
    reader: str,
    stream: str | None = None,
) -> NormalizedGolden:
    """Normalize Golden snapshot payloads across schema versions."""

    if payload is None:
        payload = {}
    stream_name = stream or STREAMS.golden_snapshot_v2
    issues: list[str] = []

    mid_usd = _coerce_float(payload.get("px_mid_usd"))
    if mid_usd is None:
        px = payload.get("px") if isinstance(payload.get("px"), Mapping) else {}
        if isinstance(px, Mapping):
            for key in ("mid_usd", "midUsd", "mid", "mid_price_usd", "price_usd", "fair_price", "price"):
                mid_usd = _coerce_float(px.get(key))
                if mid_usd is not None:
                    issues.append("fallback_px_mid_usd")
                    break
        if mid_usd is None:
            issues.append("missing_px_mid_usd")

    depth_1pct = _coerce_float(payload.get("liq_depth_1pct_usd"))
    if depth_1pct is None:
        liq = payload.get("liq") if isinstance(payload.get("liq"), Mapping) else {}
        if isinstance(liq, Mapping):
            depth_map = _find_depth_mapping(liq)
            if depth_map:
                for key in ("1", "1.0", "100", "100bps", "1pct"):
                    depth_1pct = _coerce_float(depth_map.get(key))
                    if depth_1pct is not None:
                        issues.append("fallback_liq_depth_1pct_usd")
                        break
        if depth_1pct is None:
            issues.append("missing_liq_depth_1pct_usd")

    if issues:
        _record_mismatch(stream_name, reader, issues)

    schema_version = None
    raw_version = payload.get("schema_version")
    if isinstance(raw_version, str) and raw_version.strip():
        schema_version = raw_version.strip()

    content_hash = payload.get("content_hash")
    if not isinstance(content_hash, str):
        content_hash = None

    return NormalizedGolden(
        mid_usd=mid_usd,
        depth_1pct_usd=depth_1pct,
        schema_version=schema_version,
        content_hash=content_hash,
    )

