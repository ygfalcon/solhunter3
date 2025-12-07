import json
import math
import os
from pathlib import Path
from typing import Mapping

import pytest

from solhunter_zero.golden_pipeline.contracts import STREAMS


_THRESHOLD_SEC = 5.0


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return ordered[int(rank)]
    span = ordered[upper] - ordered[lower]
    return ordered[lower] + span * (rank - lower)


def _quote_exit_latencies(harness) -> list[dict[str, float | str]]:
    quote_by_mint: dict[str, list[float]] = {}
    for quote in harness.bus.events.get(STREAMS.market_depth, []):
        ts = float(quote.get("asof", 0.0) or 0.0)
        mint = str(quote.get("mint"))
        quote_by_mint.setdefault(mint, []).append(ts)

    latencies: list[dict[str, float | str]] = []
    for fill in harness.bus.events.get(STREAMS.virtual_fills, []):
        mint = str(fill.get("mint"))
        exit_ts = float(fill.get("ts", 0.0) or 0.0)
        quote_candidates = [ts for ts in quote_by_mint.get(mint, []) if ts <= exit_ts]
        if not quote_candidates:
            quote_candidates = quote_by_mint.get(mint, []) or [exit_ts]
        quote_ts = max(quote_candidates)
        latency = max(0.0, exit_ts - quote_ts)
        latencies.append(
            {
                "mint": mint,
                "quote_asof": quote_ts,
                "exit_ts": exit_ts,
                "latency_sec": latency,
            }
        )
    return latencies


def _write_metrics_artifact(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@pytest.mark.usefixtures("golden_harness")
def test_discovery_bypass_latency(golden_harness) -> None:
    bypass_events = [entry for entry in golden_harness.discovery_breaker_journal if entry["event"] == "submission_while_open"]
    assert bypass_events, "discovery bypass scenario not exercised"
    assert bypass_events[0].get("accepted") is False

    latencies = _quote_exit_latencies(golden_harness)
    assert latencies, "no quote→exit timings recorded"

    latency_values = [entry["latency_sec"] for entry in latencies]
    p95 = _percentile(latency_values, 95.0)
    assert p95 is not None

    metrics = {
        "count": len(latencies),
        "p95_sec": p95,
        "threshold_sec": _THRESHOLD_SEC,
        "latencies": latencies,
        "bypass_events": golden_harness.discovery_breaker_journal,
    }

    artifact_root = Path(os.getenv("ARTIFACT_DIR", "artifacts/metrics"))
    artifact_path = artifact_root / "discovery_quote_exit_latency.json"
    _write_metrics_artifact(artifact_path, metrics)

    assert p95 <= _THRESHOLD_SEC, f"quote→exit p95 {p95:.3f}s exceeds {_THRESHOLD_SEC:.1f}s"
