#!/usr/bin/env python3
"""Run a 1-minute health soak against the UI and write a summary JSON.

Outputs:
- health/soak_report.json with rolling metrics (per 15s intervals and totals)
"""

from __future__ import annotations

import json
import time
import urllib.request
from pathlib import Path


def fetch(path: str, timeout: float = 3.0):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:5005/{path}", timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception:
        return None


def main() -> None:
    t = time.time()
    samples = []
    for i in range(5):  # t0, t15, t30, t45, t60 (5 samples)
        samples.append({
            "ts": time.time(),
            "trades": fetch("trades") or [],
            "roi": (fetch("roi") or {}).get("roi"),
            "sharpe": (fetch("sharpe") or {}).get("sharpe"),
            "activity": (fetch("activity") or {}).get("lines", []),
        })
        if i < 4:
            time.sleep(15)

    # Compute rolling metrics
    intervals = []
    for a, b in zip(samples, samples[1:]):
        ta = len(a.get("trades", []) or [])
        tb = len(b.get("trades", []) or [])
        aa = len(a.get("activity", []) or [])
        ab = len(b.get("activity", []) or [])
        trades_delta = max(0, tb - ta)
        activity_delta = max(0, ab - aa)
        intervals.append({
            "window_sec": 15,
            "trades_delta": trades_delta,
            "trades_per_min": trades_delta * 4,
            "activity_delta": activity_delta,
            "activity_per_min": activity_delta * 4,
            "roi_start": a.get("roi"),
            "roi_end": b.get("roi"),
            "roi_delta": ( (b.get("roi") or 0) - (a.get("roi") or 0) ) if isinstance(a.get("roi"), (int, float)) and isinstance(b.get("roi"), (int, float)) else None,
            "sharpe_end": b.get("sharpe"),
        })

    report = {
        "started_at": t,
        "ended_at": time.time(),
        "samples": samples,
        "intervals": intervals,
        "totals": {
            "trades_delta": sum(i["trades_delta"] for i in intervals),
            "trades_per_min_avg": sum(i["trades_per_min"] for i in intervals)/len(intervals) if intervals else 0,
            "activity_delta": sum(i["activity_delta"] for i in intervals),
            "activity_per_min_avg": sum(i["activity_per_min"] for i in intervals)/len(intervals) if intervals else 0,
            "roi_delta_total": sum((i["roi_delta"] or 0) for i in intervals),
            "sharpe_last": samples[-1].get("sharpe") if samples else None,
        }
    }

    outdir = Path("health")
    outdir.mkdir(exist_ok=True)
    (outdir / "soak_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report["totals"], indent=2))


if __name__ == "__main__":
    main()

