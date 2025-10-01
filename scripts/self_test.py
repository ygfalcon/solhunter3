#!/usr/bin/env python3
"""Run environment self tests and emit JSON summary."""

from __future__ import annotations

import json
from typing import Any, Dict

from scripts import preflight  # noqa: E402
from solhunter_zero import device  # noqa: E402


def run_self_test() -> Dict[str, Any]:
    """Execute environment checks returning a structured result."""

    pf_results = preflight.run_preflight()
    filtered = [r for r in pf_results if r[0] not in {"Network", "GPU"}]
    gpu_ok, gpu_msg = device.verify_gpu()
    net_ok, net_msg = preflight.check_network()
    return {
        "preflight": [
            {"name": name, "ok": ok, "message": msg}
            for name, ok, msg in filtered
        ],
        "gpu": {"ok": gpu_ok, "message": gpu_msg},
        "network": {"ok": net_ok, "message": net_msg},
    }


def main() -> int:
    data = run_self_test()
    print(json.dumps(data, indent=2))
    ok = (
        all(item["ok"] for item in data["preflight"])
        and data["gpu"]["ok"]
        and data["network"]["ok"]
    )
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
