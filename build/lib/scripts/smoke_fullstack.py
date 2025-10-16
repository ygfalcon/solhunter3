"""Full-stack smoke test for SolHunter Zero.

This script performs lightweight health checks to verify that core services
are reachable.  It is intended for CI and local paper trading environments and
therefore keeps dependencies to a minimum.

The RL daemon is now mandatory; the script fails fast when the service is not
healthy.
"""

from __future__ import annotations

import os
from solhunter_zero.health_runtime import check_redis, http_ok, wait_for


def fail(msg: str) -> int:
    print(msg)
    return 1


def main() -> int:
    EVENT_BUS_URL = os.getenv("EVENT_BUS_URL", "redis://127.0.0.1:6379/0")
    RL_HEALTH_URL = os.getenv("RL_HEALTH_URL", "http://127.0.0.1:7070/health")
    UI_HEALTH_URL = os.getenv("UI_HEALTH_URL")

    if os.getenv("USE_REDIS", "0") == "1":
        ok, msg = wait_for(lambda: check_redis(EVENT_BUS_URL))
        if not ok:
            return fail(f"Redis: {msg}")

    # RL daemon is mandatory in smoke as well
    ok, msg = wait_for(lambda: http_ok(RL_HEALTH_URL))
    if not ok:
        return fail(f"RL daemon: {msg}")

    if os.getenv("CHECK_UI_HEALTH", "0") == "1" and UI_HEALTH_URL:
        ok, msg = wait_for(lambda: http_ok(UI_HEALTH_URL))
        if not ok:
            return fail(f"UI: {msg}")

    return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())

