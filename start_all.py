"""Compatibility wrapper that injects health gates before launching runtime."""

from __future__ import annotations

import logging
import os
from typing import List

import sys

from scripts import start_all as _script

from solhunter_zero.health_runtime import http_ok, wait_for
from solhunter_zero.ui import ui_selftest


log = logging.getLogger(__name__)

__all__ = ["main", "ui_selftest", "wait_for"]


def _run_rl_gate(url: str) -> None:
    """Block until the RL daemon is healthy or raise ``SystemExit``."""

    if not url:
        log.info("Skipping RL daemon health gate: RL_HEALTH_URL not set")
        return

    def _probe() -> tuple[bool, str]:
        return http_ok(url)

    ok, message = wait_for(_probe, retries=30, sleep=1.0)
    if not ok:
        raise SystemExit(f"RL daemon gate failed: {message}")


def main(argv: List[str] | None = None) -> int:
    """Entry point that mirrors ``scripts.start_all`` with preflight gates."""

    args = _script.parse_args(list(argv or []))
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    if not args.skip_clean:
        _script.kill_lingering_processes()

    env = _script.ensure_environment(args.config)
    cfg_path = env["config_path"]

    rc = ui_selftest(config_path=cfg_path)
    if rc != 0:
        raise SystemExit(rc)

    rl_health_url = os.getenv("RL_HEALTH_URL", "").strip()
    _run_rl_gate(rl_health_url)

    if args.foreground:
        return _script.launch_foreground(args, cfg_path)
    if args.non_interactive:
        return _script.launch_detached(args, cfg_path)
    return _script.launch_detached(args, cfg_path)


if __name__ == "__main__":  # pragma: no cover - manual CLI
    raise SystemExit(main(sys.argv[1:]))
