#!/usr/bin/env python3
"""Unified launcher for the SolHunter trading runtime.

This script intentionally keeps orchestration logic lightweight:

1. Clean up lingering processes from previous runs.
2. Prepare the environment (config/keypair discovery, Redis availability).
3. Either run the runtime in the foreground or spawn a detached process.

It delegates all heavy lifting to :class:`solhunter_zero.runtime.trading_runtime.TradingRuntime`.
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

from solhunter_zero.runtime.trading_runtime import TradingRuntime
from solhunter_zero.config import (
    apply_env_overrides,
    ensure_config_file,
    load_config,
    set_env_from_config,
)
from solhunter_zero.redis_util import ensure_local_redis_if_needed


log = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start SolHunter runtime")
    parser.add_argument("--config", help="Path to config file", default=None)
    parser.add_argument("--ui-host", default="127.0.0.1", help="UI bind host")
    default_ui_port = os.getenv("UI_PORT", "5001")
    parser.add_argument("--ui-port", default=default_ui_port, help="UI bind port")
    parser.add_argument("--foreground", action="store_true", help="Run in foreground")
    parser.add_argument(
        "--non-interactive",
        action="store_true",
        help="Compatibility flag; implies detached mode",
    )
    parser.add_argument("--loop-delay", type=float, default=None, help="Override loop delay")
    parser.add_argument("--min-delay", type=float, default=None, help="Minimum inter-iteration delay")
    parser.add_argument("--max-delay", type=float, default=None, help="Maximum inter-iteration delay")
    parser.add_argument("--skip-clean", action="store_true", help="Skip process cleanup")
    args = parser.parse_args(argv)

    try:
        args.ui_port = int(args.ui_port)
    except (TypeError, ValueError) as exc:
        raise SystemExit(
            f"Invalid value for --ui-port: {args.ui_port!r}. Must be an integer."
        ) from exc

    return args


def kill_lingering_processes() -> None:
    pkill_path = shutil.which("pkill")
    if not pkill_path:
        log.warning("Skipping lingering process cleanup: pkill not found on PATH")
        return
    patterns = [
        "solhunter_zero.primary_entry_point",
        "solhunter_zero.runtime.launch",
        "depth_service",
        "run_rl_daemon.py",
    ]
    for pat in patterns:
        subprocess.run(
            [pkill_path, "-f", pat],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    time.sleep(0.5)


def ensure_environment(cfg_path: str | None) -> dict:
    cfg_path = cfg_path or ensure_config_file()
    if not cfg_path:
        raise SystemExit("No configuration file found")
    cfg_path = str(Path(cfg_path).expanduser().resolve())
    cfg = apply_env_overrides(load_config(cfg_path))
    set_env_from_config(cfg)
    broker_urls = cfg.get("broker_urls") or cfg.get("broker_url")
    if isinstance(broker_urls, str):
        broker_urls = [u.strip() for u in broker_urls.split(",") if u.strip()]
    elif not broker_urls:
        broker_urls = []
    ensure_local_redis_if_needed(broker_urls)
    return {"config_path": cfg_path, "config": cfg}


def launch_detached(args: argparse.Namespace, cfg_path: str) -> int:
    cmd = [
        sys.executable,
        "-m",
        "solhunter_zero.runtime.launch",
        f"--config={cfg_path}",
        f"--ui-host={args.ui_host}",
        f"--ui-port={args.ui_port}",
    ]
    if args.loop_delay is not None:
        cmd.append(f"--loop-delay={args.loop_delay}")
    if args.min_delay is not None:
        cmd.append(f"--min-delay={args.min_delay}")
    if args.max_delay is not None:
        cmd.append(f"--max-delay={args.max_delay}")

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    log.info("Launched runtime pid=%s", proc.pid)
    return 0


def launch_foreground(args: argparse.Namespace, cfg_path: str) -> int:
    runtime = TradingRuntime(
        config_path=cfg_path,
        ui_host=args.ui_host,
        ui_port=args.ui_port,
        loop_delay=args.loop_delay,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
    runtime.run_forever()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    if not args.skip_clean:
        kill_lingering_processes()

    env = ensure_environment(args.config)
    cfg_path = env["config_path"]

    if args.foreground:
        return launch_foreground(args, cfg_path)
    if args.non_interactive:
        return launch_detached(args, cfg_path)
    return launch_detached(args, cfg_path)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
