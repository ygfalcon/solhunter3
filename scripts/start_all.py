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
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Callable, TypeVar

from solhunter_zero.runtime.trading_runtime import TradingRuntime
from solhunter_zero.config import (
    apply_env_overrides,
    ensure_config_file,
    load_config,
    set_env_from_config,
)
from solhunter_zero.redis_util import ensure_local_redis_if_needed
from solhunter_zero.logging_utils import configure_runtime_logging


log = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class StageResult:
    """Represents the outcome for a pipeline stage."""

    name: str
    success: bool
    duration: float
    error: str | None = None


def _mask_value(key: str, value: object) -> object:
    """Best-effort masking to avoid leaking secrets in logs."""

    lowered = key.lower()
    if any(token in lowered for token in ("secret", "key", "token", "pass", "private")):
        return "***"
    return value


def log_config_overview(cfg: dict) -> None:
    """Emit a compact overview of the loaded configuration."""

    preview: dict[str, object] = {}
    for key, value in cfg.items():
        if isinstance(value, (str, int, float, bool)):
            preview[key] = _mask_value(key, value)
        elif isinstance(value, Mapping):
            preview[key] = f"{type(value).__name__}(keys={len(value)})"
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
            try:
                length = len(value)
            except TypeError:  # pragma: no cover - unlikely but safe guard
                length = "?"
            preview[key] = f"{type(value).__name__}(len={length})"
        else:
            preview[key] = type(value).__name__
    log.info("Active configuration overview: %s", preview)


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
    return parser.parse_args(argv)


def kill_lingering_processes() -> None:
    patterns = [
        "solhunter_zero.primary_entry_point",
        "solhunter_zero.runtime.launch",
        "depth_service",
        "run_rl_daemon.py",
    ]
    log.info("Scanning for lingering processes: patterns=%s", patterns)
    for pat in patterns:
        subprocess.run(["pkill", "-f", pat], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)
    log.debug("Process cleanup completed")


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
    log.info("Using configuration at %s", cfg_path)
    log.debug("Broker URLs resolved to %s", broker_urls)
    ensure_local_redis_if_needed(broker_urls)
    log_config_overview(cfg)
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

    log.info("Launching runtime in detached mode: %s", " ".join(cmd))
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.5)
    if proc.poll() is not None:
        raise RuntimeError(
            "Runtime process exited immediately with code"
            f" {proc.returncode}. Check logs for more details."
        )
    log.info("Launched runtime pid=%s", proc.pid)
    return 0


def launch_foreground(args: argparse.Namespace, cfg_path: str) -> int:
    log.info("Starting TradingRuntime in foreground mode")
    runtime = TradingRuntime(
        config_path=cfg_path,
        ui_host=args.ui_host,
        ui_port=int(args.ui_port),
        loop_delay=args.loop_delay,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
    runtime.run_forever()
    log.info("TradingRuntime exited cleanly")
    return 0


def run_stage(name: str, func: Callable[[], T], stage_results: list[StageResult]) -> T:
    """Execute a pipeline stage with structured logging."""

    log.info("▶ Starting stage: %s", name)
    started = time.perf_counter()
    try:
        result = func()
    except Exception as exc:  # pragma: no cover - exercised in failure scenarios
        duration = time.perf_counter() - started
        stage_results.append(StageResult(name=name, success=False, duration=duration, error=str(exc)))
        log.error("✗ Stage %s failed after %.2fs", name, duration, exc_info=exc)
        raise
    duration = time.perf_counter() - started
    stage_results.append(StageResult(name=name, success=True, duration=duration))
    log.info("✓ Stage %s completed in %.2fs", name, duration)
    return result


def summarize_stages(stage_results: list[StageResult]) -> None:
    if not stage_results:
        log.warning("Pipeline summary unavailable: no stages recorded")
        return
    log.info("Startup pipeline summary:")
    for result in stage_results:
        status = "OK" if result.success else "FAIL"
        extra = f" — {result.error}" if result.error else ""
        log.info("  [%s] %s (%.2fs)%s", status, result.name, result.duration, extra)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_runtime_logging(force=True)
    logging.getLogger(__name__).info("Runtime logging configured")

    stage_results: list[StageResult] = []
    exit_code = 0

    try:
        if not args.skip_clean:
            run_stage("process-cleanup", kill_lingering_processes, stage_results)
        env = run_stage("ensure-environment", lambda: ensure_environment(args.config), stage_results)
        cfg_path = env["config_path"]

        if args.foreground:
            run_stage("launch-foreground", lambda: launch_foreground(args, cfg_path), stage_results)
        elif args.non_interactive:
            run_stage("launch-detached", lambda: launch_detached(args, cfg_path), stage_results)
        else:
            run_stage("launch-detached", lambda: launch_detached(args, cfg_path), stage_results)
    except Exception as exc:
        exit_code = 1
        log.critical("Startup pipeline aborted: %s", exc)
        log.debug("Detailed traceback:\n%s", "".join(traceback.format_exception(exc)))
    finally:
        summarize_stages(stage_results)

    return exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main(sys.argv[1:]))
