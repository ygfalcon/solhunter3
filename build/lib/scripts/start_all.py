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
import asyncio
import contextlib
import errno
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
from solhunter_zero.production import (
    Provider,
    load_production_env,
    assert_providers_ok,
    format_configured_providers,
    write_env_manifest,
    ConnectivityChecker,
)
from solhunter_zero.cache_paths import RUNTIME_LOCK_PATH


log = logging.getLogger(__name__)

T = TypeVar("T")


def _process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:  # pragma: no cover - platform differences
        if exc.errno in {errno.ESRCH, getattr(errno, "EPERM", None)}:
            return exc.errno != errno.ESRCH
        return False
    return True


@contextlib.contextmanager
def _acquire_runtime_lock(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        except FileExistsError:
            existing_pid: int | None = None
            try:
                existing_pid = int(path.read_text(encoding="utf-8").strip())
            except Exception:
                existing_pid = None
            if existing_pid and _process_alive(existing_pid):
                raise SystemExit(
                    f"SolHunter runtime already running (pid {existing_pid}); "
                    "terminate it or remove the lock file."
                )
            try:
                path.unlink()
            except OSError:
                raise SystemExit(
                    "SolHunter runtime lock is held and could not be cleared; "
                    "remove artifacts/.cache/runtime.lock if the process exited."
                ) from None
            continue
        else:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(str(os.getpid()))
                handle.flush()
            break
    try:
        yield
    finally:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


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


# ---------------------------------------------------------------------------
# Production helpers
# ---------------------------------------------------------------------------


PRODUCTION_PROVIDERS: list[Provider] = [
    Provider("Solana", ("SOLANA_RPC_URL", "SOLANA_WS_URL")),
    Provider("Helius", ("HELIUS_API_KEY",)),
    Provider("Redis", ("REDIS_URL",), optional=True),
    Provider("UI", ("UI_WS_URL",), optional=True),
    Provider("Helius-DAS", ("DAS_BASE_URL",), optional=True),
]


def _load_production_environment() -> dict[str, str]:
    return load_production_env()


def _validate_keys() -> str:
    assert_providers_ok(PRODUCTION_PROVIDERS)
    message = format_configured_providers(PRODUCTION_PROVIDERS)
    log.info(message)
    return message


def _write_manifest(loaded_env: Mapping[str, str]) -> Path:
    source_map = {name: "file" for name in loaded_env}
    manifest_path = Path("artifacts/prelaunch/env_manifest.json")
    return write_env_manifest(manifest_path, PRODUCTION_PROVIDERS, source_map=source_map)


def _connectivity_check() -> list[dict[str, object]]:
    checker = ConnectivityChecker()

    async def _run() -> list[dict[str, object]]:
        results = await checker.check_all()
        formatted: list[dict[str, object]] = []
        for result in results:
            status = "OK" if result.ok else f"FAIL ({result.error or result.status})"
            log.info(
                "Connectivity %s → %s (%.2f ms)",
                result.name,
                status,
                result.latency_ms or -1.0,
            )
            formatted.append(
                {
                    "name": result.name,
                    "target": result.target,
                    "ok": result.ok,
                    "latency_ms": result.latency_ms,
                    "status": result.status,
                    "status_code": result.status_code,
                    "error": result.error,
                }
            )
        return formatted

    return asyncio.run(_run())


def _connectivity_soak() -> dict[str, object]:
    duration = float(os.getenv("CONNECTIVITY_SOAK_DURATION", "180"))
    if duration <= 0:
        log.info("Connectivity soak disabled (duration <= 0)")
        return {"disabled": True, "duration": duration}

    checker = ConnectivityChecker()
    output_path = Path("artifacts/prelaunch/connectivity_report.json")

    async def _run() -> dict[str, object]:
        summary = await checker.run_soak(duration=duration, output_path=output_path)
        log.info(
            "Connectivity soak completed in %.1fs (reconnects=%d)",
            summary.duration,
            summary.reconnect_count,
        )
        return {
            "duration": summary.duration,
            "metrics": summary.metrics,
            "reconnect_count": summary.reconnect_count,
            "report": str(output_path),
        }

    return asyncio.run(_run())


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_runtime_logging(force=True)
    logging.getLogger(__name__).info("Runtime logging configured")

    stage_results: list[StageResult] = []
    exit_code = 0

    with _acquire_runtime_lock(RUNTIME_LOCK_PATH):
        try:
            if not args.skip_clean:
                run_stage("process-cleanup", kill_lingering_processes, stage_results)
            env = run_stage("ensure-environment", lambda: ensure_environment(args.config), stage_results)
            cfg_path = env["config_path"]
            loaded_env = run_stage("load-production-env", _load_production_environment, stage_results)
            run_stage("validate-keys", _validate_keys, stage_results)
            run_stage("write-env-manifest", lambda: _write_manifest(loaded_env), stage_results)
            run_stage("connectivity-check", _connectivity_check, stage_results)
            run_stage("connectivity-soak", _connectivity_soak, stage_results)

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
