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
import math
import os
import shutil
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Any, Callable, TypeVar
from urllib.parse import urlunparse

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
from solhunter_zero.production.keypair import (
    resolve_live_keypair,
    verify_onchain_funds,
)
from solhunter_zero.cache_paths import RUNTIME_LOCK_PATH
from solhunter_zero.rl_gate import rl_health_gate


log = logging.getLogger(__name__)

T = TypeVar("T")

RUNTIME_LOG_DIR = Path("artifacts/logs")
RUNTIME_LOG_NAME = "runtime.log"
RUNTIME_LOG_MAX_BYTES = 5 * 1024 * 1024  # 5 MiB
RUNTIME_LOG_BACKUP_COUNT = 3


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
    parser.add_argument(
        "--ui-port",
        default=default_ui_port,
        type=int,
        metavar="PORT",
        help="UI bind port",
    )
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
    parser.add_argument(
        "--skip-rl-gate",
        action="store_true",
        help="Bypass RL health gate (sets RL_HEALTH_BYPASS=1)",
    )
    return parser.parse_args(argv)


def _apply_ui_cli_overrides(args: argparse.Namespace) -> dict[str, str]:
    """Ensure CLI-provided UI overrides propagate through the environment."""

    host = str(getattr(args, "ui_host", "") or "127.0.0.1")
    port = str(getattr(args, "ui_port", "") or os.getenv("UI_PORT", "5001"))
    os.environ["UI_HOST"] = host
    os.environ["UI_PORT"] = port
    return {"UI_HOST": host, "UI_PORT": port}


def kill_lingering_processes() -> None:
    patterns = [
        "solhunter_zero.primary_entry_point",
        "solhunter_zero.runtime.launch",
        "depth_service",
        "run_rl_daemon.py",
    ]
    log.info("Scanning for lingering processes: patterns=%s", patterns)
    pkill_path = shutil.which("pkill")
    if not pkill_path:
        log.warning("Skipping process cleanup: 'pkill' not available on PATH")
        return
    for pat in patterns:
        subprocess.run(
            [pkill_path, "-f", pat],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
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


def ensure_live_keypair(cfg: dict | None) -> dict:
    resolved, pubkey = resolve_live_keypair(cfg or {}, announce=True, force=True)
    return {"keypair_path": str(resolved), "keypair_pubkey": str(pubkey)}


def _resolve_min_live_sol_threshold() -> float:
    raw = os.getenv("LIVE_MIN_SOL", "").strip()
    if not raw:
        return 0.0
    try:
        threshold = float(raw)
    except Exception:
        log.warning("Invalid LIVE_MIN_SOL=%r; using default 0.0", raw)
        return 0.0
    if threshold < 0:
        log.warning("LIVE_MIN_SOL=%r below zero; using default 0.0", raw)
        return 0.0
    return threshold


def verify_live_account() -> dict:
    try:
        mode = get_feature_flags().mode.lower()
    except Exception:
        mode = "paper"
    if mode != "live":
        return {"skipped": True}
    min_sol = _resolve_min_live_sol_threshold()
    balance, blockhash = verify_onchain_funds(min_sol=min_sol)
    if min_sol > 0:
        log.info(
            "Keypair balance %.6f SOL (minimum required %.6f); latest blockhash %s",
            balance,
            min_sol,
            blockhash,
        )
    else:
        log.info("Keypair balance %.6f SOL; latest blockhash %s", balance, blockhash)
    return {"balance_sol": balance, "blockhash": blockhash, "min_required_sol": min_sol}


def _config_has_broker(cfg: Mapping[str, object] | None) -> bool:
    if not cfg:
        return False
    raw = cfg.get("broker_urls")
    if isinstance(raw, str):
        if raw.strip():
            return True
    elif isinstance(raw, Sequence):
        for item in raw:
            if str(item).strip():
                return True
    raw_single = cfg.get("broker_url")
    if isinstance(raw_single, str) and raw_single.strip():
        return True
    return False


def _config_has_event_bus(cfg: Mapping[str, object] | None) -> bool:
    if not cfg:
        return False
    raw = cfg.get("event_bus_url")
    if isinstance(raw, str) and raw.strip():
        return True
    return False


def _config_mode(cfg: Mapping[str, object] | None) -> str | None:
    if not cfg:
        return None
    raw = cfg.get("mode")
    if isinstance(raw, str):
        cleaned = raw.strip()
        if cleaned:
            return cleaned
    return None


def apply_production_defaults(cfg: Mapping[str, object] | None = None) -> dict[str, str]:
    applied: dict[str, str] = {}

    mode = _config_mode(cfg) or "live"
    for key in ("SOLHUNTER_MODE", "MODE"):
        if key not in os.environ:
            os.environ.setdefault(key, mode)
            applied[key] = os.environ[key]

    broker_env_configured = any(
        os.getenv(name) for name in ("BROKER_WS_URLS", "BROKER_URLS", "BROKER_URL")
    )
    if not broker_env_configured and _config_has_broker(cfg):
        broker_env_configured = True
    if not broker_env_configured and "BROKER_WS_URLS" not in os.environ:
        os.environ.setdefault("BROKER_WS_URLS", "ws://127.0.0.1:8769")
        applied["BROKER_WS_URLS"] = os.environ["BROKER_WS_URLS"]

    event_bus_configured = bool(os.getenv("EVENT_BUS_URL"))
    if not event_bus_configured and _config_has_event_bus(cfg):
        event_bus_configured = True
    if not event_bus_configured and "EVENT_BUS_URL" not in os.environ:
        os.environ.setdefault("EVENT_BUS_URL", "ws://127.0.0.1:8779")
        applied["EVENT_BUS_URL"] = os.environ["EVENT_BUS_URL"]

    return applied


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except Exception:
        log.warning("Invalid %s=%r; using default %s", name, raw, default)
        return default
    return max(1, value)


def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except Exception:
        log.warning("Invalid %s=%r; using default %.2f", name, raw, default)
        return default
    return max(0.1, value)


def _rotate_runtime_log(log_path: Path) -> None:
    try:
        size = log_path.stat().st_size
    except FileNotFoundError:
        return

    if size < RUNTIME_LOG_MAX_BYTES:
        return

    if RUNTIME_LOG_BACKUP_COUNT < 1:
        log_path.unlink(missing_ok=True)
        return

    oldest = log_path.with_name(f"{log_path.name}.{RUNTIME_LOG_BACKUP_COUNT}")
    if oldest.exists():
        oldest.unlink()

    for idx in range(RUNTIME_LOG_BACKUP_COUNT - 1, 0, -1):
        src = log_path.with_name(f"{log_path.name}.{idx}")
        if src.exists():
            src.rename(log_path.with_name(f"{log_path.name}.{idx + 1}"))

    log_path.rename(log_path.with_name(f"{log_path.name}.1"))


def _prepare_runtime_log() -> Path:
    log_dir = RUNTIME_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / RUNTIME_LOG_NAME
    _rotate_runtime_log(log_path)
    return log_path


def _resolve_runtime_ready_url(args: argparse.Namespace) -> str:
    """Return the readiness probe URL for the launched runtime."""

    override = (os.getenv("START_ALL_READY_URL") or "").strip()
    if override:
        if override.lower() == "rl":
            return resolve_rl_health_url(require_health_file=False)
        return override

    scheme = (os.getenv("START_ALL_READY_SCHEME") or "http").strip() or "http"
    host = (os.getenv("START_ALL_READY_HOST") or str(args.ui_host)).strip()
    port = (os.getenv("START_ALL_READY_PORT") or str(args.ui_port)).strip()
    path = (os.getenv("START_ALL_READY_PATH") or "/health").strip() or "/health"
    if not path.startswith("/"):
        path = f"/{path}"
    netloc = host
    if port:
        netloc = f"{host}:{port}"
    return urlunparse((scheme, netloc, path, "", "", ""))


def _terminate_process(proc: subprocess.Popen[Any]) -> None:
    """Best-effort termination of a spawned subprocess."""

    if proc.poll() is not None:
        return
    with contextlib.suppress(Exception):
        proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(Exception):
            proc.kill()
    except Exception:
        pass


def launch_detached(args: argparse.Namespace, cfg_path: str) -> int:
    ui_port = str(args.ui_port)
    ui_host = str(args.ui_host)
    os.environ["UI_PORT"] = ui_port
    os.environ["UI_HOST"] = ui_host
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

    log_path = _prepare_runtime_log()
    log.info("Launching runtime in detached mode: %s", " ".join(cmd))
    log.info("Runtime stdout/stderr redirected to %s", log_path)
    env = os.environ.copy()
    env["UI_PORT"] = ui_port
    env["UI_HOST"] = ui_host
    with log_path.open("ab", buffering=0) as log_file:
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env=env,
        )
        time.sleep(0.5)
        if proc.poll() is not None:
            log_file.flush()
            raise RuntimeError(
                "Runtime process exited immediately with code"
                f" {proc.returncode}. Check logs at {log_path} for more details."
            )
    log.info("Launched runtime pid=%s", proc.pid)

    ready_url = _resolve_runtime_ready_url(args)
    retries = _parse_int_env("START_ALL_READY_RETRIES", 60)
    interval = _parse_float_env("START_ALL_READY_INTERVAL", 1.0)
    timeout_override = os.getenv("START_ALL_READY_TIMEOUT")
    if timeout_override is not None and timeout_override.strip():
        try:
            total_timeout = float(timeout_override)
        except Exception:
            log.warning(
                "Invalid START_ALL_READY_TIMEOUT=%r; using retries=%s interval=%.2fs",
                timeout_override,
                retries,
                interval,
            )
        else:
            if total_timeout <= 0:
                log.warning(
                    "START_ALL_READY_TIMEOUT=%r must be positive; using retries=%s interval=%.2fs",
                    timeout_override,
                    retries,
                    interval,
                )
            else:
                retries = max(1, math.ceil(total_timeout / interval))

    log.info(
        "Waiting for runtime readiness at %s (retries=%s, interval=%.2fs)",
        ready_url,
        retries,
        interval,
    )

    last_msg = "not attempted"
    for attempt in range(retries):
        ok, last_msg = http_ok(ready_url)
        if ok:
            log.info("Runtime readiness confirmed: %s — %s", ready_url, last_msg)
            break
        if proc.poll() is not None:
            raise RuntimeError(
                "Runtime process exited before readiness succeeded"
                f" (code {proc.returncode}). Check logs at {log_path} for more details."
            )
        log.debug(
            "Runtime readiness probe %d/%d failed: %s", attempt + 1, retries, last_msg
        )
        if attempt < retries - 1:
            time.sleep(interval)
    else:
        _terminate_process(proc)
        raise RuntimeError(
            "Runtime readiness check timed out after "
            f"{retries} attempts (~{retries * interval:.1f}s): {last_msg}"
        )

    return 0


def launch_foreground(args: argparse.Namespace, cfg_path: str) -> int:
    log.info("Starting TradingRuntime in foreground mode")
    os.environ["UI_PORT"] = str(args.ui_port)
    os.environ["UI_HOST"] = str(args.ui_host)
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
    except BaseException as exc:  # pragma: no cover - exercised in failure scenarios
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
    Provider("UI", ("UI_WS_URL", "UI_HOST", "UI_PORT"), optional=True),
    Provider("Helius-DAS", ("DAS_BASE_URL",), optional=True),
]


_PROVIDER_OPTIONALITY: dict[str, bool] = {provider.name: provider.optional for provider in PRODUCTION_PROVIDERS}

_PROBE_PROVIDER_MAP: dict[str, str] = {
    "solana-rpc": "Solana",
    "solana-ws": "Solana",
    "helius-rest": "Helius",
    "helius-das": "Helius-DAS",
    "redis": "Redis",
    "ui-ws": "UI",
    "ui-http": "UI",
    "ws-gateway": "UI",
}


def _probe_required(name: str) -> bool:
    provider = _PROBE_PROVIDER_MAP.get(name)
    if provider is None:
        return True
    return not _PROVIDER_OPTIONALITY.get(provider, False)


def _load_production_environment() -> dict[str, str]:
    return load_production_env(overwrite=True)


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
        fatal: tuple[str, str, str] | None = None
        event_bus_error: str | None = None
        for result in results:
            required = _probe_required(result.name)
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
                    "required": required,
                    "latency_ms": result.latency_ms,
                    "status": result.status,
                    "status_code": result.status_code,
                    "error": result.error,
                }
            )
            if required and not result.ok and fatal is None:
                reason = result.error or result.status or "unavailable"
                fatal = (result.name, reason, result.target)
            if (
                result.name == "ui-http"
                and not result.ok
                and result.error
                and "event bus" in result.error.lower()
            ):
                event_bus_error = (
                    "Event bus unavailable: "
                    f"{result.error} ({result.target})"
                )
        if event_bus_error:
            raise SystemExit(event_bus_error)
        if fatal:
            name, reason, target = fatal
            raise SystemExit(
                "Connectivity requirement failed: "
                f"{name} — {reason} ({target})"
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
    if getattr(args, "skip_rl_gate", False):
        os.environ["RL_HEALTH_BYPASS"] = "1"
    _apply_ui_cli_overrides(args)
    configure_runtime_logging(force=True)
    logging.getLogger(__name__).info("Runtime logging configured")

    stage_results: list[StageResult] = []
    exit_code = 0

    with _acquire_runtime_lock(RUNTIME_LOCK_PATH):
        try:
            if not args.skip_clean:
                run_stage("process-cleanup", kill_lingering_processes, stage_results)
            env = run_stage("ensure-environment", lambda: ensure_environment(args.config), stage_results)
            keypair_info = run_stage(
                "resolve-live-keypair",
                lambda: ensure_live_keypair(env.get("config")),
                stage_results,
            )
            env.update(keypair_info)
            cfg_path = env["config_path"]
            loaded_env = run_stage("load-production-env", _load_production_environment, stage_results)
            run_stage(
                "apply-prod-defaults",
                lambda: apply_production_defaults(env.get("config")),
                stage_results,
            )
            run_stage(
                "apply-ui-cli-overrides",
                lambda: _apply_ui_cli_overrides(args),
                stage_results,
            )
            run_stage("validate-keys", _validate_keys, stage_results)
            run_stage("write-env-manifest", lambda: _write_manifest(loaded_env), stage_results)
            run_stage("verify-onchain-account", verify_live_account, stage_results)
            run_stage("connectivity-check", _connectivity_check, stage_results)
            run_stage("connectivity-soak", _connectivity_soak, stage_results)
            run_stage(
                "rl-health-gate",
                lambda: rl_health_gate(config=env.get("config")),
                stage_results,
            )

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
