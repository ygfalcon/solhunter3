#!/usr/bin/env python3
"""Utility to bootstrap the SolHunter runtime in paper or live mode."""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from solhunter_zero.logging_utils import setup_stdout_logging


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch SolHunter runtime controller")
    parser.add_argument("--mode", choices={"paper", "live"}, required=True)
    parser.add_argument("--micro", type=int, default=0, choices=(0, 1), help="Micro mode flag")
    parser.add_argument("--config", default=None, help="Optional config path to forward to orchestrator")
    parser.add_argument("--notify", default=None, help="Path to write readiness signal")
    parser.add_argument("--canary-budget", dest="canary_budget", default=None, help="Optional canary bankroll cap")
    parser.add_argument("--canary-risk", dest="canary_risk", default=None, help="Optional canary risk cap")
    parser.add_argument("--no-http", action="store_true", help="Disable UI HTTP server")
    parser.add_argument("--skip-clean", action="store_true", help="Skip legacy process cleanup stage")
    parser.add_argument("--skip-load-production-env", action="store_true", help="Skip loading production environment files")
    parser.add_argument("--skip-apply-prod-defaults", action="store_true", help="Skip applying production default environment overrides")
    parser.add_argument("--skip-validate-keys", action="store_true", help="Skip provider key validation")
    parser.add_argument("--skip-connectivity-check", action="store_true", help="Skip runtime connectivity probes")
    parser.add_argument("--skip-connectivity-soak", action="store_true", help="Skip extended connectivity soak test")
    return parser.parse_args(argv)


def setup_logging() -> None:
    setup_stdout_logging(
        fmt="%(asctime)s %(levelname)s [live-runtime] %(message)s",
    )


def _set_runtime_env(args: argparse.Namespace) -> None:
    os.environ.setdefault("NEW_RUNTIME", "1")
    os.environ.setdefault("FLASK_ENV", "production")
    os.environ.setdefault("FLASK_DEBUG", "0")
    os.environ.setdefault("EVENT_BUS_URL", "ws://127.0.0.1:8779")
    os.environ.setdefault("BROKER_CHANNEL", "solhunter-events-v3")
    default_redis = "redis://localhost:6379/1"
    for key in ("REDIS_URL", "MINT_STREAM_REDIS_URL", "MEMPOOL_STREAM_REDIS_URL", "AMM_WATCH_REDIS_URL"):
        os.environ.setdefault(key, default_redis)
    for key in ("MINT_STREAM_BROKER_CHANNEL", "MEMPOOL_STREAM_BROKER_CHANNEL", "AMM_WATCH_BROKER_CHANNEL"):
        os.environ.setdefault(key, os.environ["BROKER_CHANNEL"])
    os.environ["MODE"] = "live" if args.mode == "live" else "paper"
    os.environ["MICRO_MODE"] = str(args.micro)
    # Running in paper mode should never touch the live executor
    if args.mode == "paper":
        os.environ.setdefault("PAPER_TRADING", "1")
        os.environ.setdefault("LIVE_TRADING_DISABLED", "1")
        os.environ.setdefault("SHADOW_EXECUTOR_ONLY", "1")
    else:
        # Paper specific toggles must not leak into live mode
        for key in ("PAPER_TRADING", "LIVE_TRADING_DISABLED", "SHADOW_EXECUTOR_ONLY"):
            if key in os.environ:
                os.environ.pop(key)
    if args.canary_budget is not None:
        os.environ["CANARY_BUDGET_USD"] = str(args.canary_budget)
    if args.canary_risk is not None:
        os.environ["CANARY_RISK_CAP"] = str(args.canary_risk)


def _validate_environment() -> None:
    forbidden_tokens = ("${", "YOUR_", "REDACTED")
    offenders: list[str] = []
    for key, value in os.environ.items():
        if not isinstance(value, str):
            continue
        if any(token in value for token in forbidden_tokens):
            offenders.append(key)
    if offenders:
        offenders.sort()
        logging.error("Keys invalid (placeholder): %s", ", ".join(offenders))
        raise SystemExit(2)


def _emit_runtime_manifest() -> None:
    default_redis = "redis://localhost:6379/1"
    default_bus = "ws://127.0.0.1:8779"

    channel = os.environ.setdefault("BROKER_CHANNEL", "solhunter-events-v3")
    bus_url = os.environ.setdefault("EVENT_BUS_URL", default_bus)
    redis_keys = [
        "REDIS_URL",
        "MINT_STREAM_REDIS_URL",
        "MEMPOOL_STREAM_REDIS_URL",
        "AMM_WATCH_REDIS_URL",
    ]
    canonical_url: str | None = None

    for key in redis_keys:
        raw = os.environ.get(key) or default_redis
        os.environ[key] = raw
        candidate = raw if "://" in raw else f"redis://{raw}"
        parsed = urlparse(candidate)
        scheme = parsed.scheme or "redis"
        if scheme not in {"redis", "rediss"}:
            raise SystemExit(
                f"Invalid Redis URL scheme for {key}: {scheme!r}; expected redis:// or rediss://"
            )
        host = parsed.hostname or "localhost"
        port = parsed.port or 6379
        segment = (parsed.path or "/").lstrip("/").split("/", 1)[0]
        try:
            db_index = int(segment) if segment else 0
        except ValueError as exc:  # pragma: no cover - defensive
            raise SystemExit(f"Invalid Redis URL for {key}: {raw}") from exc
        if db_index != 1:
            raise SystemExit(
                f"{key} targets Redis database {db_index}; configure Redis database 1 for all runtime services."
            )
        normalized = f"{scheme}://{host}:{port}/{db_index}"
        os.environ[key] = normalized
        if canonical_url is None:
            canonical_url = normalized
        elif normalized != canonical_url:
            raise SystemExit(
                "All runtime components must share the same Redis endpoint. Set REDIS_URL, MINT_STREAM_REDIS_URL, "
                "MEMPOOL_STREAM_REDIS_URL, and AMM_WATCH_REDIS_URL to the same host:port/1 value."
            )

    for key in ("MINT_STREAM_BROKER_CHANNEL", "MEMPOOL_STREAM_BROKER_CHANNEL", "AMM_WATCH_BROKER_CHANNEL"):
        value = os.environ.get(key)
        if value and value != channel:
            raise SystemExit(
                f"{key}={value} does not match BROKER_CHANNEL={channel}; use a single channel for all producers."
            )
        os.environ.setdefault(key, channel)

    manifest = (
        "RUNTIME_MANIFEST "
        f"channel={channel} "
        f"redis={os.environ.get('REDIS_URL', default_redis)} "
        f"mint_stream={os.environ.get('MINT_STREAM_REDIS_URL', default_redis)} "
        f"mempool={os.environ.get('MEMPOOL_STREAM_REDIS_URL', default_redis)} "
        f"amm_watch={os.environ.get('AMM_WATCH_REDIS_URL', default_redis)} "
        f"bus={bus_url}"
    )
    logging.info(manifest)


async def _run_controller(args: argparse.Namespace) -> int:
    from solhunter_zero.runtime.orchestrator import RuntimeOrchestrator

    _set_runtime_env(args)
    _validate_environment()
    _emit_runtime_manifest()
    skip_stages = {
        name
        for flag, name in [
            (args.skip_clean, "process-cleanup"),
            (args.skip_load_production_env, "load-production-env"),
            (args.skip_apply_prod_defaults, "apply-prod-defaults"),
            (args.skip_validate_keys, "validate-keys"),
            (args.skip_connectivity_check, "connectivity-check"),
            (args.skip_connectivity_soak, "connectivity-soak"),
        ]
        if flag
    }
    orch = RuntimeOrchestrator(
        config_path=args.config,
        run_http=not args.no_http,
        skip_stages=skip_stages,
    )

    stop_event = asyncio.Event()

    def _signal_handler(signum: int, _frame: Optional[object] = None) -> None:
        logging.info("Signal %s received; shutting down runtime", signum)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda s=sig: _signal_handler(s))
        except NotImplementedError:  # pragma: no cover - Windows fallback
            signal.signal(sig, _signal_handler)

    try:
        logging.info("Starting runtime orchestrator (mode=%s, micro=%s)", args.mode, args.micro)
        await orch.start()
        logging.info("Runtime orchestrator ready (mode=%s)", args.mode)
        print(f"RUNTIME_READY mode={args.mode}", flush=True)
        if args.notify:
            try:
                Path(args.notify).parent.mkdir(parents=True, exist_ok=True)
                Path(args.notify).write_text("ready")
            except OSError as exc:  # pragma: no cover - best effort logging
                logging.warning("Unable to write notify file %s: %s", args.notify, exc)
        await stop_event.wait()
    except Exception:
        logging.exception("Runtime orchestrator failed")
        return 1
    finally:
        try:
            await orch.stop_all()
        except Exception:
            logging.exception("Error during orchestrator shutdown")
    logging.info("Runtime orchestrator stopped")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    setup_logging()
    try:
        return asyncio.run(_run_controller(args))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main(sys.argv[1:]))
