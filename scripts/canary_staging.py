#!/usr/bin/env python3
"""Deploy to staging, run the smoke suite, and gate promotion on failures."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_ARTIFACT_ROOT = Path("artifacts") / "canary"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the staging canary workflow")
    parser.add_argument(
        "--env-file",
        default=".env.staging",
        help="Environment file to load before deployment and smoke checks.",
    )
    parser.add_argument(
        "--deploy-cmd",
        help=(
            "Command used to deploy the staging stack. If omitted a docker compose "
            "command is inferred when available."
        ),
    )
    parser.add_argument(
        "--skip-deploy",
        action="store_true",
        help="Skip the deploy step (assumes staging already running).",
    )
    parser.add_argument(
        "--smoke-cmd",
        help=(
            "Custom smoke command to run against the staging bus. Defaults to the "
            "preflight run_all.sh harness or the redis bus smoke probes."
        ),
    )
    parser.add_argument(
        "--redis-url",
        help="Override Redis URL for smoke probes (falls back to env file).",
    )
    parser.add_argument(
        "--bus-timeout",
        type=int,
        default=120,
        help="Seconds to wait for the staging bus to accept connections.",
    )
    parser.add_argument(
        "--command-timeout",
        type=int,
        default=900,
        help="Timeout for deploy and smoke commands (in seconds).",
    )
    parser.add_argument(
        "--artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
        help="Directory where canary logs and summary are written.",
    )
    return parser.parse_args()


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        env[key] = value
    return env


def _run_command(command: Iterable[str] | str, env: dict[str, str], log_path: Path, timeout: int) -> dict[str, object]:
    if isinstance(command, str):
        cmd = shlex.split(command)
    else:
        cmd = list(command)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.time()
    rc: int | None = None
    timed_out = False
    output_lines: list[str] = []
    with log_path.open("w", encoding="utf-8") as log:
        log.write("$ %s\n" % " ".join(cmd))
        log.flush()
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            text=True,
        )
        assert proc.stdout is not None
        try:
            for line in proc.stdout:
                log.write(line)
                output_lines.append(line.rstrip())
        finally:
            try:
                rc = proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                proc.kill()
                rc = proc.wait(timeout=30)
                log.write(f"\n[canary] command timed out after {timeout}s\n")
    duration = time.time() - start
    status = "success"
    if timed_out:
        status = "timeout"
    elif rc != 0:
        status = "failed"
    return {
        "command": cmd,
        "returncode": rc,
        "status": status,
        "duration": duration,
        "log": str(log_path),
        "tail": output_lines[-10:],
    }


def _redis_ping(redis_url: str, timeout: int) -> bool:
    try:
        import redis
    except Exception:  # pragma: no cover - optional dependency
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client = redis.Redis.from_url(redis_url)
            if client.ping():
                client.close()
                return True
        except Exception:
            time.sleep(2)
        else:
            time.sleep(2)
    return False


def _determine_deploy_command(args: argparse.Namespace, env_file: Path) -> list[str] | None:
    if args.skip_deploy:
        return None
    if args.deploy_cmd:
        return shlex.split(args.deploy_cmd)
    compose_candidates = (("docker", "compose"), ("docker-compose",))
    for candidate in compose_candidates:
        if shutil.which(candidate[0]):
            return list(candidate) + ["--env-file", str(env_file), "up", "-d", "--remove-orphans"]
    return None


def main() -> int:
    args = _parse_args()
    artifact_root = args.artifact_root
    artifact_root.mkdir(parents=True, exist_ok=True)
    summary_path = artifact_root / "staging_canary.json"

    env = dict(os.environ)
    env_updates = _load_env_file(Path(args.env_file))
    env.update(env_updates)

    redis_url = args.redis_url or env.get("REDIS_URL") or ""

    summary: dict[str, object] = {
        "started_at": datetime.now(tz=timezone.utc).isoformat(),
        "env_file": args.env_file,
        "artifact_root": str(artifact_root),
        "redis_url": redis_url or None,
        "steps": [],
    }

    deploy_cmd = _determine_deploy_command(args, Path(args.env_file))
    if deploy_cmd is not None:
        result = _run_command(deploy_cmd, env, artifact_root / "deploy.log", args.command_timeout)
        summary["steps"].append({"name": "deploy", **result})
        if result["status"] != "success":
            summary["status"] = "failed"
            summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
            print(json.dumps(summary, indent=2))
            return 1
    else:
        summary["steps"].append({"name": "deploy", "status": "skipped"})

    bus_ready = False
    if redis_url:
        bus_ready = _redis_ping(redis_url, args.bus_timeout)
    summary["bus_ready"] = bus_ready
    if redis_url and not bus_ready:
        summary["status"] = "failed"
        summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
        return 1

    smoke_commands: list[tuple[str, list[str] | str]] = []
    if args.smoke_cmd:
        smoke_commands.append(("smoke", args.smoke_cmd))
    else:
        run_all = Path("scripts/preflight/run_all.sh")
        if run_all.exists():
            smoke_commands.append(("preflight", ["bash", str(run_all)]))
        else:
            smoke_commands.extend(
                [
                    (
                        "bus-ping",
                        [sys.executable, "scripts/bus_smoke.py", "ping"]
                        + (["--redis-url", redis_url] if redis_url else []),
                    ),
                    (
                        "bus-keys",
                        [sys.executable, "scripts/bus_smoke.py", "keys"]
                        + (["--redis-url", redis_url] if redis_url else []),
                    ),
                ]
            )

    failures = False
    for name, command in smoke_commands:
        log_path = artifact_root / f"{name}.log"
        result = _run_command(command, env, log_path, args.command_timeout)
        summary["steps"].append({"name": name, **result})
        if result["status"] != "success":
            failures = True

    summary.setdefault("status", "passed" if not failures else "failed")
    summary["completed_at"] = datetime.now(tz=timezone.utc).isoformat()

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))

    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
