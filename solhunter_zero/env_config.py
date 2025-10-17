from __future__ import annotations

"""Helpers for configuring environment variables for SolHunter Zero."""

from pathlib import Path
import os
import ast

import tomllib

from . import env
from .logging_utils import log_startup
from .jito_auth import ensure_jito_auth
from .config import ENV_VARS
from .env_defaults import DEFAULTS
from .paths import ROOT
from .event_bus import DEFAULT_WS_URL
from .jsonutil import dumps as json_dumps, loads as json_loads

__all__ = [
    "configure_environment",
    "configure_startup_env",
    "report_env_changes",
]

_startup_env: dict[str, str] | None = None


def configure_environment(root: Path | None = None) -> dict[str, str]:
    """Load ``.env`` and apply defaults defined in :mod:`env_defaults`.

    GPU environment variables are intentionally not configured here. They are
    handled by :func:`solhunter_zero.device.initialize_gpu` during the launcher
    startup sequence to maintain a single source of truth.

    Parameters
    ----------
    root:
        Optional project root path.  Defaults to the repository root.

    Returns
    -------
    dict[str, str]
        Mapping of variables that were applied.
    """

    if root is None:
        root = ROOT
        if "site-packages" in root.parts:
            root = Path.cwd()
    env_file = Path(root) / ".env"

    def _is_placeholder(value: str) -> bool:
        if not value:
            return False
        lower = value.lower()
        if "your_" in lower or "example" in lower:
            return True
        if "invalid" in lower or "fake" in lower:
            return True
        if value.startswith("be_") and all(ch in "xX" for ch in value[3:]):
            return True
        if lower.startswith("bd"):
            return True
        return False

    def _sanitize_lines(lines: list[str]) -> tuple[list[str], set[str]]:
        sanitized: list[str] = []
        removed: set[str] = set()
        for line in lines:
            if "=" in line and not line.lstrip().startswith("#"):
                name, value = line.split("=", 1)
                if _is_placeholder(value.strip()):
                    removed.add(name)
                    continue
                sanitized.append(line if line.endswith("\n") else line + "\n")
            else:
                sanitized.append(line if line.endswith("\n") else line + "\n")
        return sanitized, removed

    removed_placeholders: set[str] = set()
    if not env_file.exists():
        example_file = Path(root) / ".env.example"
        env_file.parent.mkdir(parents=True, exist_ok=True)
        if example_file.exists():
            with example_file.open("r", encoding="utf-8") as fh:
                sanitized, removed_placeholders = _sanitize_lines(
                    fh.readlines()
                )
            with env_file.open("w", encoding="utf-8") as fh:
                fh.writelines(sanitized)
            log_startup(
                f"Created environment file {env_file} from {example_file}"
            )
            if removed_placeholders:
                report_env_changes(
                    {name: "" for name in removed_placeholders},
                    env_file,
                )
        else:
            env_file.touch()
            log_startup(f"Created environment file {env_file}")
    else:
        with env_file.open("r", encoding="utf-8") as fh:
            sanitized, removed_placeholders = _sanitize_lines(fh.readlines())
        if removed_placeholders:
            with env_file.open("w", encoding="utf-8") as fh:
                fh.writelines(sanitized)
            report_env_changes(
                {name: "" for name in removed_placeholders},
                env_file,
            )

    env.load_env_file(env_file)

    applied: dict[str, str] = {}
    for env_name in ENV_VARS.values():
        current = os.environ.get(env_name)
        if current and _is_placeholder(current):
            del os.environ[env_name]
            applied[env_name] = ""
    for name in removed_placeholders:
        if name == "SOLANA_RPC_URL":
            os.environ[name] = DEFAULTS.get(name, "")
            applied.setdefault(name, os.environ[name])
        else:
            os.environ[name] = ""
            applied.setdefault(name, "")
    file_updates: dict[str, str] = {}

    for env_key in ("AGENTS", "AGENT_WEIGHTS"):
        raw = os.environ.get(env_key)
        if not raw:
            continue
        try:
            json_loads(raw)
            continue
        except Exception:
            try:
                parsed = ast.literal_eval(raw)
            except Exception:
                continue
            if isinstance(parsed, (list, dict)):
                corrected = json_dumps(parsed)
                os.environ[env_key] = corrected
                applied[env_key] = corrected
                file_updates[env_key] = corrected
                log_startup(f"Migrated {env_key} value to JSON format")

    cfg_path = Path(root) / "config.toml"
    if cfg_path.exists():
        try:
            with cfg_path.open("rb") as fh:
                cfg = tomllib.load(fh)
        except Exception:
            cfg = {}
        for key, env_name in ENV_VARS.items():
            val = cfg.get(key)
            if val is None:
                continue
            if isinstance(val, bool):
                value_str = str(val).lower()
            elif isinstance(val, (list, dict)):
                value_str = json_dumps(val)
            else:
                value_str = str(val)
            if _is_placeholder(value_str) or not value_str:
                applied.setdefault(env_name, "")
                continue
            if env_name not in os.environ or not os.environ[env_name]:
                os.environ[env_name] = value_str
                applied[env_name] = value_str
                file_updates[env_name] = value_str

    if file_updates:
        lines = env_file.read_text().splitlines(True)
        updated: set[str] = set()
        for i, line in enumerate(lines):
            if "=" in line and not line.lstrip().startswith("#"):
                name, _ = line.split("=", 1)
                if name in file_updates:
                    lines[i] = f"{name}={file_updates[name]}\n"
                    updated.add(name)
        for name, value in file_updates.items():
            if name not in updated:
                lines.append(f"{name}={value}\n")
        with env_file.open("w", encoding="utf-8") as fh:
            fh.writelines(lines)
        updated_list = ", ".join(file_updates)
        log_startup(
            f"Updated environment file {env_file} with: {updated_list}"
        )
        report_env_changes(file_updates, env_file)

    for key, value in DEFAULTS.items():
        if key not in os.environ:
            os.environ[key] = value
        applied[key] = os.environ[key]

    broker_env = os.environ.get("BROKER_WS_URLS", "").strip()
    urls = [u.strip() for u in broker_env.split(",") if u.strip()]
    valid = broker_env and all(u.startswith(("ws://", "wss://")) for u in urls)
    if not valid:
        os.environ["BROKER_WS_URLS"] = DEFAULT_WS_URL
        applied["BROKER_WS_URLS"] = DEFAULT_WS_URL
        lines = env_file.read_text().splitlines(True)
        updated = False
        for i, line in enumerate(lines):
            if line.startswith("BROKER_WS_URLS="):
                lines[i] = f"BROKER_WS_URLS={DEFAULT_WS_URL}\n"
                updated = True
                break
        if not updated:
            lines.append(f"BROKER_WS_URLS={DEFAULT_WS_URL}\n")
        with env_file.open("w", encoding="utf-8") as fh:
            fh.writelines(lines)
        log_startup(
            f"Set default BROKER_WS_URLS to {DEFAULT_WS_URL}"
        )
        report_env_changes({"BROKER_WS_URLS": DEFAULT_WS_URL}, env_file)
    else:
        applied["BROKER_WS_URLS"] = broker_env

    ensure_jito_auth(env_file)

    # GPU-related environment variables are configured exclusively via
    # :func:`device.initialize_gpu` during launcher startup to keep a single
    # source of truth.  ``configure_environment`` deliberately avoids calling
    # :func:`device.ensure_gpu_env`.

    for key, value in applied.items():
        log_startup(f"{key}: {value}")

    return applied


def configure_startup_env(root: Path | None = None) -> dict[str, str]:
    """Configure the environment once during startup.

    Subsequent calls return the mapping from the initial invocation without
    reapplying configuration.
    """

    global _startup_env
    if _startup_env is None:
        _startup_env = configure_environment(root)
    return _startup_env


def report_env_changes(
    changes: dict[str, str], env_file: Path | None = None
) -> None:
    """Print environment variable *changes* for command-line feedback."""

    if not changes:
        return
    if env_file is not None:
        print(f"Updated environment file {env_file} with:")
    else:
        print("Environment variables applied:")
    for key, value in changes.items():
        print(f"{key}={value}")
