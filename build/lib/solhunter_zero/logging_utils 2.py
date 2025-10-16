from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

from .paths import ROOT

MAX_STARTUP_LOG_SIZE = 1_000_000  # 1 MB
MAX_PREFLIGHT_LOG_SIZE = 1_000_000  # 1 MB

# Default log locations
STARTUP_LOG = ROOT / "logs" / "startup.log"
PREFLIGHT_LOG = ROOT / "preflight.log"
RUNTIME_LOG = ROOT / "logs" / "live_main.log"

DEFAULT_RUNTIME_MAX_BYTES = 5_000_000
DEFAULT_RUNTIME_BACKUP_COUNT = 3
DEFAULT_RUNTIME_FORMAT = (
    "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d | %(message)s"
)
DEFAULT_RUNTIME_DATEFMT = "%Y-%m-%d %H:%M:%S"


def setup_logging(
    log_name: str,
    *,
    path: Path | None = None,
    max_bytes: int | None = None,
) -> Path:
    """Prepare a log file for writing.

    ``log_name`` identifies the log (e.g. ``"startup"`` or ``"preflight"``). The
    corresponding ``<log_name>.log`` file is rotated if it exceeds ``max_bytes``;
    otherwise it is truncated. The resolved log ``Path`` is returned.
    """

    if path is None:
        if log_name == "startup":
            path = STARTUP_LOG
        elif log_name == "preflight":
            path = PREFLIGHT_LOG
        else:  # pragma: no cover - defensive branch
            path = ROOT / f"{log_name}.log"

    if max_bytes is None:
        if log_name == "startup":
            max_bytes = MAX_STARTUP_LOG_SIZE
        elif log_name == "preflight":
            max_bytes = MAX_PREFLIGHT_LOG_SIZE
        else:  # pragma: no cover - defensive branch
            max_bytes = MAX_STARTUP_LOG_SIZE

    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        try:
            if path.stat().st_size > max_bytes:
                backup = path.with_suffix(path.suffix + ".1")
                path.replace(backup)
            else:
                path.write_text("")
        except OSError:
            pass

    return path


def rotate_startup_log(
    path: Path = STARTUP_LOG, max_bytes: int = MAX_STARTUP_LOG_SIZE
) -> None:
    """Rotate or truncate the startup log before writing new output."""

    setup_logging("startup", path=path, max_bytes=max_bytes)


def rotate_preflight_log(
    path: Path = PREFLIGHT_LOG, max_bytes: int = MAX_PREFLIGHT_LOG_SIZE
) -> None:
    """Rotate or truncate the preflight log before writing new output."""

    setup_logging("preflight", path=path, max_bytes=max_bytes)


def log_startup(message: str, path: Path = STARTUP_LOG) -> None:
    """Append *message* to ``startup.log`` with a timestamp."""
    try:
        timestamp = datetime.now().isoformat(timespec="seconds")
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(f"{timestamp} {message}\n")
    except OSError:
        pass


def _parse_log_level(value: str | None) -> int:
    if not value:
        return logging.INFO
    if isinstance(value, str):
        level = value.strip().upper()
        if level.isdigit():
            try:
                return int(level)
            except ValueError:
                return logging.INFO
        return getattr(logging, level, logging.INFO)
    if isinstance(value, int):
        return value
    return logging.INFO


def configure_runtime_logging(
    *,
    level: str | int | None = None,
    console: bool | None = None,
    logfile: str | Path | None = None,
    fmt: str | None = None,
    datefmt: str | None = None,
    max_bytes: int | None = None,
    backup_count: int | None = None,
    force: bool = False,
) -> Path:
    """Configure global logging handlers for the trading runtime."""

    env_level = os.getenv("LOG_LEVEL")
    resolved_level = _parse_log_level(level or env_level)

    env_console = os.getenv("LOG_CONSOLE")
    if console is None:
        if env_console is None:
            console = True
        else:
            console = env_console.strip().lower() in {"1", "true", "yes", "on"}

    env_format = os.getenv("LOG_FORMAT")
    env_date = os.getenv("LOG_DATEFMT")
    resolved_format = fmt or env_format or DEFAULT_RUNTIME_FORMAT
    resolved_datefmt = datefmt or env_date or DEFAULT_RUNTIME_DATEFMT

    env_max_bytes = os.getenv("LOG_MAX_BYTES")
    if max_bytes is None:
        try:
            max_bytes = int(env_max_bytes) if env_max_bytes else DEFAULT_RUNTIME_MAX_BYTES
        except ValueError:
            max_bytes = DEFAULT_RUNTIME_MAX_BYTES

    env_backup = os.getenv("LOG_BACKUP_COUNT")
    if backup_count is None:
        try:
            backup_count = int(env_backup) if env_backup else DEFAULT_RUNTIME_BACKUP_COUNT
        except ValueError:
            backup_count = DEFAULT_RUNTIME_BACKUP_COUNT

    log_path = Path(logfile or os.getenv("LOG_FILE") or RUNTIME_LOG)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    root = logging.getLogger()
    if force:
        for handler in list(root.handlers):
            root.removeHandler(handler)
    if root.handlers:
        return log_path

    root.setLevel(resolved_level)

    formatter = logging.Formatter(resolved_format, datefmt=resolved_datefmt)

    handlers: list[logging.Handler] = []
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=max_bytes or DEFAULT_RUNTIME_MAX_BYTES,
        backupCount=backup_count or DEFAULT_RUNTIME_BACKUP_COUNT,
        encoding="utf-8",
    )
    handlers.append(file_handler)

    if console:
        stream = logging.StreamHandler(sys.stdout)
        handlers.append(stream)

    for handler in handlers:
        handler.setFormatter(formatter)
        root.addHandler(handler)

    root.debug("Logging initialised", extra={"log_file": str(log_path)})
    return log_path


def _normalize_for_log(value: Any, *, max_string: int) -> Any:
    if isinstance(value, dict):
        return {
            str(k): _normalize_for_log(v, max_string=max_string)
            for k, v in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_for_log(v, max_string=max_string) for v in value]
    if isinstance(value, set):
        normalized = [
            _normalize_for_log(v, max_string=max_string) for v in value
        ]
        try:
            return sorted(normalized, key=lambda item: str(item))
        except Exception:
            return normalized
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    if isinstance(value, str):
        if len(value) <= max_string:
            return value
        preview = value[: max_string]
        return f"{preview}...({len(value)} chars)"
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return str(value)


def serialize_for_log(value: Any, *, max_string: int = 256) -> str:
    """Return a JSON-formatted string safe for logging."""
    try:
        normalized = _normalize_for_log(value, max_string=max_string)
        return json.dumps(normalized, ensure_ascii=True, sort_keys=True)
    except Exception:
        try:
            return json.dumps(str(value), ensure_ascii=True)
        except Exception:
            return repr(value)
