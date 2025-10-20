from __future__ import annotations

import contextlib
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler, WatchedFileHandler
from pathlib import Path
from typing import Any, Iterable

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
    "%(asctime)sZ [%(levelname)s] %(name)s:%(lineno)d "
    "(pid=%(process)d tid=%(threadName)s) | %(message)s"
)
DEFAULT_RUNTIME_DATEFMT = "%Y-%m-%d %H:%M:%S"

_NOISY_LOGGERS: tuple[str, ...] = (
    "live-runtime",
    "asyncio",
    "websockets",
    "solana",
    "depth_service",
)

_warn_once_lock = threading.Lock()
_warn_once_last_emit: dict[str, float] = {}

try:  # pragma: no cover - optional dependency
    import orjson as _ORJSON  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - optional dependency
    _ORJSON = None


class _UTCFormatter(logging.Formatter):
    """Formatter that renders timestamps in UTC."""

    converter = time.gmtime


_LOG_RECORD_RESERVED = set(logging.LogRecord(None, 0, "", 0, "", (), None).__dict__.keys())


class JsonFormatter(logging.Formatter):
    """Structured logging formatter producing JSON payloads."""

    def format(self, record: logging.LogRecord) -> str:  # noqa: D401 - short summary sufficient
        payload: dict[str, Any] = {
            "ts": datetime.utcfromtimestamp(record.created).isoformat(timespec="milliseconds")
            + "Z",
            "level": record.levelname,
            "logger": record.name,
            "line": record.lineno,
            "msg": record.getMessage(),
            "process": record.process,
            "thread": record.threadName,
        }

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            payload["stack_info"] = record.stack_info

        for key, value in record.__dict__.items():
            if key in payload or key.startswith("_") or key in _LOG_RECORD_RESERVED:
                continue
            payload[key] = value

        if _ORJSON is not None:
            return _ORJSON.dumps(payload).decode()
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _stream_fileno(stream: Any) -> int | None:
    """Best-effort retrieval of ``stream.fileno()`` without raising."""

    if stream is None:
        return None
    try:
        fileno = stream.fileno()  # type: ignore[call-arg]
    except Exception:
        return None
    try:
        return int(fileno)
    except (TypeError, ValueError):
        return None


def _stream_matches_alias(
    stream: Any,
    aliases: set[Any],
    alias_fds: set[int],
) -> bool:
    """Return ``True`` when *stream* refers to an aliased stdout/stderr target."""

    if stream in aliases:
        return True
    fileno = _stream_fileno(stream)
    return fileno is not None and fileno in alias_fds


def setup_stdout_logging(
    *,
    level: int = logging.INFO,
    fmt: str | None = None,
    datefmt: str | None = None,
    propagate_off: Iterable[str] = _NOISY_LOGGERS,
) -> logging.StreamHandler:
    """Ensure a single ``StreamHandler`` to ``sys.stdout`` exists on the root logger."""

    root = logging.getLogger()
    root.setLevel(level)

    sentinel_key = "_solhunter_stdout_handler"
    existing = getattr(root, sentinel_key, None)

    stdout_aliases: set[Any] = {sys.stdout}
    stderr_aliases: set[Any] = {sys.stderr}
    stdout_alias_fds: set[int] = set()
    stderr_alias_fds: set[int] = set()

    def _register_alias(stream: Any, *, for_stdout: bool) -> None:
        if stream is None:
            return
        fileno = _stream_fileno(stream)
        if for_stdout:
            stdout_aliases.add(stream)
            if fileno is not None:
                stdout_alias_fds.add(fileno)
        else:
            stderr_aliases.add(stream)
            if fileno is not None:
                stderr_alias_fds.add(fileno)

    _register_alias(sys.stdout, for_stdout=True)
    _register_alias(sys.stderr, for_stdout=False)

    for attr in ("__stdout__", "__stderr__"):
        extra = getattr(sys, attr, None)
        if extra is None:
            continue
        if attr == "__stdout__":
            _register_alias(extra, for_stdout=True)
        else:
            _register_alias(extra, for_stdout=False)

    stream_handler: logging.StreamHandler | None = None
    if isinstance(existing, logging.StreamHandler) and existing in root.handlers:
        stream_handler = existing
    for handler in list(root.handlers):
        if not isinstance(handler, logging.StreamHandler):
            continue

        stream = getattr(handler, "stream", None)
        resolved_stream = stream if stream is not None else sys.stderr

        is_stdout_alias = _stream_matches_alias(
            resolved_stream, stdout_aliases, stdout_alias_fds
        )
        is_stderr_alias = _stream_matches_alias(
            resolved_stream, stderr_aliases, stderr_alias_fds
        )

        if is_stdout_alias and stream_handler is None:
            stream_handler = handler
            continue

        # ``logging.basicConfig`` may have attached stream handlers that point to
        # stdout/stderr (or their ``sys.__stdout__/sys.__stderr__`` fallbacks).
        # These extra handlers cause duplicate log emission. Remove only the
        # duplicates while keeping custom user-provided streams intact.
        if is_stdout_alias or is_stderr_alias:
            root.removeHandler(handler)
            with contextlib.suppress(Exception):  # pragma: no cover - best effort
                handler.close()

    if stream_handler is None:
        stream_handler = logging.StreamHandler(sys.stdout)
        root.addHandler(stream_handler)
    else:
        try:
            stream_handler.setStream(sys.stdout)
        except Exception:  # pragma: no cover - fall back to attribute assignment
            stream_handler.stream = sys.stdout  # type: ignore[attr-defined]

    stream_handler.setLevel(level)

    if fmt:
        stream_handler.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    for name in propagate_off:
        logging.getLogger(name).propagate = False

    setattr(root, sentinel_key, stream_handler)
    root.propagate = False

    return stream_handler


def warn_once_per(
    minutes: float,
    key: str,
    message: str,
    *args: Any,
    logger: logging.Logger | None = None,
    **kwargs: Any,
) -> bool:
    """Emit ``logger.warning`` for *message* at most once per *minutes* interval."""

    interval = max(0.0, minutes) * 60.0
    now = time.monotonic()

    with _warn_once_lock:
        last = _warn_once_last_emit.get(key)
        if last is not None and interval > 0 and now - last < interval:
            return False
        _warn_once_last_emit[key] = now

    target = logger or logging.getLogger()
    target.warning(message, *args, **kwargs)
    return True


def reset_warn_once_cache() -> None:
    """Clear cached emission timestamps for :func:`warn_once_per`."""

    with _warn_once_lock:
        _warn_once_last_emit.clear()


def setup_logging(
    log_name: str,
    *,
    path: Path | None = None,
    max_bytes: int | None = None,
    backup_count: int = 1,
) -> Path:
    """Prepare a log file for writing.

    ``log_name`` identifies the log (e.g. ``"startup"`` or ``"preflight"``). The
    corresponding ``<log_name>.log`` file is rotated if it exceeds ``max_bytes``;
    otherwise it is truncated. When rotated, up to ``backup_count`` historical
    files are preserved (e.g. ``startup.log.1``). The resolved log ``Path`` is
    returned.
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
                if backup_count > 0:
                    highest = path.with_suffix(path.suffix + f".{backup_count}")
                    if highest.exists():
                        try:
                            highest.unlink()
                        except OSError:
                            pass
                    for index in range(backup_count - 1, 0, -1):
                        src = path.with_suffix(path.suffix + f".{index}")
                        if src.exists():
                            dst = path.with_suffix(path.suffix + f".{index + 1}")
                            try:
                                src.replace(dst)
                            except OSError:
                                pass
                    first_backup = path.with_suffix(path.suffix + ".1")
                    path.replace(first_backup)
                else:
                    path.write_text("")
            else:
                path.write_text("")
        except OSError:
            pass

    return path


def rotate_startup_log(
    path: Path = STARTUP_LOG,
    max_bytes: int = MAX_STARTUP_LOG_SIZE,
    backup_count: int = 1,
) -> None:
    """Rotate or truncate the startup log before writing new output."""

    setup_logging("startup", path=path, max_bytes=max_bytes, backup_count=backup_count)


def rotate_preflight_log(
    path: Path = PREFLIGHT_LOG,
    max_bytes: int = MAX_PREFLIGHT_LOG_SIZE,
    backup_count: int = 1,
) -> None:
    """Rotate or truncate the preflight log before writing new output."""

    setup_logging("preflight", path=path, max_bytes=max_bytes, backup_count=backup_count)


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
    json_logs: bool | None = None,
    use_utc: bool = True,
    runtime_handler: str | None = None,
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

    env_json = os.getenv("LOG_JSON")
    if json_logs is None and env_json is not None:
        json_logs = env_json.strip().lower() in {"1", "true", "yes", "on"}
    json_logs = bool(json_logs)

    env_handler = os.getenv("LOG_RUNTIME_HANDLER")
    handler_choice_source = runtime_handler if runtime_handler is not None else env_handler
    handler_choice = (handler_choice_source or "rotating").strip().lower()
    if handler_choice not in {"rotating", "watched"}:
        handler_choice = "rotating"

    log_path = Path(logfile or os.getenv("LOG_FILE") or RUNTIME_LOG)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path = log_path.resolve()

    root = logging.getLogger()
    if force:
        for handler in list(root.handlers):
            root.removeHandler(handler)

    root.setLevel(resolved_level)

    if json_logs:
        formatter: logging.Formatter = JsonFormatter()
    elif use_utc:
        formatter = _UTCFormatter(resolved_format, datefmt=resolved_datefmt)
    else:
        formatter = logging.Formatter(resolved_format, datefmt=resolved_datefmt)

    file_handler: logging.Handler | None = None
    expected_handler = (
        WatchedFileHandler if handler_choice == "watched" else RotatingFileHandler
    )
    for handler in list(root.handlers):
        base = getattr(handler, "baseFilename", None)
        if base is None:
            continue
        if Path(base) == log_path:
            if not isinstance(handler, expected_handler):
                root.removeHandler(handler)
                with contextlib.suppress(Exception):
                    handler.close()
                continue
            if file_handler is None:
                file_handler = handler
                continue
            root.removeHandler(handler)
            with contextlib.suppress(Exception):
                handler.close()

    if file_handler is None:
        if handler_choice == "watched":
            file_handler = WatchedFileHandler(log_path, encoding="utf-8")
        else:
            file_handler = RotatingFileHandler(
                log_path,
                maxBytes=max_bytes or DEFAULT_RUNTIME_MAX_BYTES,
                backupCount=backup_count or DEFAULT_RUNTIME_BACKUP_COUNT,
                encoding="utf-8",
            )
        root.addHandler(file_handler)

    file_handler.setLevel(resolved_level)
    file_handler.setFormatter(formatter)

    stream_handler: logging.StreamHandler | None = None
    if console:
        stream_handler = setup_stdout_logging(
            level=resolved_level,
            fmt=None if json_logs else resolved_format,
            datefmt=None if json_logs else resolved_datefmt,
        )

    if stream_handler is not None:
        stream_handler.setLevel(resolved_level)
        if json_logs:
            stream_handler.setFormatter(JsonFormatter())
        elif use_utc:
            stream_handler.setFormatter(_UTCFormatter(resolved_format, datefmt=resolved_datefmt))
        # otherwise ``setup_stdout_logging`` already applies the formatter

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
        if _ORJSON is not None:
            return _ORJSON.dumps(normalized).decode()
        return json.dumps(normalized, ensure_ascii=True, sort_keys=True)
    except Exception:
        try:
            if _ORJSON is not None:
                return _ORJSON.dumps(str(value)).decode()
            return json.dumps(str(value), ensure_ascii=True)
        except Exception:
            return repr(value)
