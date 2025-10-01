from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .paths import ROOT

MAX_STARTUP_LOG_SIZE = 1_000_000  # 1 MB
MAX_PREFLIGHT_LOG_SIZE = 1_000_000  # 1 MB

# Default log locations
STARTUP_LOG = ROOT / "logs" / "startup.log"
PREFLIGHT_LOG = ROOT / "preflight.log"


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
