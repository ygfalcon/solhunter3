# Utility functions for runtime helpers.

from __future__ import annotations

from typing import Any, Coroutine, Iterable, List, Mapping, TypeVar

import asyncio
import importlib
import logging
import os
import sys
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

logger = logging.getLogger(__name__)


_TRUE_VALUES = {"1", "true", "yes", "y", "on", "enable", "enabled", "t"}
_FALSE_VALUES = {"0", "false", "no", "n", "off", "disable", "disabled", "f"}
_PLACEHOLDER_MARKERS = ("YOUR_KEY", "YOUR_HELIUS_KEY", "CHANGE_ME", "EXAMPLE", "REDACTED")
_SECRET_QUERY_KEYS = {"api_key", "apikey", "token", "auth", "secret", "password"}


def parse_bool_env(
    name: str,
    default: bool = False,
    *,
    overrides: Mapping[str, bool] | None = None,
    extra_true: Iterable[str] | None = None,
    extra_false: Iterable[str] | None = None,
    log_unknown: bool = False,
) -> bool:
    """Return the boolean value for environment variable ``name``.

    Additional truthy/falsey spellings can be supplied via ``extra_true`` and
    ``extra_false``.  ``overrides`` allows per-call mappings (after
    normalization) to accommodate application-specific aliases.  Unknown
    values fall back to ``default`` and can optionally be logged.
    """

    val = os.getenv(name)
    if val is None:
        return default
    norm = val.strip().lower()
    if overrides:
        lowered_overrides = {str(k).strip().lower(): v for k, v in overrides.items()}
        if norm in lowered_overrides:
            return lowered_overrides[norm]
    true_values = _TRUE_VALUES | {str(s).strip().lower() for s in (extra_true or [])}
    false_values = _FALSE_VALUES | {str(s).strip().lower() for s in (extra_false or [])}
    if norm in true_values:
        return True
    if norm in false_values:
        return False
    if log_unknown:
        logger.debug(
            "Ignoring unknown boolean env %s=%r; using default=%s", name, val, default
        )
    return default


def install_uvloop() -> bool:
    """Install ``uvloop`` if available and supported.

    Returns ``True`` when the policy is active (either newly installed or
    already present) and ``False`` otherwise.  The function skips unsupported
    platforms (non-CPython or Windows) and quietly returns when the optional
    dependency is missing.
    """
    if sys.implementation.name != "cpython":
        logger.debug("uvloop unsupported on interpreter %s; skipping", sys.implementation.name)
        return False
    if sys.platform.startswith("win"):
        logger.debug("uvloop unsupported on Windows; skipping")
        return False
    try:
        uvloop = importlib.import_module("uvloop")
    except Exception:  # pragma: no cover - optional dependency missing
        logger.debug("uvloop not installed")
        return False

    if getattr(asyncio, "uvloop_installed", False):
        return True

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio.uvloop_installed = True  # type: ignore[attr-defined]
    logger.debug("uvloop installed as event loop policy")
    return True


T = TypeVar("T")


def run_coro(
    coro: Coroutine[Any, Any, T] | T,
    *,
    name: str | None = None,
    shield: bool = False,
    log_exceptions: bool = True,
) -> T | asyncio.Task:
    """Run ``coro`` using the active loop if present.

    When called with no running event loop this function falls back to
    :func:`asyncio.run` and returns the coroutine result.  If a loop is
    already running, the coroutine is scheduled with
    :func:`asyncio.AbstractEventLoop.create_task` and the resulting task
    is returned.  Tasks can be optionally named, shielded from cancellation,
    and instrumented to log unhandled exceptions.
    """

    if not asyncio.iscoroutine(coro):
        return coro
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    if shield:

        async def _shielded() -> T:
            return await asyncio.shield(coro)

        task = loop.create_task(_shielded(), name=name)
    else:
        task = loop.create_task(coro, name=name)
    if log_exceptions:

        def _log_task_done(done_task: asyncio.Task) -> None:
            exc = done_task.exception()
            if exc is not None:
                logger.exception(
                    "Background task %s failed", done_task.get_name() or "<unnamed>", exc_info=exc
                )

        task.add_done_callback(_log_task_done)
    return task


def run_coro_sync(coro: Coroutine[Any, Any, T]) -> T:
    """Always block and return the result, even if a loop is already running."""

    if not asyncio.iscoroutine(coro):
        return coro  # type: ignore[return-value]
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    from concurrent.futures import Future, ThreadPoolExecutor

    fut: Future[T] = Future()

    def _runner() -> None:
        try:
            fut.set_result(asyncio.run(coro))
        except BaseException as exc:  # pragma: no cover - propagate to caller
            fut.set_exception(exc)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(_runner)
        return fut.result()


def _normalize_path(path: str) -> str:
    if not path:
        return ""
    segments: list[str] = []
    for segment in path.split("/"):
        if not segment or segment == ".":
            continue
        if segment == "..":
            if segments:
                segments.pop()
            continue
        segments.append(segment)
    return "/" + "/".join(segments) if segments else ""


def _normalize_url(
    url: str,
    *,
    allow_non_http: bool,
    redact_secrets: bool,
) -> str | None:
    try:
        parts = urlsplit(url.strip())
    except Exception:
        return None

    if not parts.scheme and not parts.netloc and "://" not in url:
        return _normalize_url(
            "https://" + url.strip(),
            allow_non_http=allow_non_http,
            redact_secrets=redact_secrets,
        )

    scheme = parts.scheme.lower() if parts.scheme else "https"
    if scheme not in {"http", "https"} and not allow_non_http:
        return None

    if not parts.netloc:
        return None

    host = parts.hostname.lower() if parts.hostname else ""
    if not host and parts.netloc:
        host = parts.netloc.lower()
    netloc = host
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if parts.username:
        auth = parts.username
        if parts.password:
            auth = f"{auth}:{parts.password}"
        netloc = f"{auth}@{netloc}"

    raw_path = parts.path or ""
    norm_path = _normalize_path(raw_path)

    if redact_secrets and parts.query:
        query_pairs = []
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            if key.lower() in _SECRET_QUERY_KEYS:
                query_pairs.append((key, "REDACTED"))
            else:
                query_pairs.append((key, value))
        query = urlencode(query_pairs, doseq=True)
    else:
        query = parts.query

    return urlunsplit((scheme, netloc, norm_path.rstrip("/") if norm_path else "", query, ""))


def sanitize_priority_urls(
    urls: Iterable[str] | None,
    *,
    allow_non_http: bool = False,
    redact_secrets: bool = True,
) -> List[str]:
    """Return ``urls`` without placeholders, empties, or duplicates.

    URLs are normalized (scheme/host lowercased, redundant slashes collapsed,
    and trailing slashes trimmed) before deduplication.  Query parameters whose
    keys match common secret markers are redacted when ``redact_secrets`` is
    enabled.  Non-HTTP(S) schemes are skipped unless ``allow_non_http`` is set.
    """

    seen: set[str] = set()
    cleaned: List[str] = []
    if not urls:
        return cleaned
    for raw in urls:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        if any(marker in text for marker in _PLACEHOLDER_MARKERS):
            continue
        normalized = _normalize_url(text, allow_non_http=allow_non_http, redact_secrets=redact_secrets)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        cleaned.append(normalized)
    return cleaned
