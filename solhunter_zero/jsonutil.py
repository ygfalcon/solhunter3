from __future__ import annotations

"""Minimal JSON utilities with optional orjson, tuned for speed and parity."""

from typing import Any, Callable

try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    orjson = None

__all__ = ["loads", "dumps", "dumps_bytes"]


def loads(data: str | bytes) -> Any:
    """Parse JSON from ``data`` into Python objects."""
    if orjson is not None:
        if isinstance(data, str):
            data = data.encode("utf-8")
        return orjson.loads(data)
    import json
    if isinstance(data, bytes):
        data = data.decode("utf-8")
    return json.loads(data)


def _resolve_orjson_opts(
    sort_keys: bool,
    indent: int | None,
    allow_numpy: bool,
    allow_non_str_keys: bool,
) -> int:
    if orjson is None:
        return 0
    opts = 0
    if indent:
        opts |= getattr(orjson, "OPT_INDENT_2", 0)
    if sort_keys:
        opts |= getattr(orjson, "OPT_SORT_KEYS", 0)
    if allow_numpy:
        opts |= getattr(orjson, "OPT_SERIALIZE_NUMPY", 0)
    if allow_non_str_keys:
        opts |= getattr(orjson, "OPT_NON_STR_KEYS", 0)
    return opts


def dumps_bytes(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    default: Callable[[Any], Any] | None = None,
    allow_numpy: bool = False,
    allow_non_str_keys: bool = False,
) -> bytes:
    """Serialize ``obj`` to a JSON byte string."""
    if orjson is not None:
        opts = _resolve_orjson_opts(sort_keys, indent, allow_numpy, allow_non_str_keys)
        return orjson.dumps(obj, option=opts, default=default)
    import json
    text = json.dumps(
        obj,
        sort_keys=sort_keys,
        indent=indent if indent else None,
        default=default,
        separators=None if indent else (",", ":"),
        ensure_ascii=False,
    )
    return text.encode("utf-8")


def dumps(
    obj: Any,
    *,
    sort_keys: bool = False,
    indent: int | None = None,
    default: Callable[[Any], Any] | None = None,
    allow_numpy: bool = False,
    allow_non_str_keys: bool = False,
) -> str:
    """Serialize ``obj`` to a JSON formatted ``str``."""
    return dumps_bytes(
        obj,
        sort_keys=sort_keys,
        indent=indent,
        default=default,
        allow_numpy=allow_numpy,
        allow_non_str_keys=allow_non_str_keys,
    ).decode("utf-8")
