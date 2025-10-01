"""Minimal JSON utilities with optional :mod:`orjson` support."""

try:
    import orjson  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    orjson = None

__all__ = ["loads", "dumps"]


def loads(data):
    """Parse JSON from ``data``.

    Parameters
    ----------
    data : str | bytes
        The JSON data to parse.

    Returns
    -------
    Any
        Parsed Python object.
    """
    if orjson is not None:
        if isinstance(data, str):
            data = data.encode()
        return orjson.loads(data)
    import json
    return json.loads(data)


def dumps(obj) -> str:
    """Serialize ``obj`` to a JSON formatted ``str``.

    Parameters
    ----------
    obj : Any
        Object to serialize.

    Returns
    -------
    str
        JSON string representation of ``obj``.
    """
    if orjson is not None:
        return orjson.dumps(obj).decode()
    import json
    return json.dumps(obj)
