from __future__ import annotations

import os
import logging
import socket
from typing import Any
import aiohttp
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import DefaultResolver
import json as _json_std  # type: ignore

from .optional_imports import try_import

_json = try_import("orjson", stub=_json_std)  # type: ignore
USE_ORJSON = _json is not _json_std

logger = logging.getLogger(__name__)


def dumps(obj: object) -> bytes:
    """Serialize *obj* to JSON bytes using ``orjson`` when available."""
    if USE_ORJSON:
        return _json.dumps(obj)
    return _json.dumps(obj).encode()


def loads(data: str | bytes) -> object:
    """Deserialize JSON *data* using ``orjson`` when available."""
    if USE_ORJSON:
        if isinstance(data, str):
            data = data.encode()
        return _json.loads(data)
    if isinstance(data, bytes):
        data = data.decode()
    return _json.loads(data)


def check_endpoint(url: str, retries: int = 3) -> None:
    """Send a ``HEAD`` request to *url* ensuring it is reachable.

    The request is attempted up to ``retries`` times using exponential backoff
    (1s, 2s, ...).  For providers that block ``HEAD`` requests, we fall back to
    issuing a lightweight ``GET`` (with a ``Range`` header where supported)
    before ultimately raising :class:`urllib.error.URLError`.
    """

    import time
    import urllib.error
    import urllib.request

    def _head() -> None:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=5):  # nosec B310
            return

    def _fallback_get() -> None:
        req = urllib.request.Request(url, method="GET")
        req.add_header("Range", "bytes=0-0")
        with urllib.request.urlopen(req, timeout=5):  # nosec B310
            return

    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            _head()
            return
        except urllib.error.HTTPError as exc:  # pragma: no cover - provider quirks
            last_exc = exc
            if exc.code < 500:
                try:
                    _fallback_get()
                except urllib.error.HTTPError as get_exc:
                    last_exc = get_exc
                    if 200 <= get_exc.code < 500:
                        return
                except urllib.error.URLError as get_exc:
                    last_exc = get_exc
                else:
                    return
        except urllib.error.URLError as exc:  # pragma: no cover - network failure
            last_exc = exc

        if attempt == retries - 1:
            if last_exc is not None:
                raise last_exc
            raise urllib.error.URLError("endpoint check failed")

        wait = 2**attempt
        err = last_exc or Exception("unknown error")
        logger.warning(
            f"Attempt {attempt + 1} failed for {url}: {err}. Retrying in {wait} seconds..."
        )
        time.sleep(wait)

# Maintain a session per event loop to avoid cross-loop usage errors when
# running multiple asyncio loops in different threads.
import asyncio, weakref
_SESSIONS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, aiohttp.ClientSession]" = weakref.WeakKeyDictionary()

# Connector limits are configurable via environment variables.
CONNECTOR_LIMIT = int(os.getenv("HTTP_CONNECTOR_LIMIT", "0") or 0)
CONNECTOR_LIMIT_PER_HOST = int(os.getenv("HTTP_CONNECTOR_LIMIT_PER_HOST", "0") or 0)
def _parse_static_dns(value: str | None) -> dict[str, list[str]]:
    mapping: dict[str, list[str]] = {}
    if not value:
        return mapping
    for entry in value.split(";"):
        chunk = entry.strip()
        if not chunk or "=" not in chunk:
            continue
        host, _, ips = chunk.partition("=")
        host = host.strip().lower()
        if not host or not ips:
            continue
        addr_list = [ip.strip() for ip in ips.split(",") if ip.strip()]
        if addr_list:
            mapping[host] = addr_list
    return mapping


STATIC_DNS_DEFAULT = ""
STATIC_DNS_MAP = _parse_static_dns(os.getenv("HTTP_STATIC_DNS", STATIC_DNS_DEFAULT))


class StaticResolver(AbstractResolver):
    def __init__(self, mapping: dict[str, list[str]], fallback: AbstractResolver | None = None) -> None:
        self._mapping = {host: list(ips) for host, ips in mapping.items() if ips}
        self._fallback = fallback or DefaultResolver()

    async def resolve(self, host: str, port: int = 0, family: int = 0) -> list[dict[str, Any]]:  # type: ignore[name-defined]
        host_lower = host.lower()
        ips = self._mapping.get(host_lower)
        if ips:
            resolved = []
            for ip in ips:
                resolved.append(
                    {
                        "hostname": host,
                        "host": ip,
                        "port": port,
                        "family": socket.AF_INET,
                        "proto": 0,
                        "flags": 0,
                    }
                )
            return resolved
        return await self._fallback.resolve(host, port, family)

    async def close(self) -> None:
        await self._fallback.close()

async def get_session() -> aiohttp.ClientSession:
    """Return an aiohttp session bound to the current event loop."""
    loop = asyncio.get_running_loop()
    sess = _SESSIONS.get(loop)
    if sess is None or getattr(sess, "closed", False):
        conn_cls = getattr(aiohttp, "TCPConnector", None)
        connector = None
        if conn_cls is not object and conn_cls is not None:
            force_ipv4 = str(os.getenv("HTTP_FORCE_IPV4", "")).lower() in {"1", "true", "yes"}
            family = socket.AF_INET if force_ipv4 else getattr(socket, "AF_UNSPEC", 0)
            try:
                resolver: AbstractResolver | None = StaticResolver(STATIC_DNS_MAP) if STATIC_DNS_MAP else None
                connector = conn_cls(
                    limit=CONNECTOR_LIMIT,
                    limit_per_host=CONNECTOR_LIMIT_PER_HOST,
                    family=family,
                    resolver=resolver,
                )
            except TypeError:
                connector = None
        # Default headers with a friendly User-Agent to avoid 403s on some APIs
        ua = os.getenv("HTTP_USER_AGENT", "SolhunterZero/1.0 (+https://local)")
        client_kwargs = {"headers": {"User-Agent": ua}}
        if connector is not None:
            client_kwargs["connector"] = connector
        try:
            sess = aiohttp.ClientSession(**client_kwargs)
        except TypeError as exc:
            # Some test doubles provide simplified constructors without keyword args
            if connector is not None:
                close = getattr(connector, "close", None)
                if callable(close):
                    try:
                        result = close()
                        if asyncio.iscoroutine(result):
                            loop.create_task(result)
                    except Exception:
                        pass
            client_kwargs.pop("connector", None)
            try:
                sess = aiohttp.ClientSession(**client_kwargs)
            except TypeError as exc2:
                if "unexpected keyword" in str(exc2):
                    sess = aiohttp.ClientSession()
                else:
                    raise
        _SESSIONS[loop] = sess
    return sess

async def close_session() -> None:
    """Close all known aiohttp sessions."""
    to_close = list(_SESSIONS.values())
    _SESSIONS.clear()
    for sess in to_close:
        try:
            if not getattr(sess, "closed", False):
                await sess.close()
        except Exception:
            pass
    try:
        from .depth_client import close_mmap, close_ipc_clients
        close_mmap()
        await close_ipc_clients()
    except Exception:
        pass
