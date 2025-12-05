from __future__ import annotations

import asyncio
import os
import logging
import socket
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict
from urllib.parse import urlparse
import aiohttp
from aiohttp.abc import AbstractResolver
from aiohttp.resolver import DefaultResolver
import json as _json_std  # type: ignore

from .optional_imports import try_import

_json = try_import("orjson", stub=_json_std)  # type: ignore
USE_ORJSON = _json is not _json_std

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        return float(raw) if raw not in {None, ""} else float(default)
    except Exception:
        return float(default)


class HTTPError(Exception):
    """Raised when an HTTP request returns a non-success status code."""


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
                # Normalize HTTPError to URLError to match the docstring
                if isinstance(last_exc, urllib.error.HTTPError):
                    raise urllib.error.URLError(
                        f"HTTP {last_exc.code}: {last_exc.reason}"
                    ) from last_exc
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


def reload_static_dns(value: str | None = None) -> None:
    """Reload the static DNS mapping, optionally using ``value`` instead of env vars."""

    global STATIC_DNS_MAP
    source = value if value is not None else os.getenv("HTTP_STATIC_DNS", STATIC_DNS_DEFAULT)
    STATIC_DNS_MAP = _parse_static_dns(source)


class StaticResolver(AbstractResolver):
    def __init__(self, mapping: dict[str, list[str]], fallback: AbstractResolver | None = None) -> None:
        self._mapping = {host: list(ips) for host, ips in mapping.items() if ips}
        self._fallback = fallback or DefaultResolver()

    async def resolve(self, host: str, port: int = 0, family: int = 0) -> list[dict[str, Any]]:
        host_lower = host.lower()
        ips = self._mapping.get(host_lower)
        if ips:
            resolved = []
            for ip in ips:
                fam = socket.AF_INET6 if ":" in ip else socket.AF_INET
                requested_any = family in {getattr(socket, "AF_UNSPEC", 0), 0}
                if requested_any or fam == family:
                    resolved.append(
                        {
                            "hostname": host,
                            "host": ip,
                            "port": port,
                            "family": fam,
                            "proto": 0,
                            "flags": 0,
                        }
                    )
            if resolved:
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
            resolver: AbstractResolver | None = StaticResolver(STATIC_DNS_MAP) if STATIC_DNS_MAP else None
            try:
                connector = conn_cls(
                    limit=CONNECTOR_LIMIT,
                    limit_per_host=CONNECTOR_LIMIT_PER_HOST,
                    family=family,
                    resolver=resolver,
                )
            except TypeError:
                if resolver is not None:
                    try:
                        await resolver.close()
                    except Exception:
                        pass
                connector = None
        # Default headers with a friendly User-Agent to avoid 403s on some APIs
        ua = os.getenv("HTTP_USER_AGENT", "SolhunterZero/1.0 (+https://local)")
        try:
            timeout_total = float(os.getenv("HTTP_TIMEOUT_SEC", "15") or 15)
        except ValueError:
            timeout_total = 15.0
        trust_env = str(os.getenv("HTTP_TRUST_ENV", "")).lower() in {"1", "true", "yes"}
        if trust_env:
            logger.info("HTTP session will honor proxy settings from the environment")
        client_kwargs = {
            "headers": {"User-Agent": ua},
            "timeout": aiohttp.ClientTimeout(total=timeout_total),
            "trust_env": trust_env,
        }
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


# ---------------------------------------------------------------------------
# Host-level concurrency guards and retry hints
# ---------------------------------------------------------------------------


class HostCircuitOpenError(RuntimeError):
    """Raised when a host circuit breaker blocks new requests."""


@dataclass(slots=True)
class _HostConfig:
    host: str
    limit: int
    threshold: int
    cooldown: float
    max_attempts: int
    backoff: float


_HOST_RULES: tuple[_HostConfig, ...] = (
    _HostConfig("lite-api.jup.ag", limit=6, threshold=3, cooldown=20.0, max_attempts=2, backoff=0.25),
    _HostConfig("api.dexscreener.com", limit=8, threshold=3, cooldown=20.0, max_attempts=2, backoff=0.3),
    _HostConfig("public-api.birdeye.so", limit=6, threshold=3, cooldown=30.0, max_attempts=2, backoff=0.35),
    _HostConfig("api.meteora.ag", limit=4, threshold=3, cooldown=20.0, max_attempts=2, backoff=0.3),
    _HostConfig("api.dexlab.space", limit=2, threshold=2, cooldown=30.0, max_attempts=2, backoff=0.4),
    _HostConfig("pro-api.solscan.io", limit=4, threshold=3, cooldown=30.0, max_attempts=2, backoff=0.35),
    _HostConfig("public-api.solscan.io", limit=4, threshold=3, cooldown=30.0, max_attempts=2, backoff=0.35),
    _HostConfig("api.helius.xyz", limit=4, threshold=3, cooldown=20.0, max_attempts=2, backoff=0.25),
    _HostConfig("mainnet.helius-rpc.com", limit=4, threshold=3, cooldown=20.0, max_attempts=2, backoff=0.25),
    _HostConfig("hermes.pyth.network", limit=6, threshold=3, cooldown=20.0, max_attempts=2, backoff=0.25),
)


class _HostController:
    """Track concurrency and failures for a particular host."""

    __slots__ = ("config", "semaphore", "_failures", "_opened_until")

    def __init__(self, config: _HostConfig) -> None:
        self.config = config
        limit = max(1, config.limit) if config.limit > 0 else 1
        self.semaphore: asyncio.Semaphore = asyncio.Semaphore(limit)
        self._failures: list[float] = []
        self._opened_until: float = 0.0

    def allow(self) -> bool:
        now = time.monotonic()
        if self._opened_until and now < self._opened_until:
            return False
        if self._opened_until and now >= self._opened_until:
            self._opened_until = 0.0
            self._failures.clear()
        return True

    def record_success(self) -> None:
        self._failures.clear()
        self._opened_until = 0.0

    def record_failure(self) -> None:
        now = time.monotonic()
        self._failures.append(now)
        window_start = now - self.config.cooldown
        self._failures = [ts for ts in self._failures if ts >= window_start]
        if len(self._failures) >= self.config.threshold:
            self._opened_until = now + self.config.cooldown


_HOST_CONTROLLERS: Dict[str, _HostController] = {}


def _match_host_config(host: str) -> _HostConfig:
    host = host.lower()
    for rule in _HOST_RULES:
        if host == rule.host or host.endswith("." + rule.host):
            return rule
    default_limit = CONNECTOR_LIMIT_PER_HOST or 4
    return _HostConfig(host, limit=max(1, default_limit), threshold=3, cooldown=30.0, max_attempts=2, backoff=0.3)


def _controller_for(host: str) -> _HostController:
    controller = _HOST_CONTROLLERS.get(host)
    if controller is None:
        controller = _HostController(_match_host_config(host))
        _HOST_CONTROLLERS[host] = controller
    return controller


# ---------------------------------------------------------------------------
# Shared discovery breaker (HTTP provider failures)
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _SharedBreakerConfig:
    threshold: int
    window_sec: float
    cooldown_sec: float


class _SharedBreaker:
    def __init__(self, config: _SharedBreakerConfig) -> None:
        self.config = config
        self._failures: list[float] = []
        self._opened_until: float = 0.0
        self._openings: int = 0
        self._last_log: float = 0.0

    def _prune(self) -> None:
        now = time.monotonic()
        cutoff = now - self.config.window_sec
        if cutoff <= 0:
            return
        self._failures = [ts for ts in self._failures if ts >= cutoff]

    def _notify_open(self, duration: float) -> None:
        self._openings += 1
        now = time.monotonic()
        if now - self._last_log > 1.0:
            logger.warning(
                "Discovery HTTP circuit open for %.1fs after repeated failures",
                duration,
            )
            self._last_log = now

    @property
    def is_open(self) -> bool:
        self._prune()
        now = time.monotonic()
        if self._opened_until and now >= self._opened_until:
            self._opened_until = 0.0
            self._failures.clear()
            return False
        return bool(self._opened_until and now < self._opened_until)

    def record_success(self) -> None:
        self._failures.clear()
        if self._opened_until and self._opened_until <= time.monotonic():
            self._opened_until = 0.0

    def record_failure(self) -> None:
        self._prune()
        now = time.monotonic()
        self._failures.append(now)
        if len(self._failures) >= self.config.threshold:
            already_open = self.is_open
            self._opened_until = max(self._opened_until, now + self.config.cooldown_sec)
            if not already_open:
                self._notify_open(self.config.cooldown_sec)

    def snapshot(self) -> dict[str, float | int | bool]:
        self._prune()
        now = time.monotonic()
        remaining = max(0.0, self._opened_until - now) if self._opened_until else 0.0
        return {
            "open": self.is_open,
            "cooldown_remaining": remaining,
            "failure_count": len(self._failures),
            "openings": self._openings,
            "threshold": self.config.threshold,
            "window_sec": self.config.window_sec,
            "cooldown_sec": self.config.cooldown_sec,
        }


def _shared_breaker_config() -> _SharedBreakerConfig:
    return _SharedBreakerConfig(
        threshold=max(1, int(os.getenv("DISCOVERY_HTTP_BREAKER_FAILS", "5") or 5)),
        window_sec=max(1.0, _env_float("DISCOVERY_HTTP_BREAKER_WINDOW", 30.0)),
        cooldown_sec=max(1.0, _env_float("DISCOVERY_HTTP_BREAKER_COOLDOWN", 60.0)),
    )


_SHARED_BREAKER = _SharedBreaker(_shared_breaker_config())



@asynccontextmanager
async def host_request(url: str) -> AsyncIterator[_HostConfig]:
    """Context manager guarding a request to *url*'s host."""

    parsed = urlparse(url)
    host = parsed.hostname or parsed.netloc
    if not host:
        yield _match_host_config("unknown")
        return
    controller = _controller_for(host)
    if not controller.allow():
        raise HostCircuitOpenError(f"circuit open for host {host}")
    async with controller.semaphore:
        try:
            yield controller.config
        except Exception:
            controller.record_failure()
            raise
        else:
            controller.record_success()


@asynccontextmanager
async def provider_request(url: str) -> AsyncIterator[_HostConfig]:
    """Context manager that applies host guard rails and shared breaker."""

    if _SHARED_BREAKER.is_open:
        raise HostCircuitOpenError("shared discovery circuit open")
    async with host_request(url) as cfg:
        try:
            yield cfg
        except Exception:
            _SHARED_BREAKER.record_failure()
            raise
        else:
            _SHARED_BREAKER.record_success()


def http_breaker_state() -> dict[str, float | int | bool]:
    """Expose the shared HTTP breaker telemetry for monitoring/metrics."""

    return _SHARED_BREAKER.snapshot()


def host_retry_config(url: str) -> tuple[int, float]:
    """Return ``(max_attempts, backoff_seconds)`` for *url*."""

    parsed = urlparse(url)
    host = parsed.hostname or parsed.netloc
    if not host:
        return 1, 0.0
    config = _controller_for(host).config
    return max(1, config.max_attempts), max(0.0, config.backoff)


async def fetch_json(url: str, method: str = "GET", **kwargs: Any) -> Any:
    """Fetch *url* using *method* and return the parsed JSON body."""

    sess = await get_session()
    attempts, backoff = host_retry_config(url)
    attempt = 0
    last_error: Exception | None = None
    while attempt < attempts:
        try:
            async with provider_request(url):
                async with sess.request(method, url, **kwargs) as response:
                    if response.status >= 400:
                        text = await response.text()
                        raise HTTPError(f"{method} {url} -> {response.status}: {text[:300]}")
                    raw = await response.read()
                    return loads(raw)
        except HostCircuitOpenError:
            raise
        except Exception as exc:
            last_error = exc
            attempt += 1
            if attempt >= attempts:
                break
            await asyncio.sleep(backoff * (2 ** (attempt - 1)))
    if last_error:
        raise last_error
    raise RuntimeError(f"failed to fetch {url}")


__all__ = [
    "HTTPError",
    "dumps",
    "loads",
    "check_endpoint",
    "get_session",
    "close_session",
    "fetch_json",
    "host_request",
    "provider_request",
    "http_breaker_state",
    "host_retry_config",
    "HostCircuitOpenError",
]
