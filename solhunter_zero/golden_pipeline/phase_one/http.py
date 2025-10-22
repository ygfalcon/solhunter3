"""Shared HTTP client utilities for the Phase-One Golden Stream."""

from __future__ import annotations

import asyncio
import random
import time
from collections import Counter
from typing import Any, Dict, Mapping, MutableMapping, Optional
from urllib.parse import urlparse

import aiohttp

from .circuit import HostCircuitBreaker
from .rate_limit import HostRateLimiter


class CircuitOpen(RuntimeError):
    """Raised when a host circuit breaker is open."""


class HttpClient:
    """Shared aiohttp client with per-host rate limiting and caching."""

    def __init__(
        self,
        *,
        limiter: HostRateLimiter,
        circuit: HostCircuitBreaker,
        dns_ttl: float = 300.0,
        user_agent: str = "GoldenStream/1.0",
    ) -> None:
        self._limiter = limiter
        self._circuit = circuit
        self._dns_ttl = dns_ttl
        self._user_agent = user_agent
        self._session: aiohttp.ClientSession | None = None
        self._session_lock = asyncio.Lock()
        self._timeout = aiohttp.ClientTimeout(total=1.0, connect=0.3, sock_read=0.7)
        self._etag: MutableMapping[str, str] = {}
        self._modified: MutableMapping[str, str] = {}
        self._metrics_req = Counter()
        self._metrics_err = Counter()

    async def start(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                return
            connector = aiohttp.TCPConnector(ttl_dns_cache=self._dns_ttl, limit=0)
            headers = {
                "User-Agent": self._user_agent,
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
            }
            self._session = aiohttp.ClientSession(
                connector=connector,
                headers=headers,
                timeout=self._timeout,
            )

    async def close(self) -> None:
        async with self._session_lock:
            if self._session is not None and not self._session.closed:
                await self._session.close()
            self._session = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            await self.start()
        assert self._session is not None
        return self._session

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        params: Optional[Mapping[str, Any]] = None,
        json_body: Any | None = None,
        host_override: Optional[str] = None,
        allow_retry: bool = True,
        budget: float = 1.0,
    ) -> aiohttp.ClientResponse:
        session = await self._ensure_session()
        parsed = urlparse(url)
        host = host_override or parsed.hostname or ""
        if not host:
            raise ValueError(f"Cannot determine host for URL {url}")
        if await self._circuit.is_open(host):
            raise CircuitOpen(f"circuit open for host {host}")

        start = time.monotonic()
        attempt = 0
        while True:
            attempt += 1
            req_headers = dict(headers or {})
            cache_key = f"{method.upper()}:{url}"
            etag = self._etag.get(cache_key)
            modified = self._modified.get(cache_key)
            if etag and "If-None-Match" not in req_headers:
                req_headers["If-None-Match"] = etag
            if modified and "If-Modified-Since" not in req_headers:
                req_headers["If-Modified-Since"] = modified

            async with self._limiter.limiter(host):
                self._metrics_req[host] += 1
                try:
                    resp = await session.request(
                        method,
                        url,
                        headers=req_headers,
                        params=params,
                        json=json_body,
                    )
                except Exception as exc:
                    await self._circuit.record_failure(host)
                    self._metrics_err[host] += 1
                    if allow_retry and self._has_budget(start, budget):
                        await asyncio.sleep(self._jitter())
                        allow_retry = False
                        continue
                    raise

            if resp.status in {429} or resp.status >= 500:
                await self._circuit.record_failure(host)
                self._metrics_err[host] += 1
                if allow_retry and self._has_budget(start, budget):
                    await resp.release()
                    await asyncio.sleep(self._jitter())
                    allow_retry = False
                    continue
                return resp

            await self._circuit.record_success(host)
            etag_value = resp.headers.get("ETag")
            if etag_value:
                self._etag[cache_key] = etag_value
            modified_value = resp.headers.get("Last-Modified")
            if modified_value:
                self._modified[cache_key] = modified_value
            return resp

    @staticmethod
    def _jitter() -> float:
        return random.uniform(0.05, 0.15)

    @staticmethod
    def _has_budget(start: float, budget: float) -> bool:
        return (time.monotonic() - start) < budget

    async def metrics(self) -> Dict[str, Dict[str, float]]:
        data: Dict[str, Dict[str, float]] = {}
        for host in set(self._metrics_req) | set(self._metrics_err):
            data[host] = {
                "req_total": float(self._metrics_req.get(host, 0)),
                "err_total": float(self._metrics_err.get(host, 0)),
            }
        return data


