"""Connectivity verification utilities for production bootstraps."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import random
import socket
import statistics
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, MutableMapping
from urllib.parse import urlparse

import aiohttp
import websockets

from solhunter_zero.http import get_session

logger = logging.getLogger(__name__)


@dataclass
class ConnectivityResult:
    name: str
    target: str
    ok: bool
    latency_ms: float | None = None
    status: str | None = None
    status_code: int | None = None
    error: str | None = None
    attempts: int = 0


@dataclass
class ConnectivitySoakSummary:
    started_at: float
    finished_at: float
    duration: float
    metrics: Dict[str, Dict[str, Any]]
    reconnect_count: int = 0


def _percentile(values: Iterable[float], pct: float) -> float | None:
    seq = list(values)
    if not seq:
        return None
    try:
        return statistics.quantiles(seq, n=100, method="inclusive")[int(pct) - 1]
    except Exception:  # pragma: no cover - defensive
        seq.sort()
        index = max(0, min(len(seq) - 1, int(len(seq) * pct / 100)))
        return seq[index]


class ConnectivityChecker:
    """Probe critical endpoints with retries, jitter, and circuit breakers."""

    def __init__(
        self,
        *,
        env: Mapping[str, str] | None = None,
        http_timeout: float | None = None,
        redis_timeout: float | None = None,
        breaker_threshold: int = 3,
        breaker_cooldown: float = 30.0,
    ) -> None:
        self.env = dict(env or os.environ)
        self.http_timeout = http_timeout or float(self.env.get("CONNECTIVITY_HTTP_TIMEOUT", "6"))
        self.redis_timeout = redis_timeout or float(self.env.get("CONNECTIVITY_REDIS_TIMEOUT", "3"))
        self.backoff_base = float(self.env.get("CONNECTIVITY_BACKOFF_BASE", "0.5"))
        self.backoff_cap = float(self.env.get("CONNECTIVITY_BACKOFF_CAP", "8"))
        self.max_attempts = int(self.env.get("CONNECTIVITY_MAX_ATTEMPTS", "4"))
        self.breaker_threshold = breaker_threshold
        self.breaker_cooldown = breaker_cooldown
        self._breaker_state: MutableMapping[str, float] = {}
        self._failure_counts: MutableMapping[str, int] = defaultdict(int)
        self._metrics: MutableMapping[str, Dict[str, Any]] = defaultdict(
            lambda: {"latencies": [], "errors": Counter(), "statuses": Counter()}
        )
        self.targets = self._build_targets()

    # ------------------------------------------------------------------
    # Target configuration
    # ------------------------------------------------------------------

    def _build_targets(self) -> list[dict[str, Any]]:
        env = self.env
        rpc = env.get("SOLANA_RPC_URL") or env.get("HELIUS_RPC_URL")
        ws = env.get("SOLANA_WS_URL") or env.get("HELIUS_WS_URL")
        das_base = env.get("DAS_BASE_URL") or "https://api.helius.xyz/v1"
        das = das_base.rstrip("/")
        rest = env.get("HELIUS_PRICE_REST_URL") or f"{das}/nft-events"
        redis_url = env.get("REDIS_URL") or "redis://127.0.0.1:6379/0"
        ui_ws = env.get("UI_WS_URL")
        if not ui_ws:
            host = env.get("UI_HOST", "127.0.0.1")
            port = env.get("UI_PORT", "5001")
            ui_ws = f"ws://{host}:{port}/ws"
        ws_gateway = env.get("GATEWAY_WS_URL") or env.get("WS_GATEWAY_URL")
        targets: list[dict[str, Any]] = []
        if rpc:
            targets.append({"name": "solana-rpc", "type": "http", "url": rpc})
        if ws:
            targets.append({"name": "solana-ws", "type": "ws", "url": ws})
        if das:
            targets.append({"name": "helius-das", "type": "http", "url": f"{das}/searchAssets"})
        if rest:
            targets.append({"name": "helius-rest", "type": "http", "url": rest})
        if redis_url:
            targets.append({"name": "redis", "type": "redis", "url": redis_url})
        if ui_ws:
            targets.append({"name": "ui-ws", "type": "ws", "url": ui_ws})
        if ws_gateway:
            targets.append({"name": "ws-gateway", "type": "ws", "url": ws_gateway})
        return targets

    # ------------------------------------------------------------------
    # Metrics helpers
    # ------------------------------------------------------------------

    def _metrics_for(self, name: str) -> Dict[str, Any]:
        return self._metrics[name]

    def _record_latency(self, name: str, latency_ms: float) -> None:
        metrics = self._metrics_for(name)
        metrics.setdefault("latencies", []).append(latency_ms)

    def _record_error(self, name: str, key: str) -> None:
        metrics = self._metrics_for(name)
        metrics.setdefault("errors", Counter())[key] += 1

    def _record_status(self, name: str, status_code: int) -> None:
        metrics = self._metrics_for(name)
        metrics.setdefault("statuses", Counter())[status_code] += 1

    # ------------------------------------------------------------------
    # Circuit breaker
    # ------------------------------------------------------------------

    def _breaker_allows(self, name: str) -> bool:
        open_until = self._breaker_state.get(name)
        if open_until is None:
            return True
        if time.monotonic() >= open_until:
            self._breaker_state.pop(name, None)
            self._failure_counts[name] = 0
            logger.info("Circuit breaker for %s reset", name)
            return True
        return False

    def _register_failure(self, name: str) -> None:
        count = self._failure_counts[name] + 1
        self._failure_counts[name] = count
        if count >= self.breaker_threshold:
            cooldown = time.monotonic() + self.breaker_cooldown
            self._breaker_state[name] = cooldown
            logger.warning(
                "Circuit breaker opened for %s after %d failures", name, count
            )

    def _register_success(self, name: str) -> None:
        self._failure_counts[name] = 0
        self._breaker_state.pop(name, None)

    # ------------------------------------------------------------------
    # Backoff helper
    # ------------------------------------------------------------------

    async def _with_backoff(self, name: str, func, *, attempts: int | None = None):
        attempts = attempts or self.max_attempts
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                result = await func()
            except Exception as exc:  # pragma: no cover - network failure
                last_error = exc
                self._record_error(name, exc.__class__.__name__)
                if attempt == attempts:
                    raise
                delay = min(self.backoff_cap, self.backoff_base * (2 ** (attempt - 1)))
                jitter = random.uniform(0, delay / 3)
                await asyncio.sleep(delay + jitter)
            else:
                return result
        if last_error:
            raise last_error
        raise RuntimeError("backoff finished without result")

    # ------------------------------------------------------------------
    # Probes
    # ------------------------------------------------------------------

    async def _probe_http(self, name: str, url: str) -> ConnectivityResult:
        async def attempt(force_ipv4: bool = False) -> ConnectivityResult:
            start = time.perf_counter()
            session = None
            close_session = False
            try:
                if force_ipv4:
                    connector = aiohttp.TCPConnector(family=socket.AF_INET)
                    session = aiohttp.ClientSession(connector=connector)
                    close_session = True
                else:
                    session = await get_session()
                assert session is not None
                try:
                    async with session.head(url, timeout=self.http_timeout) as resp:
                        latency = (time.perf_counter() - start) * 1000
                        self._record_latency(name, latency)
                        self._record_status(name, resp.status)
                        ok = resp.status < 400
                        if not ok and resp.status in {405, 501}:
                            # Some providers reject HEAD; fall back to GET
                            async with session.get(url, timeout=self.http_timeout) as get_resp:
                                latency = (time.perf_counter() - start) * 1000
                                self._record_latency(name, latency)
                                self._record_status(name, get_resp.status)
                                ok = get_resp.status < 400
                                if not ok:
                                    self._record_error(name, f"HTTP_{get_resp.status}")
                                    return ConnectivityResult(
                                        name=name,
                                        target=url,
                                        ok=False,
                                        latency_ms=latency,
                                        status="error",
                                        status_code=get_resp.status,
                                        error=f"HTTP {get_resp.status}",
                                        attempts=1,
                                    )
                                return ConnectivityResult(
                                    name=name,
                                    target=url,
                                    ok=True,
                                    latency_ms=latency,
                                    status="ok",
                                    status_code=get_resp.status,
                                    attempts=1,
                                )
                        if ok:
                            return ConnectivityResult(
                                name=name,
                                target=url,
                                ok=True,
                                latency_ms=latency,
                                status="ok",
                                status_code=resp.status,
                                attempts=1,
                            )
                        self._record_error(name, f"HTTP_{resp.status}")
                        return ConnectivityResult(
                            name=name,
                            target=url,
                            ok=False,
                            latency_ms=latency,
                            status="error",
                            status_code=resp.status,
                            error=f"HTTP {resp.status}",
                            attempts=1,
                        )
                except aiohttp.ClientConnectorError as exc:
                    if not force_ipv4 and isinstance(exc.os_error, socket.gaierror):
                        logger.warning("DNS resolution failed for %s; retrying with IPv4", url)
                        return await attempt(True)
                    raise
            finally:
                if close_session and session is not None:
                    await session.close()

        try:
            result = await self._with_backoff(name, lambda: attempt(False))
        except Exception as exc:
            self._register_failure(name)
            return ConnectivityResult(name=name, target=url, ok=False, error=str(exc), attempts=self.max_attempts)
        else:
            if result.ok:
                self._register_success(name)
            else:
                self._register_failure(name)
            return result

    async def _probe_ws(self, name: str, url: str) -> ConnectivityResult:
        async def attempt() -> ConnectivityResult:
            started = time.perf_counter()
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=self.http_timeout,
                    close_timeout=self.http_timeout,
                ) as ws:
                    await ws.ping()
                    await asyncio.sleep(0.1)
                    latency = (time.perf_counter() - started) * 1000
                    self._record_latency(name, latency)
                    return ConnectivityResult(
                        name=name,
                        target=url,
                        ok=True,
                        latency_ms=latency,
                        status="ok",
                        attempts=1,
                    )
            except Exception as exc:
                self._record_error(name, exc.__class__.__name__)
                raise

        if not self._breaker_allows(name):
            return ConnectivityResult(name=name, target=url, ok=False, error="circuit-open")
        try:
            result = await self._with_backoff(name, attempt)
        except Exception as exc:
            self._register_failure(name)
            return ConnectivityResult(name=name, target=url, ok=False, error=str(exc))
        else:
            if result.ok:
                self._register_success(name)
            else:
                self._register_failure(name)
            return result

    async def _probe_redis(self, name: str, url: str) -> ConnectivityResult:
        parsed = urlparse(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 6379
        async def attempt() -> ConnectivityResult:
            start = time.perf_counter()
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port),
                timeout=self.redis_timeout,
            )
            try:
                writer.write(b"PING\r\n")
                await writer.drain()
                data = await asyncio.wait_for(reader.readline(), timeout=self.redis_timeout)
                latency = (time.perf_counter() - start) * 1000
                self._record_latency(name, latency)
                ok = data.startswith(b"+PONG")
                if ok:
                    return ConnectivityResult(
                        name=name,
                        target=url,
                        ok=True,
                        latency_ms=latency,
                        status="ok",
                        attempts=1,
                    )
                self._record_error(name, "redis-response")
                return ConnectivityResult(
                    name=name,
                    target=url,
                    ok=False,
                    latency_ms=latency,
                    status="error",
                    error=f"Unexpected response: {data!r}",
                    attempts=1,
                )
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()

        if not self._breaker_allows(name):
            return ConnectivityResult(name=name, target=url, ok=False, error="circuit-open")
        try:
            result = await self._with_backoff(name, attempt)
        except Exception as exc:
            self._register_failure(name)
            return ConnectivityResult(name=name, target=url, ok=False, error=str(exc))
        else:
            if result.ok:
                self._register_success(name)
            else:
                self._register_failure(name)
            return result

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_all(self) -> list[ConnectivityResult]:
        results: list[ConnectivityResult] = []
        for target in self.targets:
            name = target["name"]
            if not self._breaker_allows(name):
                results.append(
                    ConnectivityResult(name=name, target=target["url"], ok=False, error="circuit-open")
                )
                continue
            if target["type"] == "http":
                results.append(await self._probe_http(name, target["url"]))
            elif target["type"] == "ws":
                results.append(await self._probe_ws(name, target["url"]))
            elif target["type"] == "redis":
                results.append(await self._probe_redis(name, target["url"]))
        return results

    async def run_soak(
        self,
        *,
        duration: float = 180.0,
        interval: float = 2.0,
        output_path: Path | None = None,
    ) -> ConnectivitySoakSummary:
        start = time.monotonic()
        reconnects = 0
        end_time = start + duration
        while time.monotonic() < end_time:
            for target in self.targets:
                name = target["name"]
                result: ConnectivityResult
                if target["type"] == "http":
                    result = await self._probe_http(name, target["url"])
                elif target["type"] == "ws":
                    before_errors = sum(self._metrics_for(name)["errors"].values())
                    result = await self._probe_ws(name, target["url"])
                    after_errors = sum(self._metrics_for(name)["errors"].values())
                    if after_errors > before_errors and not result.ok:
                        reconnects += 1
                elif target["type"] == "redis":
                    result = await self._probe_redis(name, target["url"])
                else:  # pragma: no cover - defensive
                    continue
                if not result.ok:
                    logger.warning("Soak probe failed for %s: %s", name, result.error)
            await asyncio.sleep(interval)
        finished = time.monotonic()
        summary = self._render_summary(start, finished, reconnects)
        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(
                    {
                        "started_at": summary.started_at,
                        "finished_at": summary.finished_at,
                        "duration": summary.duration,
                        "reconnect_count": summary.reconnect_count,
                        "metrics": summary.metrics,
                    },
                    indent=2,
                )
            )
            logger.info("Connectivity soak report saved to %s", output_path)
        return summary

    def _render_summary(
        self,
        started: float,
        finished: float,
        reconnects: int,
    ) -> ConnectivitySoakSummary:
        payload: Dict[str, Dict[str, Any]] = {}
        for name, metrics in self._metrics.items():
            latencies = metrics.get("latencies", [])
            payload[name] = {
                "latency_ms": {
                    "p50": statistics.median(latencies) if latencies else None,
                    "p95": _percentile(latencies, 95),
                    "p99": _percentile(latencies, 99),
                    "samples": len(latencies),
                },
                "errors": dict(metrics.get("errors", {})),
                "status_codes": dict(metrics.get("statuses", {})),
            }
        return ConnectivitySoakSummary(
            started_at=started,
            finished_at=finished,
            duration=finished - started,
            metrics=payload,
            reconnect_count=reconnects,
        )
