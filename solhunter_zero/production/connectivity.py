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
from urllib.parse import urlparse, urlsplit

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
        self._refresh_environment(override=False)
        self.http_timeout = http_timeout or float(self.env.get("CONNECTIVITY_HTTP_TIMEOUT", "6"))
        self.redis_timeout = redis_timeout or float(self.env.get("CONNECTIVITY_REDIS_TIMEOUT", "3"))
        self.backoff_base = float(self.env.get("CONNECTIVITY_BACKOFF_BASE", "0.5"))
        self.backoff_cap = float(self.env.get("CONNECTIVITY_BACKOFF_CAP", "8"))
        self.max_attempts = int(self.env.get("CONNECTIVITY_MAX_ATTEMPTS", "4"))
        self.breaker_threshold = breaker_threshold
        self.breaker_cooldown = breaker_cooldown
        self.skip_ui_probes = self._env_flag("CONNECTIVITY_SKIP_UI_PROBES")
        self._breaker_state: MutableMapping[str, float] = {}
        self._failure_counts: MutableMapping[str, int] = defaultdict(int)
        self._metrics: MutableMapping[str, Dict[str, Any]] = defaultdict(
            lambda: {"latencies": [], "errors": Counter(), "statuses": Counter()}
        )
        self.targets = self._build_targets()

    def _refresh_environment(self, *, override: bool) -> None:
        for key, value in os.environ.items():
            if not (key.startswith("UI_") or key.startswith("CONNECTIVITY_")):
                continue
            if override or key not in self.env:
                self.env[key] = value

    def _refresh_targets_from_env(self) -> None:
        self._refresh_environment(override=True)
        self.skip_ui_probes = self._env_flag("CONNECTIVITY_SKIP_UI_PROBES")
        self.targets = self._build_targets()

    def _env_flag(self, key: str) -> bool:
        raw = self.env.get(key)
        if raw is None:
            return False
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in {"1", "true", "yes", "on"}

    # ------------------------------------------------------------------
    # Runtime websocket discovery helpers
    # ------------------------------------------------------------------

    def _runtime_log_paths(self) -> list[Path]:
        candidates: list[Path] = []
        override = self.env.get("CONNECTIVITY_RUNTIME_LOG") or self.env.get("RUNTIME_LOG_PATH")
        if override:
            candidates.append(Path(override))
        else:
            log_dir = Path(self.env.get("RUNTIME_LOG_DIR", "artifacts/logs"))
            log_name = self.env.get("RUNTIME_LOG_NAME", "runtime.log")
            candidates.append(log_dir / log_name)
        return candidates

    @staticmethod
    def _parse_ws_ready_line(line: str) -> dict[str, str]:
        if "UI_WS_READY" not in line:
            return {}
        fragments = line.strip().split()
        urls: dict[str, str] = {}
        for fragment in fragments:
            if "=" not in fragment:
                continue
            key, value = fragment.split("=", 1)
            key = key.strip()
            value = value.strip().rstrip(",")
            if key in {"rl_ws", "events_ws", "logs_ws"}:
                if value and value not in {"-", "unavailable"}:
                    urls[key[:-3]] = value
        return urls

    def _load_ws_urls_from_runtime_log(self) -> dict[str, str]:
        for path in self._runtime_log_paths():
            try:
                with path.open("r", encoding="utf-8") as handle:
                    lines = handle.readlines()
            except OSError:
                continue
            for raw_line in reversed(lines):
                parsed = self._parse_ws_ready_line(raw_line)
                if parsed:
                    return parsed
        return {}

    def _manifest_paths(self) -> list[Path]:
        candidates: list[Path] = []
        override = self.env.get("CONNECTIVITY_UI_MANIFEST") or self.env.get("UI_MANIFEST_PATH")
        if override:
            candidates.append(Path(override))
        candidates.append(Path("artifacts/prelaunch/ui_manifest.json"))
        return candidates

    def _load_ws_urls_from_manifest(self) -> dict[str, str]:
        for path in self._manifest_paths():
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            urls: dict[str, str] = {}
            for channel in ("events", "rl", "logs"):
                key = f"{channel}_ws"
                value = payload.get(key)
                if isinstance(value, str) and value:
                    urls[channel] = value
            if urls:
                return urls
        return {}

    def _resolve_ui_ws_urls(self) -> dict[str, str]:
        urls = self._load_ws_urls_from_runtime_log()
        if not urls:
            urls = self._load_ws_urls_from_manifest()
        env_map = {
            "events": ("UI_EVENTS_WS", "UI_EVENTS_WS_URL", "UI_WS_URL"),
            "rl": ("UI_RL_WS", "UI_RL_WS_URL"),
            "logs": ("UI_LOGS_WS", "UI_LOG_WS_URL"),
        }
        for channel, keys in env_map.items():
            if urls.get(channel):
                continue
            for key in keys:
                value = self.env.get(key)
                if value:
                    urls[channel] = value
                    break
        for channel, keys in env_map.items():
            value = urls.get(channel)
            if not value:
                continue
            for key in keys:
                self.env.setdefault(key, value)
        return {channel: url for channel, url in urls.items() if isinstance(url, str) and url}

    # ------------------------------------------------------------------
    # Target configuration
    # ------------------------------------------------------------------

    def _build_targets(self) -> list[dict[str, Any]]:
        env = self.env
        rpc = env.get("SOLANA_RPC_URL") or env.get("HELIUS_RPC_URL")
        ws = env.get("SOLANA_WS_URL") or env.get("HELIUS_WS_URL")
        das_base = env.get("DAS_BASE_URL") or "https://api.helius.xyz/v1"
        das = das_base.rstrip("/")
        rest = env.get("HELIUS_PRICE_REST_URL")
        if not rest:
            price_base = env.get("HELIUS_PRICE_BASE_URL") or "https://api.helius.xyz"
            rest = f"{price_base.rstrip('/')}/v0/token-metadata"
        redis_url = env.get("REDIS_URL") or "redis://127.0.0.1:6379/1"
        ui_ws_map = self._resolve_ui_ws_urls()
        ui_ws: str | Mapping[str, str] | None = None
        if ui_ws_map:
            ui_ws = ui_ws_map
        if not self.skip_ui_probes:
            if ui_ws is None:
                events_env = (
                    env.get("UI_EVENTS_WS")
                    or env.get("UI_EVENTS_WS_URL")
                    or env.get("UI_WS_URL")
                )
                if events_env:
                    ui_ws = {"events": events_env}
            if ui_ws is None:
                host = env.get("UI_HOST", "127.0.0.1")
                if host in {"0.0.0.0", "::"}:
                    host = "127.0.0.1"
                port = env.get("UI_PORT", "5001")
                scheme = (env.get("UI_WS_SCHEME") or env.get("WS_SCHEME") or "ws").strip().lower()
                if scheme not in {"ws", "wss"}:
                    scheme = "ws"
                path_template = env.get("UI_WS_PATH_TEMPLATE") or "/ws/{channel}"
                try:
                    path = path_template.format(channel="events")
                except Exception:
                    path = "/ws/events"
                if not path.startswith("/"):
                    path = "/" + path.lstrip("/")
                ui_ws = {"events": f"{scheme}://{host}:{port}{path}"}
        ws_gateway = env.get("GATEWAY_WS_URL") or env.get("WS_GATEWAY_URL")
        targets: list[dict[str, Any]] = []
        if rpc:
            targets.append({"name": "solana-rpc", "type": "http", "url": rpc})
        if ws:
            targets.append({"name": "solana-ws", "type": "ws", "url": ws})
        if das:
            search_candidates = []
            configured = env.get("DAS_SEARCH_PATH")
            if configured:
                search_candidates.append(configured)
            search_candidates.extend(["searchAssets", "asset/search"])
            das_path = None
            for candidate in search_candidates:
                norm = (candidate or "").strip().lstrip("/")
                if norm:
                    das_path = norm
                    break
            if das_path:
                targets.append({"name": "helius-das", "type": "http", "url": f"{das}/{das_path}"})
        if rest:
            targets.append({"name": "helius-rest", "type": "http", "url": rest})
        if redis_url:
            targets.append({"name": "redis", "type": "redis", "url": redis_url})
        ui_http = None
        if not self.skip_ui_probes:
            ui_http = env.get("UI_HEALTH_URL")
            if not ui_http:
                path = env.get("UI_HEALTH_PATH") or "/health"
                if not path.startswith("/"):
                    path = "/" + path
                base_http = (env.get("UI_HTTP_URL") or "").strip()
                parsed_base = None
                if base_http:
                    try:
                        parsed_base = urlsplit(base_http)
                    except ValueError:
                        parsed_base = None
                scheme = (
                    env.get("UI_HTTP_SCHEME")
                    or env.get("UI_SCHEME")
                    or (parsed_base.scheme if parsed_base is not None else "http")
                    or "http"
                )
                scheme = (scheme or "http").strip().lower()
                if scheme not in {"http", "https"}:
                    scheme = "http"
                host = env.get("UI_HOST")
                if not host and parsed_base is not None:
                    host = parsed_base.hostname
                if not host:
                    host = "127.0.0.1"
                host = host or "127.0.0.1"
                if host in {"0.0.0.0", "::"}:
                    host = "127.0.0.1"
                port = None
                if parsed_base is not None and parsed_base.port:
                    port = str(parsed_base.port)
                if not port:
                    port = env.get("UI_PORT") or env.get("PORT")
                if not port:
                    port = "5001"
                if ":" in host and not host.startswith("["):
                    host_component = f"[{host}]"
                else:
                    host_component = host
                base_url = f"{scheme}://{host_component}"
                if port:
                    base_url = f"{base_url}:{port}"
                ui_http = f"{base_url}{path}"
        if self.skip_ui_probes:
            logger.info("UI connectivity targets disabled via CONNECTIVITY_SKIP_UI_PROBES")
        if ui_ws:
            targets.append({"name": "ui-ws", "type": "ws", "url": ui_ws})
        if ui_http:
            targets.append({"name": "ui-http", "type": "http", "url": ui_http})
        if ws_gateway:
            targets.append({"name": "ws-gateway", "type": "ws", "url": ws_gateway})
        return targets

    # ------------------------------------------------------------------
    # Metrics helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalise_ui_ws_candidates(url: Any) -> list[tuple[str | None, str]]:
        candidates: list[tuple[str | None, str]] = []
        if isinstance(url, Mapping):
            for key, value in url.items():
                if isinstance(value, str) and value:
                    candidates.append((str(key), value))
        elif isinstance(url, (list, tuple, set)):
            for value in url:
                if isinstance(value, str) and value:
                    candidates.append((None, value))
        elif isinstance(url, str):
            if url:
                candidates.append((None, url))
        return candidates

    @staticmethod
    def _format_ui_ws_target(candidates: Iterable[tuple[str | None, str]]) -> str:
        parts = []
        for channel, endpoint in candidates:
            label = channel or "unknown"
            parts.append(f"{label}={endpoint}")
        return ", ".join(parts)

    async def _handshake_ui_ws(self, url: str, *, expected_channel: str | None) -> float:
        started = time.perf_counter()
        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=self.http_timeout,
            close_timeout=self.http_timeout,
        ) as ws:
            await ws.ping()
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.redis_timeout)
            except asyncio.TimeoutError as exc:
                raise RuntimeError("ui-hello-timeout") from exc
            except Exception as exc:  # pragma: no cover - defensive
                raise RuntimeError(f"ui-hello-error: {exc}") from exc
            try:
                payload = json.loads(raw)
            except Exception as exc:
                raise RuntimeError("ui-hello-invalid-json") from exc
            if payload.get("event") != "hello":
                raise RuntimeError("ui-hello-missing")
            channel = payload.get("channel")
            if expected_channel:
                if channel != expected_channel:
                    raise RuntimeError(f"ui-hello-channel-mismatch:{channel}")
            else:
                if channel not in {"events", "rl", "logs"}:
                    raise RuntimeError("ui-hello-unknown-channel")
            await asyncio.sleep(0.05)
        latency = (time.perf_counter() - started) * 1000
        return latency

    async def _probe_ui_ws(self, name: str, url: Any) -> ConnectivityResult:
        candidates = self._normalise_ui_ws_candidates(url)
        if not candidates:
            self._register_failure(name)
            return ConnectivityResult(
                name=name,
                target="",
                ok=False,
                status="error",
                error="no websocket urls",
                attempts=0,
            )
        successes: list[tuple[str | None, float]] = []
        errors: dict[str, str] = {}
        for channel, endpoint in candidates:
            label = channel or endpoint
            try:
                latency = await self._handshake_ui_ws(endpoint, expected_channel=channel)
            except Exception as exc:
                errors[label] = str(exc)
                self._record_error(name, f"{label}:{exc.__class__.__name__}")
            else:
                self._record_latency(name, latency)
                successes.append((channel, latency))
        attempts = len(successes) + len(errors)
        target_desc = self._format_ui_ws_target(candidates)
        if successes:
            latency_ms = min(lat for _, lat in successes)
            status = "ok" if not errors else "degraded"
            error_msg = None
            if errors:
                error_msg = "; ".join(f"{label}: {msg}" for label, msg in errors.items())
            self._register_success(name)
            return ConnectivityResult(
                name=name,
                target=target_desc,
                ok=True,
                latency_ms=latency_ms,
                status=status,
                error=error_msg,
                attempts=attempts or 1,
            )
        self._register_failure(name)
        error_msg = "; ".join(f"{label}: {msg}" for label, msg in errors.items()) or "no websocket channels succeeded"
        return ConnectivityResult(
            name=name,
            target=target_desc,
            ok=False,
            latency_ms=None,
            status="error",
            error=error_msg,
            attempts=attempts or 1,
        )

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
                if name == "ui-http":
                    async with aiohttp.ClientSession() as session_local:
                        session = session_local
                        close_session = False
                        async with session.get(url, timeout=self.http_timeout) as resp:
                            latency = (time.perf_counter() - start) * 1000
                            self._record_latency(name, latency)
                            self._record_status(name, resp.status)
                            text: str | None = None
                            try:
                                text = await resp.text()
                            except Exception:
                                text = None
                            if resp.status >= 400:
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
                            data: Dict[str, Any] | None = None
                            if text:
                                try:
                                    data = json.loads(text)
                                except Exception:
                                    data = None
                            bus_ok = bool(data and data.get("status", {}).get("event_bus"))
                            loop_ok = bool(data and data.get("status", {}).get("trading_loop"))
                            explicit_ok = None
                            if data is not None and "ok" in data:
                                explicit_ok = bool(data.get("ok"))
                            ok = resp.status < 400 and bus_ok and loop_ok
                            if explicit_ok is not None:
                                ok = ok and explicit_ok
                            if not ok:
                                self._record_error(name, "ui-health")
                                if not bus_ok and loop_ok:
                                    detail = "event bus offline"
                                elif loop_ok is False and bus_ok:
                                    detail = "trading loop inactive"
                                elif loop_ok is False and not bus_ok:
                                    detail = "event bus offline; trading loop inactive"
                                else:
                                    detail = "missing status payload"
                                return ConnectivityResult(
                                    name=name,
                                    target=url,
                                    ok=False,
                                    latency_ms=latency,
                                    status="error",
                                    status_code=resp.status,
                                    error=f"ui-health: {detail}",
                                    attempts=1,
                                )
                            return ConnectivityResult(
                                name=name,
                                target=url,
                                ok=True,
                                latency_ms=latency,
                                status="ok",
                                status_code=resp.status,
                                attempts=1,
                            )
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

    async def _probe_ws(self, name: str, url: Any) -> ConnectivityResult:
        if name == "ui-ws":
            return await self._probe_ui_ws(name, url)

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
            self._refresh_targets_from_env()
            for target in list(self.targets):
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
