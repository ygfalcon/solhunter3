"""Momentum and sentiment enrichment for Golden snapshots."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

import aiohttp

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore
except Exception:  # pragma: no cover - metrics optional
    Counter = None  # type: ignore
    Gauge = None  # type: ignore
    Histogram = None  # type: ignore

from ..http import HostCircuitOpenError, get_session, host_request
from ..token_aliases import canonical_mint
from .types import GoldenSnapshot
from .utils import CircuitBreaker, clamp, now_ts

logger = logging.getLogger(__name__)


_DEFAULT_INTERVAL_SEC = 60.0
_ACTIVE_WINDOW_SEC = 120.0
_PER_MINT_BUDGET_SEC = 1.0
_CONNECT_TIMEOUT_SEC = float(os.getenv("GOLDEN_MOMENTUM_CONNECT_TIMEOUT", "0.3") or 0.3)
_READ_TIMEOUT_SEC = float(os.getenv("GOLDEN_MOMENTUM_READ_TIMEOUT", "0.7") or 0.7)
_TOTAL_TIMEOUT_SEC = max(_CONNECT_TIMEOUT_SEC + _READ_TIMEOUT_SEC, 1.0)
_RETRY_BACKOFF_SEC = float(os.getenv("GOLDEN_MOMENTUM_RETRY_BACKOFF", "0.15") or 0.15)

_HOST_RPS: Mapping[str, tuple[float, int]] = {
    "public-api.birdeye.so": (5.0, 5),
    "api.dexscreener.com": (5.0, 5),
    "pump.fun": (3.0, 3),
    "nitter.net": (2.0, 2),
    "www.nitter.net": (2.0, 2),
    "nitter.pufe.org": (2.0, 2),
    "www.dextools.io": (2.0, 2),
}

_BIRDEYE_TOKEN_LIST = "https://public-api.birdeye.so/defi/v3/token/list"
_BIRDEYE_TRENDING = "https://public-api.birdeye.so/defi/token_trending"
_BIRDEYE_META_SINGLE = "https://public-api.birdeye.so/defi/v3/token/meta-data/single"
_DEXSCREENER_TRENDING = "https://api.dexscreener.com/latest/dex/trending"
_DEXSCREENER_TOKENS = "https://api.dexscreener.com/tokens/v1/solana/"
_PUMPFUN_TRENDING = "https://pump.fun/api/trending"
_NITTER_SEARCH = "https://nitter.net/search"
_DEXTOOLS_POOL_META = "https://www.dextools.io/shared/data/pool/"

_DEFAULT_SOCIAL_MIN = 0.0
_DEFAULT_SOCIAL_MAX = 2.0

_CACHE_TTL_SEC = 90.0
_SOCIAL_CACHE_TTL_SEC = 300.0

_WEIGHTS_DEFAULT = {
    "volume_rank_1h": 0.40,
    "volume_rank_24h": 0.20,
    "price_momentum_5m": 0.20,
    "price_momentum_1h": 0.10,
    "pump_intensity": 0.05,
    "social_sentiment": 0.05,
}


class MomentumError(Exception):
    """Base error raised by the momentum enrichment agent."""


class MomentumRateLimitError(MomentumError):
    """Raised when an upstream provider signals backpressure."""


class MomentumParameterError(MomentumError):
    """Raised when an upstream provider rejects parameters (4xx)."""


class MomentumTimeoutError(MomentumError):
    """Raised when a fetch exceeds the per-request timeout."""


class MomentumParseError(MomentumError):
    """Raised when an upstream response cannot be parsed."""


class TokenBucket:
    """Simple token bucket used to guard host throughput."""

    __slots__ = ("host", "rate", "capacity", "tokens", "updated", "_lock")

    def __init__(self, host: str, *, rate: float, burst: int) -> None:
        self.host = host
        self.rate = max(0.0, float(rate))
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        if self.rate <= 0:
            return
        now = time.monotonic()
        elapsed = max(0.0, now - self.updated)
        if not elapsed:
            return
        self.updated = now
        refill = elapsed * self.rate
        if refill > 0:
            self.tokens = min(self.capacity, self.tokens + refill)

    async def acquire(self, *, timeout: float | None = None) -> bool:
        start = time.monotonic()
        while True:
            async with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    _set_bucket_level(self.host, self.tokens)
                    return True
                wait_for = 0.05 if self.rate <= 0 else min(0.5, max(0.05, 1.0 / max(self.rate, 1e-6)))
            if timeout is not None and time.monotonic() - start > timeout:
                return False
            await asyncio.sleep(wait_for)

    def set_tokens(self, value: float) -> None:
        self.tokens = max(0.0, min(float(value), float(self.capacity)))
        _set_bucket_level(self.host, self.tokens)


if Gauge is not None:  # pragma: no branch - optional metrics
    _BUCKET_LEVEL: Gauge | None = Gauge(
        "bucket_level",
        "Current token bucket level by upstream host",
        labelnames=("host",),
    )
else:  # pragma: no cover - metrics optional
    _BUCKET_LEVEL = None


def _set_bucket_level(host: str, level: float) -> None:
    if _BUCKET_LEVEL is not None:
        try:
            _BUCKET_LEVEL.labels(host=host).set(level)
        except Exception:  # pragma: no cover - metrics optional
            pass


if Counter is not None:  # pragma: no branch - optional metrics
    MOMENTUM_EMIT_TOTAL = Counter(
        "momentum_emit_total",
        "Momentum enrichment emissions by source",
        labelnames=("host",),
    )
    MOMENTUM_ERROR_TOTAL = Counter(
        "momentum_error_total",
        "Momentum enrichment errors",
        labelnames=("host", "reason"),
    )
else:  # pragma: no cover - metrics optional
    MOMENTUM_EMIT_TOTAL = None
    MOMENTUM_ERROR_TOTAL = None

if Histogram is not None:  # pragma: no branch - optional metrics
    MOMENTUM_LATENCY_MS = Histogram(
        "momentum_latency_ms",
        "Momentum enrichment latency per mint",
        buckets=(25, 50, 100, 200, 300, 400, 600, 800, 1000, 1500, 2500),
    )
else:  # pragma: no cover - metrics optional
    MOMENTUM_LATENCY_MS = None


@dataclass(slots=True)
class MomentumWeights:
    volume_rank_1h: float = _WEIGHTS_DEFAULT["volume_rank_1h"]
    volume_rank_24h: float = _WEIGHTS_DEFAULT["volume_rank_24h"]
    price_momentum_5m: float = _WEIGHTS_DEFAULT["price_momentum_5m"]
    price_momentum_1h: float = _WEIGHTS_DEFAULT["price_momentum_1h"]
    pump_intensity: float = _WEIGHTS_DEFAULT["pump_intensity"]
    social_sentiment: float = _WEIGHTS_DEFAULT["social_sentiment"]


@dataclass(slots=True)
class MomentumConfig:
    """Resolved configuration for the momentum agent."""

    enabled: bool = False
    top_n_active: int = 250
    weights: MomentumWeights = field(default_factory=MomentumWeights)
    social_min: float = _DEFAULT_SOCIAL_MIN
    social_max: float = _DEFAULT_SOCIAL_MAX


@dataclass(slots=True)
class MomentumComputation:
    """Normalized momentum data ready to publish."""

    mint: str
    momentum_score: float | None
    pump_intensity: float | None
    pump_score: float | None
    social_score: float | None
    social_sentiment: float | None
    tweets_per_min: float | None
    buyers_last_hour: int | None
    momentum_partial: bool
    momentum_stale: bool
    momentum_sources: tuple[str, ...]
    momentum_breakdown: Dict[str, Any]
    latency_ms: float


def _resolve_weights_path() -> Path:
    configured = os.getenv("GOLDEN_MOMENTUM_CONFIG")
    if configured:
        candidate = Path(configured).expanduser()
        if candidate.exists():
            return candidate
    discovery_override = os.getenv("DISCOVERY_SCORE_WEIGHTS")
    if discovery_override:
        candidate = Path(discovery_override).expanduser()
        if candidate.exists():
            return candidate
    return Path(__file__).resolve().parents[1] / "configs" / "discovery_score_weights.yaml"


def _read_yaml(path: Path) -> Mapping[str, Any]:
    if yaml is None:
        return {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}
        if isinstance(payload, Mapping):
            return payload
    except FileNotFoundError:
        return {}
    except Exception:  # pragma: no cover - defensive
        logger.debug("Failed reading %s", path, exc_info=True)
    return {}


def load_momentum_config(config: Mapping[str, Any] | None = None) -> MomentumConfig:
    """Load momentum configuration from YAML and runtime config mapping."""

    payload = _read_yaml(_resolve_weights_path())
    weights_cfg = payload.get("momentum", {}) if isinstance(payload, Mapping) else {}
    weights_data = weights_cfg.get("weights") if isinstance(weights_cfg, Mapping) else {}
    social_cfg = weights_cfg.get("social") if isinstance(weights_cfg, Mapping) else {}
    min_max = ()
    if isinstance(social_cfg, Mapping):
        min_max = social_cfg.get("min_max") or social_cfg.get("range")
    if isinstance(min_max, (list, tuple)) and len(min_max) >= 2:
        try:
            social_min = float(min_max[0])
            social_max = float(min_max[1])
        except Exception:
            social_min = _DEFAULT_SOCIAL_MIN
            social_max = _DEFAULT_SOCIAL_MAX
    else:
        social_min = _DEFAULT_SOCIAL_MIN
        social_max = _DEFAULT_SOCIAL_MAX

    top_n = weights_cfg.get("top_n_active") if isinstance(weights_cfg, Mapping) else None
    try:
        top_n_active = max(1, int(top_n)) if top_n is not None else 250
    except Exception:
        top_n_active = 250

    resolved_weights = MomentumWeights()
    for key, value in (weights_data or {}).items():
        if not hasattr(resolved_weights, key):
            continue
        try:
            setattr(resolved_weights, key, float(value))
        except Exception:
            continue

    env_flag = os.getenv("GOLDEN_MOMENTUM_ENABLED")
    enabled = False
    if env_flag is not None:
        enabled = str(env_flag).strip().lower() in {"1", "true", "yes", "on", "enabled"}
    else:
        if isinstance(config, Mapping):
            golden_cfg = config.get("golden")
            if isinstance(golden_cfg, Mapping):
                momentum_cfg = golden_cfg.get("momentum")
                if isinstance(momentum_cfg, Mapping):
                    flag = momentum_cfg.get("enabled")
                    if isinstance(flag, str):
                        enabled = flag.strip().lower() in {"1", "true", "yes", "on", "enabled"}
                    elif isinstance(flag, (bool, int)):
                        enabled = bool(flag)
                if not enabled:
                    direct = golden_cfg.get("momentum_enabled")
                    if isinstance(direct, str):
                        enabled = direct.strip().lower() in {"1", "true", "yes", "on", "enabled"}
                    elif isinstance(direct, (bool, int)):
                        enabled = bool(direct)
        if not enabled and isinstance(payload, Mapping):
            golden_section = payload.get("golden")
            if isinstance(golden_section, Mapping):
                section = golden_section.get("momentum")
                if isinstance(section, Mapping):
                    flag = section.get("enabled")
                    if isinstance(flag, str):
                        enabled = flag.strip().lower() in {"1", "true", "yes", "on", "enabled"}
                    elif isinstance(flag, (bool, int)):
                        enabled = bool(flag)

    return MomentumConfig(
        enabled=enabled,
        top_n_active=top_n_active,
        weights=resolved_weights,
        social_min=social_min,
        social_max=social_max if social_max > social_min else social_min + 2.0,
    )


def _logistic(delta_pct: float, *, alpha: float = 0.3) -> float:
    try:
        scaled = float(delta_pct)
    except Exception:
        return 0.0
    return clamp(1.0 / (1.0 + math.exp(-alpha * scaled)), 0.0, 1.0)


def _normalize_rank(values: Mapping[str, float | int | None]) -> Dict[str, float]:
    items: list[tuple[str, float]] = []
    for mint, raw in values.items():
        try:
            numeric = float(raw) if raw is not None else math.nan
        except Exception:
            numeric = math.nan
        if math.isnan(numeric):
            continue
        items.append((mint, numeric))
    if not items:
        return {}
    items.sort(key=lambda pair: pair[1], reverse=True)
    if len(items) == 1:
        return {items[0][0]: 1.0}
    tail = len(items) - 1
    ranked: Dict[str, float] = {}
    for index, (mint, _value) in enumerate(items):
        ranked[mint] = clamp(1.0 - (index / tail), 0.0, 1.0)
    return ranked


def _normalize_min_max(value: float | None, *, minimum: float, maximum: float) -> float:
    if value is None:
        return 0.0
    if maximum <= minimum:
        return 0.0
    return clamp((value - minimum) / (maximum - minimum), 0.0, 1.0)


def _pump_intensity_from_rank(rank: Optional[int]) -> float:
    if rank is None or rank <= 0:
        return 0.0
    try:
        return clamp(1.0 / math.log(rank + 1.0), 0.0, 1.0)
    except Exception:
        return 0.0


class MomentumAgent:
    """Asynchronous momentum enrichment pipeline."""

    def __init__(
        self,
        *,
        pipeline: Any,
        publish: Callable[[str, MomentumComputation], Awaitable[None]],
        config: Mapping[str, Any] | None = None,
        interval: float = _DEFAULT_INTERVAL_SEC,
    ) -> None:
        self._pipeline = pipeline
        self._publish = publish
        self._config = load_momentum_config(config)
        self._interval = max(5.0, float(interval))
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._cycle_lock = asyncio.Lock()
        self._latest_snapshots: Dict[str, GoldenSnapshot] = {}
        self._last_seen: Dict[str, float] = {}
        self._candidate_seen: Dict[str, float] = {}
        self._cache: Dict[str, tuple[float, Any]] = {}
        self._etag_cache: Dict[str, tuple[str, Any, float]] = {}
        self._limiters: Dict[str, TokenBucket] = {}
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._session_timeout = aiohttp.ClientTimeout(
            total=_TOTAL_TIMEOUT_SEC,
            sock_connect=_CONNECT_TIMEOUT_SEC,
            sock_read=_READ_TIMEOUT_SEC,
        )
        for host, (rate, burst) in _HOST_RPS.items():
            self._limiters[host] = TokenBucket(host, rate=rate, burst=burst)
            self._breakers[host] = CircuitBreaker(
                threshold=3,
                window_sec=30.0,
                cooldown_sec=30.0,
            )

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def update_config(self, config: Mapping[str, Any] | None) -> None:
        self._config = load_momentum_config(config)

    async def start(self) -> None:
        if not self.enabled or self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="momentum_agent")

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    def record_candidate(self, mint: str, *, ts: float | None = None) -> None:
        if not self.enabled:
            return
        candidate = canonical_mint(mint)
        if not candidate:
            return
        self._candidate_seen[candidate] = ts or now_ts()

    def record_snapshot(self, snapshot: GoldenSnapshot) -> None:
        if not self.enabled:
            return
        self._latest_snapshots[snapshot.mint] = snapshot
        self._last_seen[snapshot.mint] = snapshot.asof or now_ts()

    async def run_cycle(self) -> None:
        if not self.enabled:
            return
        if not self._latest_snapshots:
            return
        async with self._cycle_lock:
            await self._execute_cycle()

    async def _run_loop(self) -> None:
        try:
            while self._running:
                start = time.monotonic()
                try:
                    await self.run_cycle()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("Momentum agent cycle failed")
                elapsed = time.monotonic() - start
                delay = max(5.0, self._interval - elapsed)
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            raise

    async def _execute_cycle(self) -> None:
        now = now_ts()
        active: list[tuple[str, float, GoldenSnapshot]] = []
        for mint, snapshot in list(self._latest_snapshots.items()):
            last = self._last_seen.get(mint, snapshot.asof)
            if last and now - last > _ACTIVE_WINDOW_SEC:
                continue
            depth_value = self._extract_depth(snapshot)
            active.append((mint, depth_value, snapshot))
        if not active:
            return
        active.sort(key=lambda item: item[1], reverse=True)
        top_n = min(self._config.top_n_active, len(active))
        selected = active[:top_n]
        sources_snapshot = await self._collect_sources([mint for mint, _depth, _snap in selected])
        tasks = []
        for mint, _depth, snapshot in selected:
            tasks.append(asyncio.create_task(self._process_mint(mint, snapshot, sources_snapshot)))
        if not tasks:
            return
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _collect_sources(self, mints: Sequence[str]) -> Dict[str, Any]:
        tasks = [
            asyncio.create_task(self._fetch_birdeye_snapshot()),
            asyncio.create_task(self._fetch_dexscreener_trending()),
            asyncio.create_task(self._fetch_pumpfun()),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        volumes: Mapping[str, Mapping[str, Any]] = {}
        trending: Mapping[str, Mapping[str, Any]] = {}
        pump: Mapping[str, Mapping[str, Any]] = {}
        errors: Dict[str, Exception] = {}
        for task, label in zip(results, ("birdeye", "dexscreener", "pumpfun")):
            if isinstance(task, Exception):
                self._record_error(label, task)
                errors[label] = task
                continue
            if label == "birdeye":
                volumes = task or {}
            elif label == "dexscreener":
                trending = task or {}
            else:
                pump = task or {}
        extra_price: Mapping[str, Mapping[str, Any]] = {}
        if mints:
            try:
                extra_price = await self._fetch_dexscreener_tokens(mints)
            except Exception as exc:  # pragma: no cover - network failure guard
                self._record_error("dexscreener", exc)
                errors.setdefault("dexscreener", exc)
                extra_price = {}
        return {
            "birdeye": volumes,
            "dexscreener": trending,
            "dexscreener_tokens": extra_price,
            "pumpfun": pump,
            "_errors": errors,
        }

    async def _process_mint(
        self,
        mint: str,
        snapshot: GoldenSnapshot,
        sources: Mapping[str, Any],
    ) -> None:
        start = time.perf_counter()
        budget = _PER_MINT_BUDGET_SEC
        try:
            result = await asyncio.wait_for(
                self._compute_momentum(mint, snapshot, sources, budget),
                timeout=budget,
            )
        except asyncio.TimeoutError:
            result = MomentumComputation(
                mint=mint,
                momentum_score=None,
                pump_intensity=None,
                pump_score=None,
                social_score=None,
                social_sentiment=None,
                tweets_per_min=None,
                buyers_last_hour=None,
                momentum_partial=True,
                momentum_stale=True,
                momentum_sources=tuple(),
                momentum_breakdown={},
                latency_ms=(time.perf_counter() - start) * 1000.0,
            )
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.debug("Momentum processing failed for %s: %s", mint, exc, exc_info=True)
            result = MomentumComputation(
                mint=mint,
                momentum_score=None,
                pump_intensity=None,
                pump_score=None,
                social_score=None,
                social_sentiment=None,
                tweets_per_min=None,
                buyers_last_hour=None,
                momentum_partial=True,
                momentum_stale=True,
                momentum_sources=tuple(),
                momentum_breakdown={},
                latency_ms=(time.perf_counter() - start) * 1000.0,
            )
        else:
            result = MomentumComputation(
                mint=result.mint,
                momentum_score=_round4(result.momentum_score),
                pump_intensity=_round4(result.pump_intensity),
                pump_score=_round4(result.pump_score),
                social_score=_round4(result.social_score),
                social_sentiment=_round4(result.social_sentiment),
                tweets_per_min=_round4(result.tweets_per_min),
                buyers_last_hour=result.buyers_last_hour,
                momentum_partial=result.momentum_partial,
                momentum_stale=result.momentum_stale,
                momentum_sources=result.momentum_sources,
                momentum_breakdown=result.momentum_breakdown,
                latency_ms=(time.perf_counter() - start) * 1000.0,
            )
        await self._publish(mint, result)
        self._log_update(result)
        if MOMENTUM_LATENCY_MS is not None:
            try:
                MOMENTUM_LATENCY_MS.observe(result.latency_ms)
            except Exception:  # pragma: no cover - metrics optional
                pass

    async def _compute_momentum(
        self,
        mint: str,
        snapshot: GoldenSnapshot,
        sources: Mapping[str, Any],
        budget: float,
    ) -> MomentumComputation:
        breakdown: Dict[str, Any] = {}
        sources_used: set[str] = set()
        momentum_partial = False
        buyers_last_hour: int | None = None
        error_map = sources.get("_errors") if isinstance(sources, Mapping) else {}
        stale_due_to_errors = False
        if isinstance(error_map, Mapping):
            error_hosts: list[str] = []
            for host, exc in error_map.items():
                try:
                    error_hosts.append(str(host))
                except Exception:
                    continue
                if isinstance(
                    exc,
                    (
                        MomentumRateLimitError,
                        MomentumTimeoutError,
                        MomentumParseError,
                    ),
                ):
                    stale_due_to_errors = True
                elif isinstance(exc, MomentumError) and not isinstance(
                    exc, MomentumParameterError
                ):
                    stale_due_to_errors = True
            if error_hosts:
                breakdown["error_hosts"] = sorted(set(error_hosts))
                momentum_partial = True

        volumes = sources.get("birdeye") or {}
        trending = sources.get("dexscreener") or {}
        extra_price = sources.get("dexscreener_tokens") or {}
        pumpfun = sources.get("pumpfun") or {}

        volume_1h_map = {k: (v or {}).get("volume_1h_usd") for k, v in volumes.items()}
        volume_24h_map = {k: (v or {}).get("volume_24h_usd") for k, v in volumes.items()}
        ranks_1h = _normalize_rank(volume_1h_map)
        ranks_24h = _normalize_rank(volume_24h_map)
        volume_rank_1h = ranks_1h.get(mint)
        volume_rank_24h = ranks_24h.get(mint)
        if volume_rank_1h is not None:
            breakdown["volume_rank_1h"] = volume_rank_1h
            sources_used.add("birdeye")
        else:
            breakdown["missing_volume_rank_1h"] = True
            momentum_partial = True
        if volume_rank_24h is not None:
            breakdown["volume_rank_24h"] = volume_rank_24h
            sources_used.add("birdeye")
        else:
            breakdown["missing_volume_rank_24h"] = True
            momentum_partial = True

        price_entry = {}  # type: ignore[var-annotated]
        if mint in trending:
            price_entry = trending[mint]
        elif mint in extra_price:
            price_entry = extra_price[mint]

        def _extract_price(field: str) -> Optional[float]:
            value = price_entry.get(field) if isinstance(price_entry, Mapping) else None
            if value is None and isinstance(price_entry, Mapping):
                price_change = price_entry.get("priceChange")
                if isinstance(price_change, Mapping):
                    value = price_change.get(field)
            try:
                return float(value)
            except Exception:
                return None

        price_5m = _extract_price("m5")
        price_1h = _extract_price("h1") or _extract_price("h60")
        if price_5m is None and isinstance(extra_price.get(mint), Mapping):
            candidate = extra_price[mint]
            if isinstance(candidate.get("priceChange"), Mapping):
                try:
                    price_5m = float(candidate["priceChange"].get("m5"))
                except Exception:
                    price_5m = None
            try:
                price_1h = price_1h or float(candidate.get("priceChange", {}).get("h1"))
            except Exception:
                pass

        price_norm_5m = _logistic((price_5m or 0.0) / 100.0) if price_5m is not None else 0.0
        price_norm_1h = _logistic((price_1h or 0.0) / 100.0) if price_1h is not None else 0.0
        if price_5m is not None:
            sources_used.add("dexscreener")
        else:
            breakdown["missing_price_momentum_5m"] = True
            momentum_partial = True
        if price_1h is not None:
            sources_used.add("dexscreener")
        else:
            breakdown["missing_price_momentum_1h"] = True
            momentum_partial = True
        breakdown["price_momentum_5m"] = price_norm_5m
        breakdown["price_momentum_1h"] = price_norm_1h

        pump_entry = pumpfun.get(mint) if isinstance(pumpfun, Mapping) else None
        pump_score = None
        pump_rank = None
        tweets_per_min = None
        social_sentiment = None
        if isinstance(pump_entry, Mapping):
            pump_rank = pump_entry.get("rank")
            buyers_last_hour = pump_entry.get("buyersLastHour")
            pump_score = pump_entry.get("score")
            tweets_last_hour = pump_entry.get("tweetsLastHour")
            if tweets_last_hour is not None:
                try:
                    tweets_per_min = float(tweets_last_hour) / 60.0
                except Exception:
                    tweets_per_min = None
            social_sentiment = pump_entry.get("sentiment")
            sources_used.add("pumpfun")
        if buyers_last_hour is not None:
            try:
                breakdown["buyers_last_hour"] = int(float(buyers_last_hour))
            except Exception:
                breakdown["buyers_last_hour"] = buyers_last_hour

        pump_intensity = _pump_intensity_from_rank(
            int(pump_rank) if pump_rank is not None else None
        )
        breakdown["pump_intensity"] = pump_intensity
        if pump_rank is None:
            breakdown["missing_pump_rank"] = True
            momentum_partial = True

        social_min = self._config.social_min
        social_max = self._config.social_max
        tweets_norm = _normalize_min_max(tweets_per_min, minimum=social_min, maximum=social_max)
        breakdown["tweets_per_min"] = tweets_norm
        if tweets_per_min is None:
            breakdown["missing_tweets_per_min"] = True
            momentum_partial = True
        if social_sentiment is None:
            social_sentiment = pump_score
        if social_sentiment is None:
            breakdown["missing_social_sentiment"] = True
            momentum_partial = True
        else:
            try:
                social_sentiment = clamp(float(social_sentiment), 0.0, 1.0)
            except Exception:
                social_sentiment = clamp(float(pump_score or 0.0), 0.0, 1.0)
            breakdown["social_sentiment"] = social_sentiment

        community_score = None
        if pump_score is not None:
            try:
                community_score = clamp(float(pump_score), 0.0, 1.0)
            except Exception:
                community_score = None
        if community_score is None:
            community_score = social_sentiment
        if community_score is None:
            community_score = tweets_norm
        social_base = community_score if community_score is not None else 0.0
        social_denominator = 2.0 if community_score is not None else 1.0
        if social_denominator <= 0:
            social_denominator = 1.0
        social_score = clamp((social_base + tweets_norm) / social_denominator, 0.0, 1.0)
        breakdown["social_score"] = social_score

        if MOMENTUM_EMIT_TOTAL is not None:
            for source in sources_used:
                try:
                    MOMENTUM_EMIT_TOTAL.labels(host=source).inc()
                except Exception:  # pragma: no cover - metrics optional
                    pass

        weights = self._config.weights
        weighted = (
            (volume_rank_1h or 0.0) * weights.volume_rank_1h
            + (volume_rank_24h or 0.0) * weights.volume_rank_24h
            + price_norm_5m * weights.price_momentum_5m
            + price_norm_1h * weights.price_momentum_1h
            + pump_intensity * weights.pump_intensity
            + (social_sentiment or 0.0) * weights.social_sentiment
        )
        weight_sum = (
            weights.volume_rank_1h
            + weights.volume_rank_24h
            + weights.price_momentum_5m
            + weights.price_momentum_1h
            + weights.pump_intensity
            + weights.social_sentiment
        )
        momentum_score = weighted / weight_sum if weight_sum else weighted

        if stale_due_to_errors:
            momentum_partial = True
        if not sources_used:
            stale_due_to_errors = True
            momentum_partial = True

        return MomentumComputation(
            mint=mint,
            momentum_score=momentum_score,
            pump_intensity=pump_intensity,
            pump_score=community_score,
            social_score=social_score,
            social_sentiment=social_sentiment,
            tweets_per_min=tweets_norm,
            buyers_last_hour=int(buyers_last_hour) if buyers_last_hour is not None else None,
            momentum_partial=momentum_partial,
            momentum_stale=stale_due_to_errors,
            momentum_sources=tuple(sorted(sources_used)),
            momentum_breakdown=breakdown,
            latency_ms=0.0,
        )

    def _extract_depth(self, snapshot: GoldenSnapshot) -> float:
        liq = snapshot.liq or {}
        depth_usd = None
        if isinstance(liq, Mapping):
            depth_usd = liq.get("depth_usd_by_pct") or liq.get("depth_pct")
            if isinstance(depth_usd, Mapping):
                for key in ("1", "1.0", "100", "100bps"):
                    if key in depth_usd:
                        try:
                            return float(depth_usd[key])
                        except Exception:
                            continue
        metrics = snapshot.metrics or {}
        if isinstance(metrics, Mapping):
            depth_val = metrics.get("depth") or metrics.get("depth_usd")
            if depth_val is not None:
                try:
                    return float(depth_val)
                except Exception:
                    pass
        return 0.0

    async def _fetch_birdeye_snapshot(self) -> Mapping[str, Mapping[str, Any]]:
        cache_key = "birdeye_snapshot"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        params = {
            "sort_by": "volume_24h_usd",
            "sort_type": "desc",
            "page": 1,
            "per_page": 250,
            "chain": "solana",
        }
        headers = {"accept": "application/json", "x-chain": "solana"}
        payload = await self._request_json(
            _BIRDEYE_TOKEN_LIST,
            host="public-api.birdeye.so",
            params=params,
            headers=headers,
        )
        tokens = []
        if isinstance(payload, Mapping):
            data = payload.get("data")
            if isinstance(data, Mapping):
                tokens = data.get("tokens") or []
        result: Dict[str, Dict[str, Any]] = {}
        for token in tokens or []:
            if not isinstance(token, Mapping):
                continue
            address = token.get("address") or token.get("mint")
            if not isinstance(address, str):
                continue
            address = canonical_mint(address)
            if not address:
                continue
            result[address] = {
                "volume_1h_usd": token.get("volume_1h_usd"),
                "volume_24h_usd": token.get("volume_24h_usd"),
                "rank": token.get("rank"),
            }
        self._cache_set(cache_key, result)
        return result

    async def _fetch_dexscreener_trending(self) -> Mapping[str, Mapping[str, Any]]:
        cache_key = "dexscreener_trending"
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        params = {"chainId": "solana"}
        payload = await self._request_json(
            _DEXSCREENER_TRENDING,
            host="api.dexscreener.com",
            params=params,
        )
        result: Dict[str, Dict[str, Any]] = {}
        if isinstance(payload, Mapping):
            data = payload.get("pairs")
            if isinstance(data, list):
                for entry in data:
                    if not isinstance(entry, Mapping):
                        continue
                    base = entry.get("baseToken")
                    if not isinstance(base, Mapping):
                        continue
                    mint = base.get("address")
                    if not isinstance(mint, str):
                        continue
                    mint = canonical_mint(mint)
                    if not mint:
                        continue
                    price_change = entry.get("priceChange")
                    result[mint] = {
                        "rank": entry.get("rank"),
                        "priceChange": price_change if isinstance(price_change, Mapping) else {},
                    }
        self._cache_set(cache_key, result)
        return result

    async def _fetch_dexscreener_tokens(self, mints: Sequence[str]) -> Mapping[str, Mapping[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {}
        chunks: list[list[str]] = []
        batch: list[str] = []
        for mint in mints:
            if len(batch) >= 100:
                chunks.append(batch)
                batch = []
            batch.append(mint)
        if batch:
            chunks.append(batch)
        for chunk in chunks:
            path = _DEXSCREENER_TOKENS + ",".join(chunk)
            try:
                payload = await self._request_json(path, host="api.dexscreener.com")
            except MomentumError as exc:
                self._record_error("dexscreener", exc)
                continue
            if not isinstance(payload, Mapping):
                continue
            data = payload.get("pairs") or payload.get("tokens")
            if isinstance(data, list):
                for entry in data:
                    if not isinstance(entry, Mapping):
                        continue
                    base = entry.get("baseToken")
                    if isinstance(base, Mapping):
                        mint = base.get("address")
                    else:
                        mint = entry.get("address")
                    if not isinstance(mint, str):
                        continue
                    mint = canonical_mint(mint)
                    if not mint:
                        continue
                    result[mint] = entry
        return result

    async def _fetch_pumpfun(self) -> Mapping[str, Mapping[str, Any]]:
        cache_key = "pumpfun_trending"
        cached = self._cache_get(cache_key, ttl=_SOCIAL_CACHE_TTL_SEC)
        if cached is not None:
            return cached
        payload = await self._request_json(_PUMPFUN_TRENDING, host="pump.fun")
        result: Dict[str, Dict[str, Any]] = {}
        if isinstance(payload, Mapping):
            data = payload.get("tokens") or payload.get("items") or payload.get("results")
            if isinstance(data, list):
                for entry in data:
                    if not isinstance(entry, Mapping):
                        continue
                    mint = entry.get("tokenAddress") or entry.get("address")
                    if not isinstance(mint, str):
                        continue
                    mint = canonical_mint(mint)
                    if not mint:
                        continue
                    result[mint] = {
                        "rank": entry.get("rank"),
                        "buyersLastHour": entry.get("buyersLastHour") or entry.get("buyers_last_hour"),
                        "score": entry.get("score") or entry.get("pumpScore"),
                        "tweetsLastHour": entry.get("tweetsLastHour") or entry.get("tweets_last_hour"),
                        "sentiment": entry.get("sentiment"),
                    }
        self._cache_set(cache_key, result, ttl=_SOCIAL_CACHE_TTL_SEC)
        return result

    async def _request_json(
        self,
        url: str,
        *,
        host: str,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> Any:
        limiter = self._limiters.get(host)
        breaker = self._breakers.get(host)
        if breaker and breaker.is_open:
            raise MomentumRateLimitError(f"circuit open for {host}")
        if limiter:
            acquired = await limiter.acquire(timeout=_TOTAL_TIMEOUT_SEC)
            if not acquired:
                raise MomentumTimeoutError(f"token bucket unavailable for {host}")
        request_headers = {"accept": "application/json"}
        if headers:
            request_headers.update(headers)
        cache_entry = self._etag_cache.get(url)
        if cache_entry is not None:
            etag, data, cached_at = cache_entry
            if now_ts() - cached_at <= _CACHE_TTL_SEC and etag:
                request_headers.setdefault("If-None-Match", etag)
            request_headers.setdefault("Accept-Encoding", "gzip, deflate")
        attempts = 2
        last_error: Exception | None = None
        for attempt in range(attempts):
            if attempt and _RETRY_BACKOFF_SEC > 0:
                await asyncio.sleep(_RETRY_BACKOFF_SEC + random.random() * 0.05)
            try:
                async with host_request(url):
                    session = await get_session()
                    async with session.get(
                        url,
                        params=dict(params or {}),
                        headers=request_headers,
                        timeout=self._session_timeout,
                    ) as resp:
                        if resp.status == 304 and cache_entry is not None:
                            return cache_entry[1]
                        if resp.status == 429:
                            if breaker:
                                breaker.record_failure()
                            if MOMENTUM_ERROR_TOTAL is not None:
                                try:
                                    MOMENTUM_ERROR_TOTAL.labels(host=host, reason="429").inc()
                                except Exception:
                                    pass
                            raise MomentumRateLimitError(f"429 from {host}")
                        if resp.status in {400, 401, 403, 404, 422}:
                            if MOMENTUM_ERROR_TOTAL is not None:
                                try:
                                    MOMENTUM_ERROR_TOTAL.labels(host=host, reason=str(resp.status)).inc()
                                except Exception:
                                    pass
                            raise MomentumParameterError(f"HTTP {resp.status} from {host}")
                        if 500 <= resp.status < 600:
                            text = await resp.text()
                            raise MomentumError(f"HTTP {resp.status} {text[:120]}")
                        body = await resp.read()
                        try:
                            data = json.loads(body.decode())
                        except Exception as exc:
                            raise MomentumParseError(f"Failed to parse JSON from {host}: {exc}") from exc
                        etag = resp.headers.get("ETag")
                        if etag:
                            self._etag_cache[url] = (etag, data, now_ts())
                        if breaker:
                            breaker.record_success()
                        return data
            except HostCircuitOpenError as exc:
                raise MomentumRateLimitError(str(exc)) from exc
            except asyncio.TimeoutError as exc:
                last_error = MomentumTimeoutError(f"timeout contacting {host}")
            except MomentumError as exc:
                last_error = exc
            except Exception as exc:  # pragma: no cover - defensive network guard
                last_error = MomentumError(str(exc))
        if last_error is None:
            last_error = MomentumError(f"failed to fetch {url}")
        raise last_error

    def _cache_get(self, key: str, ttl: float = _CACHE_TTL_SEC) -> Any:
        entry = self._cache.get(key)
        if not entry:
            return None
        timestamp, value = entry
        if now_ts() - timestamp > ttl:
            self._cache.pop(key, None)
            return None
        return value

    def _cache_set(self, key: str, value: Any, ttl: float = _CACHE_TTL_SEC) -> None:
        self._cache[key] = (now_ts(), value)

    def _record_error(self, host: str, exc: Exception) -> None:
        if MOMENTUM_ERROR_TOTAL is not None:
            reason = exc.__class__.__name__
            try:
                MOMENTUM_ERROR_TOTAL.labels(host=host, reason=reason).inc()
            except Exception:  # pragma: no cover - metrics optional
                pass
        logger.debug("Momentum source %s failed: %s", host, exc, exc_info=True)

    def _log_update(self, result: MomentumComputation) -> None:
        try:
            payload = {
                "mint": result.mint,
                "momentum_score": result.momentum_score,
                "pump_intensity": result.pump_intensity,
                "social_score": result.social_score,
                "sources": list(result.momentum_sources),
                "partial": result.momentum_partial,
                "stale": result.momentum_stale,
                "lat_ms": result.latency_ms,
            }
            payload.update({k: v for k, v in result.momentum_breakdown.items() if isinstance(v, (int, float))})
            logger.info("momentum.update %s", json.dumps(payload, separators=(",", ":")))
        except Exception:  # pragma: no cover - logging safety
            logger.debug("Failed to log momentum update", exc_info=True)


def _round4(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), 4)
    except Exception:
        return None


__all__ = ["MomentumAgent", "MomentumConfig", "MomentumComputation", "load_momentum_config"]

