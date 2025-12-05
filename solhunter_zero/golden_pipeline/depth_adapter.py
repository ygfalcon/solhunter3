from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

import aiohttp

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter, Histogram  # type: ignore
except Exception:  # pragma: no cover - prometheus optional
    Counter = None  # type: ignore
    Histogram = None  # type: ignore

from ..http import HostCircuitOpenError, get_session, host_request
from ..prices import _parse_pyth_mapping, _pyth_decimal_price, _pyth_price_from_info
from ..token_aliases import canonical_mint
from .types import DepthSnapshot

logger = logging.getLogger(__name__)


def _parse_ladder(raw: str | None) -> tuple[float, ...]:
    if not raw:
        return (1000.0, 5000.0, 10_000.0, 25_000.0)
    values: list[float] = []
    for chunk in raw.split(","):
        try:
            value = float(chunk.strip())
        except Exception:
            continue
        if value > 0:
            values.append(value)
    return tuple(values) or (1000.0, 5000.0, 10_000.0, 25_000.0)


_USDC_MINT = os.getenv(
    "GOLDEN_DEPTH_USDC_MINT",
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
)
_USDC_DECIMALS = int(os.getenv("GOLDEN_DEPTH_USDC_DECIMALS", "6") or 6)
_USDC_FACTOR = 10 ** _USDC_DECIMALS
_JUP_PRICE_URL = os.getenv("JUPITER_PRICE_URL", "https://lite-api.jup.ag/price/v3").rstrip("/")
_JUP_QUOTE_URL = os.getenv("JUPITER_QUOTE_URL", "https://lite-api.jup.ag/swap/v1/quote").rstrip("/")
_PYTH_HERMES_URL = os.getenv(
    "PYTH_HERMES_PRICE_URL",
    "https://hermes.pyth.network/v2/updates/price/latest",
).rstrip("/")
_CONNECT_TIMEOUT = float(os.getenv("GOLDEN_DEPTH_CONNECT_TIMEOUT", "0.3") or 0.3)
_READ_TIMEOUT = float(os.getenv("GOLDEN_DEPTH_READ_TIMEOUT", "0.7") or 0.7)
_TOTAL_TIMEOUT = float(os.getenv("GOLDEN_DEPTH_TOTAL_TIMEOUT", "1.2") or 1.2)
_PER_MINT_BUDGET = float(os.getenv("GOLDEN_DEPTH_BUDGET_SEC", "0.8") or 0.8)
_MIN_RETRY_BUDGET = float(os.getenv("GOLDEN_DEPTH_MIN_RETRY_SEC", "0.18") or 0.18)
_LADDER_USD = _parse_ladder(os.getenv("GOLDEN_DEPTH_LADDER"))
_PYTH_MAPPING_TTL = float(os.getenv("GOLDEN_DEPTH_PYTH_MAPPING_TTL", "900") or 900)
_MAX_RAW_AMOUNT = int(float(os.getenv("GOLDEN_DEPTH_MAX_RAW_AMOUNT", "9e15")))
_DEFAULT_STALENESS_SEC = float(os.getenv("GOLDEN_DEPTH_JUP_STALE_SEC", "15") or 15)


if Counter is not None:  # pragma: no branch - optional metrics
    DEPTH_EMIT_TOTAL = Counter(
        "golden_depth_emit_total",
        "Golden depth emissions",
        labelnames=("source",),
    )
    QUOTE_ERROR_TOTAL = Counter(
        "golden_quote_error_total",
        "Golden depth quote errors",
        labelnames=("reason",),
    )
else:  # pragma: no cover - metrics optional
    DEPTH_EMIT_TOTAL = None
    QUOTE_ERROR_TOTAL = None

if Histogram is not None:  # pragma: no branch - optional metrics
    DEPTH_LATENCY_MS = Histogram(
        "golden_depth_adapter_latency_ms",
        "Golden depth adapter latency",
        buckets=(10, 25, 50, 75, 100, 200, 300, 400, 600, 800, 1200),
    )
    DEPTH_CACHE_AGE_SECONDS = Histogram(
        "golden_depth_cache_age_seconds",
        "Age of cached Golden depth snapshots in seconds",
        buckets=(0.25, 0.5, 1, 2, 4, 6, 8, 10, 12, 15, 20, 30, 45, 60, 90, 120),
    )
else:  # pragma: no cover - optional metrics
    DEPTH_LATENCY_MS = None
    DEPTH_CACHE_AGE_SECONDS = None


class _Budget:
    __slots__ = ("limit", "_start")

    def __init__(self, limit: float) -> None:
        self.limit = max(0.1, float(limit))
        self._start = time.perf_counter()

    def elapsed(self) -> float:
        return max(0.0, time.perf_counter() - self._start)

    def remaining(self) -> float:
        return max(0.0, self.limit - self.elapsed())

    def exhausted(self) -> bool:
        return self.remaining() <= 0.0


@dataclass(slots=True)
class AnchorResult:
    price: float
    confidence: float
    publish_time: float
    source: str
    degraded: bool = False

    @property
    def staleness_ms(self) -> float:
        now = time.time()
        return max(0.0, (now - self.publish_time) * 1000.0)


@dataclass(slots=True)
class QuoteLeg:
    notional_usd: float
    realized_usd: float
    effective_price: float
    impact_bps: float
    route: Sequence[str]
    hops: int
    direction: str  # "sell" (token->USDC) or "buy" (USDC->token)


class QuoteError(RuntimeError):
    pass


class QuoteRateLimitError(QuoteError):
    pass


class QuoteParameterError(QuoteError):
    pass


class GoldenDepthAdapter:
    """Compute depth bands for Golden snapshots via routed quotes."""

    def __init__(
        self,
        *,
        enabled: bool,
        submit_depth: Callable[[DepthSnapshot], Awaitable[None]],
        decimals_resolver: Callable[[str], int],
        max_active: int = 8,
        cache_ttl: float = 10.0,
    ) -> None:
        self._enabled = enabled
        self._submit_depth = submit_depth
        self._resolve_decimals = decimals_resolver
        self._max_active = max(1, int(max_active))
        try:
            ttl_value = float(cache_ttl)
        except Exception:
            ttl_value = 10.0
        self._cache_ttl = max(1.0, ttl_value)
        if ttl_value < 1.0:
            logger.warning(
                "depth cache_ttl %.3fs too low; using minimum 1s to avoid provider hammering",
                ttl_value,
            )
        self._activity: MutableMapping[str, tuple[float, float]] = {}
        self._cache: MutableMapping[str, tuple[float, DepthSnapshot]] = {}
        self._cache_warned: MutableMapping[str, float] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._task: asyncio.Task | None = None
        self._ladder = _LADDER_USD
        self._pyth_mapping: Dict[str, str] = {}
        self._pyth_mapping_expiry = 0.0
        self._pyth_lock = asyncio.Lock()

    @property
    def cache_ttl(self) -> float:
        return self._cache_ttl

    def _record_cache_age(self, mint: str, age: float) -> None:
        if DEPTH_CACHE_AGE_SECONDS is not None:
            try:
                DEPTH_CACHE_AGE_SECONDS.observe(max(0.0, float(age)))
            except Exception:  # pragma: no cover - metrics optional
                pass
        if age <= self._cache_ttl:
            return
        now = time.time()
        last_warn = self._cache_warned.get(mint)
        if last_warn is not None and now - last_warn < max(self._cache_ttl, 5.0):
            return
        self._cache_warned[mint] = now
        logger.debug(
            "depth cache for %s stale (age=%.2fs, ttl=%.2fs)",
            mint,
            age,
            self._cache_ttl,
        )

    def record_activity(self, mint: str, weight: float = 1.0) -> None:
        if not self._enabled:
            return
        canonical = canonical_mint(mint)
        now = time.time()
        weight = max(0.0, float(weight))
        prev = self._activity.get(canonical)
        score = weight + (prev[0] if prev else 0.0)
        self._activity[canonical] = (score, now)

    async def start(self) -> None:
        if not self._enabled or self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="golden_depth_adapter")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._task = None

    async def _run_loop(self) -> None:
        try:
            while self._running:
                await self._tick()
                await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("golden depth adapter loop crashed")

    async def _tick(self) -> None:
        if not self._enabled:
            return
        now = time.time()
        cutoff = now - 60.0
        active: list[tuple[str, float]] = []
        for mint, (score, ts) in list(self._activity.items()):
            if ts < cutoff:
                self._activity.pop(mint, None)
                continue
            active.append((mint, score))
        active.sort(key=lambda item: item[1], reverse=True)
        selected = [mint for mint, _ in active[: self._max_active]]
        for mint in selected:
            await self._refresh_mint(mint)

    async def _refresh_mint(self, mint: str) -> None:
        if not self._enabled:
            return
        try:
            await self._generate_snapshot(mint)
        except Exception as exc:  # noqa: BLE001 - defensive guard
            logger.warning("golden depth adapter failed for %s: %s", mint, exc)
            if QUOTE_ERROR_TOTAL is not None:
                QUOTE_ERROR_TOTAL.labels(reason=exc.__class__.__name__).inc()

    async def _get_session(self) -> aiohttp.ClientSession:
        return await get_session()

    async def _generate_snapshot(self, mint: str) -> None:
        start = time.perf_counter()
        now = time.time()
        cached = self._cache.get(mint)
        if cached:
            age = now - cached[0]
            self._record_cache_age(mint, age)
            if age < self._cache_ttl:
                return
        session = await self._get_session()
        budget = _Budget(_PER_MINT_BUDGET)
        try:
            decimals = max(0, int(self._resolve_decimals(mint)))
        except Exception:
            decimals = 6
        anchor = await self._resolve_anchor(session, mint, budget)
        legs: list[QuoteLeg] = []
        quote_failed_reason: str | None = None
        if not budget.exhausted() and anchor.price > 0:
            try:
                legs = await self._collect_quotes(
                    session, mint, anchor, decimals, budget
                )
            except QuoteRateLimitError as exc:
                quote_failed_reason = "rate_limit"
                logger.debug("rate limited while quoting %s: %s", mint, exc)
            except QuoteParameterError as exc:
                quote_failed_reason = "parameter"
                logger.debug("quote parameter rejection for %s: %s", mint, exc)
            except Exception as exc:
                quote_failed_reason = exc.__class__.__name__
                logger.debug("transient quote error for %s: %s", mint, exc)
        has_sell_quote = any(leg.direction == "sell" for leg in legs)
        asof = time.time()
        latency_ms = (time.perf_counter() - start) * 1000.0
        snapshot = self.build_snapshot(
            mint,
            anchor,
            legs,
            asof=asof,
            latency_ms=latency_ms,
        )
        snapshot.source = "jup_route" if has_sell_quote else "pyth_synthetic"
        snapshot.degraded = not has_sell_quote
        self._validate_snapshot(snapshot, anchor)
        self._cache[mint] = (time.time(), snapshot)
        self._cache_warned.pop(mint, None)
        self._record_cache_age(mint, 0.0)
        await self._submit_depth(snapshot)
        if DEPTH_EMIT_TOTAL is not None:
            DEPTH_EMIT_TOTAL.labels(source=snapshot.source or "unknown").inc()
        if DEPTH_LATENCY_MS is not None:
            DEPTH_LATENCY_MS.observe(latency_ms)
        if quote_failed_reason and QUOTE_ERROR_TOTAL is not None:
            QUOTE_ERROR_TOTAL.labels(reason=quote_failed_reason).inc()
        logger.info(
            "golden depth emit",
            extra={
                "mint": mint,
                "source": snapshot.source,
                "bid_usd": snapshot.px_bid_usd,
                "ask_usd": snapshot.px_ask_usd,
                "depth_0_1pct": (snapshot.depth_bands_usd or {}).get("0.1"),
                "depth_0_5pct": (snapshot.depth_bands_usd or {}).get("0.5"),
                "depth_1_0pct": (snapshot.depth_bands_usd or {}).get("1.0"),
                "adapter_latency_ms": latency_ms,
            },
        )

    async def _resolve_anchor(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        budget: _Budget,
    ) -> AnchorResult:
        try:
            anchor = await self._fetch_jupiter_anchor(session, mint, budget)
        except QuoteRateLimitError as exc:
            logger.debug("jupiter anchor rate limited for %s: %s", mint, exc)
            anchor = None
        except Exception:
            anchor = None
        if anchor and not self._anchor_stale(anchor):
            return anchor
        try:
            fallback = await self._fetch_pyth_anchor(session, mint, budget)
        except QuoteRateLimitError as exc:
            logger.debug("pyth anchor rate limited for %s: %s", mint, exc)
            fallback = None
        if fallback:
            return fallback
        price = anchor.price if anchor else 1.0
        confidence = anchor.confidence if anchor else 0.0
        publish_time = anchor.publish_time if anchor else time.time()
        return AnchorResult(
            price=price or 1.0,
            confidence=confidence,
            publish_time=publish_time,
            source="synthetic",
            degraded=True,
        )

    def _anchor_stale(self, anchor: AnchorResult) -> bool:
        age = max(0.0, time.time() - anchor.publish_time)
        if age > _DEFAULT_STALENESS_SEC:
            return True
        return anchor.price <= 0

    async def _fetch_jupiter_anchor(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        budget: _Budget,
    ) -> AnchorResult | None:
        if budget.exhausted():
            return None
        params = {"ids": canonical_mint(mint)}
        timeout = aiohttp.ClientTimeout(
            total=min(_TOTAL_TIMEOUT, budget.remaining() or _TOTAL_TIMEOUT),
            sock_connect=_CONNECT_TIMEOUT,
            sock_read=_READ_TIMEOUT,
        )
        try:
            async with host_request(_JUP_PRICE_URL):
                async with session.get(
                    _JUP_PRICE_URL,
                    params=params,
                    timeout=timeout,
                ) as resp:
                    if resp.status == 429:
                        raise QuoteRateLimitError("jupiter price rate limited")
                    if resp.status >= 400:
                        text = await resp.text()
                        raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                    payload = await resp.json()
        except QuoteRateLimitError:
            raise
        except HostCircuitOpenError:
            raise
        except Exception as exc:
            logger.debug("jupiter price failed for %s: %s", mint, exc)
            return None
        entry = None
        if isinstance(payload, Mapping):
            entry = payload.get(mint) or payload.get(canonical_mint(mint))
        if not isinstance(entry, Mapping):
            return None
        price = entry.get("usdPrice") or entry.get("price")
        try:
            price_value = float(price)
        except Exception:
            return None
        if not math.isfinite(price_value) or price_value <= 0:
            return None
        block_id = entry.get("blockId")
        publish_time = time.time()
        if isinstance(block_id, (int, float)) and block_id > 0:
            publish_time = publish_time  # slot only, no timestamp available
        return AnchorResult(
            price=price_value,
            confidence=0.0,
            publish_time=publish_time,
            source="jupiter",
            degraded=False,
        )

    async def _fetch_pyth_anchor(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        budget: _Budget,
    ) -> AnchorResult | None:
        feed_id = await self._resolve_pyth_feed(mint)
        if not feed_id or budget.exhausted():
            return None
        params = [("ids[]", feed_id)]
        timeout = aiohttp.ClientTimeout(
            total=min(_TOTAL_TIMEOUT, max(0.2, budget.remaining() or _TOTAL_TIMEOUT)),
            sock_connect=_CONNECT_TIMEOUT,
            sock_read=_READ_TIMEOUT,
        )
        try:
            async with host_request(_PYTH_HERMES_URL):
                async with session.get(
                    _PYTH_HERMES_URL,
                    params=params,
                    timeout=timeout,
                ) as resp:
                    if resp.status == 429:
                        raise QuoteRateLimitError("pyth rate limited")
                    if resp.status >= 400:
                        text = await resp.text()
                        raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                    payload = await resp.json()
        except QuoteRateLimitError:
            raise
        except HostCircuitOpenError:
            raise
        except Exception as exc:
            logger.debug("pyth hermes failed for %s: %s", mint, exc)
            return None
        records: list[Mapping[str, Any]] = []
        if isinstance(payload, Mapping):
            parsed = payload.get("parsed") or payload.get("data")
            if isinstance(parsed, list):
                records.extend([item for item in parsed if isinstance(item, Mapping)])
        if isinstance(payload, list):
            records.extend([item for item in payload if isinstance(item, Mapping)])
        if not records:
            return None
        record = records[0]
        price_info = record.get("price")
        if not isinstance(price_info, Mapping):
            return None
        price_value = _pyth_price_from_info(price_info)
        if price_value is None or price_value <= 0:
            return None
        confidence = _pyth_decimal_price(price_info.get("conf"), price_info.get("expo"))
        if confidence is None:
            confidence = 0.0
        publish_time = price_info.get("publish_time") or price_info.get("publishTime")
        if isinstance(publish_time, (int, float)):
            publish_sec = float(publish_time)
            if publish_sec > 1e12:
                publish_sec = publish_sec / 1000.0
        else:
            publish_sec = time.time()
        return AnchorResult(
            price=price_value,
            confidence=abs(confidence),
            publish_time=publish_sec,
            source="pyth",
            degraded=True,
        )

    async def _resolve_pyth_feed(self, mint: str) -> str | None:
        now = time.time()
        async with self._pyth_lock:
            if not self._pyth_mapping or now >= self._pyth_mapping_expiry:
                try:
                    mapping, _ = _parse_pyth_mapping()
                except Exception as exc:  # pragma: no cover - defensive
                    logger.debug("pyth mapping refresh failed: %s", exc)
                    self._pyth_mapping = {}
                else:
                    self._pyth_mapping = {
                        canonical_mint(key): identifier.feed_id
                        for key, identifier in mapping.items()
                        if getattr(identifier, "feed_id", None)
                    }
                self._pyth_mapping_expiry = now + _PYTH_MAPPING_TTL
            return self._pyth_mapping.get(canonical_mint(mint))

    async def _collect_quotes(
        self,
        session: aiohttp.ClientSession,
        mint: str,
        anchor: AnchorResult,
        decimals: int,
        budget: _Budget,
    ) -> list[QuoteLeg]:
        legs: list[QuoteLeg] = []
        for usd in self._ladder:
            if budget.exhausted():
                break
            raw_amount = self._usd_to_token_amount(usd, anchor.price, decimals)
            if raw_amount <= 0:
                continue
            try:
                quote = await self._request_quote(
                    session,
                    input_mint=mint,
                    output_mint=_USDC_MINT,
                    amount=raw_amount,
                    budget=budget,
                )
            except QuoteParameterError:
                continue
            leg = self._parse_sell_leg(quote, raw_amount, usd, anchor, decimals)
            if leg:
                legs.append(leg)
        probe_usd = self._ladder[0] if self._ladder else 1000.0
        if probe_usd > 0 and not budget.exhausted():
            usdc_amount = self._usd_to_usdc_amount(probe_usd)
            if usdc_amount > 0:
                try:
                    quote = await self._request_quote(
                        session,
                        input_mint=_USDC_MINT,
                        output_mint=mint,
                        amount=usdc_amount,
                        budget=budget,
                    )
                except QuoteParameterError:
                    quote = None
                if quote:
                    leg = self._parse_buy_leg(quote, usdc_amount, anchor, decimals)
                    if leg:
                        legs.append(leg)
        return legs

    async def _request_quote(
        self,
        session: aiohttp.ClientSession,
        *,
        input_mint: str,
        output_mint: str,
        amount: int,
        budget: _Budget,
    ) -> Mapping[str, Any] | None:
        if amount <= 0 or budget.exhausted():
            return None
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(amount),
            "slippageBps": "50",
            "swapMode": "ExactIn",
        }
        attempts = 1
        if budget.remaining() > _MIN_RETRY_BUDGET:
            attempts = 2
        last_error: Exception | None = None
        for attempt in range(attempts):
            timeout = aiohttp.ClientTimeout(
                total=min(_TOTAL_TIMEOUT, max(0.2, budget.remaining() or _TOTAL_TIMEOUT)),
                sock_connect=_CONNECT_TIMEOUT,
                sock_read=_READ_TIMEOUT,
            )
            try:
                async with host_request(_JUP_QUOTE_URL):
                    async with session.get(
                        _JUP_QUOTE_URL,
                        params=params,
                        timeout=timeout,
                    ) as resp:
                        if resp.status == 429:
                            raise QuoteRateLimitError("jupiter quote rate limited")
                        if resp.status in {400, 404, 422}:
                            text = await resp.text()
                            raise QuoteParameterError(text[:200])
                        if resp.status >= 500:
                            text = await resp.text()
                            raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                        resp.raise_for_status()
                        payload = await resp.json()
                        if isinstance(payload, Mapping):
                            return payload
                        return None
            except QuoteParameterError:
                raise
            except QuoteRateLimitError:
                raise
            except (aiohttp.ClientError, asyncio.TimeoutError, RuntimeError) as exc:
                last_error = exc
                if attempt >= attempts - 1:
                    break
                if budget.remaining() <= _MIN_RETRY_BUDGET:
                    break
                jitter = random.uniform(0.05, 0.2)
                await asyncio.sleep(min(jitter, budget.remaining()))
            except HostCircuitOpenError as exc:
                last_error = exc
                break
        if last_error:
            raise last_error
        return None

    def _parse_sell_leg(
        self,
        quote: Mapping[str, Any],
        raw_amount: int,
        usd_requested: float,
        anchor: AnchorResult,
        decimals: int,
    ) -> QuoteLeg | None:
        try:
            out_amount = float(quote.get("outAmount"))
        except Exception:
            return None
        token_amount = raw_amount / (10**decimals)
        if token_amount <= 0:
            return None
        realized_usd = out_amount / _USDC_FACTOR
        effective_price = realized_usd / token_amount if token_amount > 0 else 0.0
        if effective_price <= 0:
            return None
        impact = 0.0
        if anchor.price > 0:
            impact = max(0.0, (anchor.price - effective_price) / anchor.price * 10_000)
        route, hops = self._extract_route(quote)
        return QuoteLeg(
            notional_usd=float(usd_requested),
            realized_usd=float(realized_usd),
            effective_price=float(effective_price),
            impact_bps=float(impact),
            route=route,
            hops=hops,
            direction="sell",
        )

    def _parse_buy_leg(
        self,
        quote: Mapping[str, Any],
        usdc_amount: int,
        anchor: AnchorResult,
        decimals: int,
    ) -> QuoteLeg | None:
        try:
            out_amount = float(quote.get("outAmount"))
        except Exception:
            return None
        tokens_out = out_amount / (10**decimals)
        usd_in = usdc_amount / _USDC_FACTOR
        if tokens_out <= 0 or usd_in <= 0:
            return None
        effective_price = usd_in / tokens_out
        impact = 0.0
        if anchor.price > 0:
            impact = max(0.0, (effective_price - anchor.price) / anchor.price * 10_000)
        route, hops = self._extract_route(quote)
        return QuoteLeg(
            notional_usd=float(usd_in),
            realized_usd=float(usd_in),
            effective_price=float(effective_price),
            impact_bps=float(impact),
            route=route,
            hops=hops,
            direction="buy",
        )

    def _extract_route(self, quote: Mapping[str, Any]) -> tuple[Sequence[str], int]:
        plan = quote.get("routePlan")
        if not isinstance(plan, list):
            return [], 0
        labels: list[str] = []
        for entry in plan:
            if not isinstance(entry, Mapping):
                continue
            swap = entry.get("swapInfo")
            label = None
            if isinstance(swap, Mapping):
                label = swap.get("label") or swap.get("ammLabel")
            if not label and isinstance(entry.get("label"), str):
                label = entry.get("label")
            if isinstance(label, str) and label:
                labels.append(label)
            elif isinstance(swap, Mapping):
                amm_key = swap.get("ammKey")
                if isinstance(amm_key, str) and amm_key:
                    labels.append(amm_key[:6])
        return labels, len(labels)

    def _usd_to_token_amount(self, usd: float, price: float, decimals: int) -> int:
        if price <= 0:
            return 0
        tokens = usd / price
        raw = int(tokens * (10**decimals))
        return max(0, min(raw, _MAX_RAW_AMOUNT))

    def _usd_to_usdc_amount(self, usd: float) -> int:
        raw = int(usd * _USDC_FACTOR)
        return max(0, min(raw, _MAX_RAW_AMOUNT))

    def _validate_snapshot(self, snapshot: DepthSnapshot, anchor: AnchorResult) -> None:
        bands = snapshot.depth_bands_usd or {}
        depth_10 = float(bands.get("0.1", 0.0) or 0.0)
        depth_50 = float(bands.get("0.5", 0.0) or 0.0)
        depth_100 = float(bands.get("1.0", 0.0) or 0.0)
        if depth_50 < depth_10:
            depth_50 = depth_10
        if depth_100 < depth_50:
            depth_100 = depth_50
        snapshot.depth_bands_usd = {"0.1": depth_10, "0.5": depth_50, "1.0": depth_100}
        now = time.time()
        if snapshot.asof > now + 1.0:
            snapshot.asof = now
        if snapshot.source == "pyth_synthetic" and anchor.price > 0:
            min_spread_bps = 0.0
            if anchor.confidence > 0:
                min_spread_bps = 2.0 * (anchor.confidence / anchor.price) * 10_000
            actual_spread = 0.0
            if snapshot.px_bid_usd and snapshot.px_ask_usd and snapshot.mid_usd > 0:
                actual_spread = (
                    (snapshot.px_ask_usd - snapshot.px_bid_usd)
                    / snapshot.mid_usd
                    * 10_000
                )
            if actual_spread < min_spread_bps:
                half = snapshot.mid_usd * (min_spread_bps / 20_000.0)
                snapshot.px_bid_usd = max(0.0, snapshot.mid_usd - half)
                snapshot.px_ask_usd = snapshot.mid_usd + half
                snapshot.spread_bps = max(snapshot.spread_bps, min_spread_bps)

    @staticmethod
    def build_snapshot(
        mint: str,
        anchor: AnchorResult,
        legs: Sequence[QuoteLeg],
        *,
        asof: float | None = None,
        latency_ms: float | None = None,
    ) -> DepthSnapshot:
        asof = asof or time.time()
        legs = [leg for leg in legs if leg.notional_usd > 0 and leg.realized_usd > 0]
        sell_legs = [leg for leg in legs if leg.direction == "sell"]
        buy_legs = [leg for leg in legs if leg.direction == "buy"]

        def _best_sell_price() -> float | None:
            if not sell_legs:
                return None
            return max(leg.effective_price for leg in sell_legs)

        def _best_buy_price() -> float | None:
            if not buy_legs:
                return None
            return min(leg.effective_price for leg in buy_legs)

        def _interpolate(target_bps: float, samples: Sequence[QuoteLeg]) -> float:
            if not samples:
                return 0.0
            ordered = sorted(samples, key=lambda leg: leg.realized_usd)
            prev_notional = 0.0
            prev_impact = 0.0
            for leg in ordered:
                impact = leg.impact_bps
                notional = leg.realized_usd
                if impact <= target_bps:
                    prev_notional = notional
                    prev_impact = impact
                    continue
                if prev_notional <= 0:
                    return 0.0
                if math.isclose(impact, prev_impact):
                    return prev_notional
                ratio = (target_bps - prev_impact) / (impact - prev_impact)
                ratio = min(max(ratio, 0.0), 1.0)
                return prev_notional + (notional - prev_notional) * ratio
            return prev_notional

        depth_10bps = _interpolate(10.0, sell_legs)
        depth_50bps = max(depth_10bps, _interpolate(50.0, sell_legs))
        depth_100bps = max(depth_50bps, _interpolate(100.0, sell_legs))

        mid = anchor.price
        bid = _best_sell_price()
        ask = _best_buy_price()
        if bid is None:
            min_spread_bps = 0.0
            if anchor.price > 0 and anchor.confidence > 0:
                min_spread_bps = 2.0 * (anchor.confidence / anchor.price) * 10_000
            half = mid * (min_spread_bps / 20_000.0)
            bid = max(0.0, mid - half)
        if ask is None:
            min_spread_bps = 0.0
            if anchor.price > 0 and anchor.confidence > 0:
                min_spread_bps = 2.0 * (anchor.confidence / anchor.price) * 10_000
            half = mid * (min_spread_bps / 20_000.0)
            ask = mid + half

        spread_bps = 0.0
        if bid > 0 and ask > 0 and mid > 0:
            spread_bps = max(0.0, (ask - bid) / mid * 10_000)

        sweeps: list[Dict[str, float | str]] = []
        for leg in sell_legs + buy_legs:
            sweeps.append(
                {
                    "direction": leg.direction,
                    "usd": float(leg.realized_usd),
                    "impact_bps": float(leg.impact_bps),
                }
            )

        route_meta: Dict[str, Any] | None = None
        if sell_legs:
            best_sell = max(sell_legs, key=lambda leg: leg.realized_usd)
            route_meta = {"hops": best_sell.hops, "dexes": list(best_sell.route)}
        elif buy_legs:
            best_buy = min(buy_legs, key=lambda leg: leg.impact_bps)
            route_meta = {"hops": best_buy.hops, "dexes": list(best_buy.route)}
        if route_meta is not None:
            if sweeps:
                route_meta["sweeps"] = sweeps
            if latency_ms is not None:
                route_meta["latency_ms"] = float(latency_ms)

        depth_pct = {
            "0.1": depth_10bps,
            "0.5": depth_50bps,
            "1.0": depth_100bps,
            "1": depth_100bps,
        }

        snapshot = DepthSnapshot(
            mint=mint,
            venue="golden.depth",
            mid_usd=mid,
            spread_bps=spread_bps,
            depth_pct=depth_pct,
            asof=asof,
            px_bid_usd=bid,
            px_ask_usd=ask,
            depth_bands_usd={"0.1": depth_10bps, "0.5": depth_50bps, "1.0": depth_100bps},
            degraded=anchor.degraded,
            source="jup_route" if sell_legs else "pyth_synthetic",
            route_meta=route_meta,
            staleness_ms=anchor.staleness_ms,
        )
        return snapshot


__all__ = ["GoldenDepthAdapter"]
