"""Runtime self-check routines for the live launcher.

This module performs a lightweight, non-blocking diagnostics sweep so the
runtime can surface readiness information to the UI without gating startup.
The checks deliberately favour resilience over strict accuracy – transient
failures are marked as degraded states instead of raising exceptions.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import threading

try:  # Optional dependency – gracefully degrade when unavailable
    import aiohttp
except Exception:  # pragma: no cover - aiohttp not installed in some environments
    aiohttp = None  # type: ignore[assignment]

from .. import prices
from .. import synthetic_depth
from .. import token_scanner
from ..event_bus import verify_broker_connection


_LOG = logging.getLogger(__name__)

_DEFAULT_SEED_TOKENS = (
    "So11111111111111111111111111111111111111112",  # SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
)

_DEXSCREENER_TRENDING = "https://api.dexscreener.com/latest/dex/tokens/trending"

_CHECK_TIMEOUT = 10.0
_SESSION_TIMEOUT = aiohttp.ClientTimeout(total=_CHECK_TIMEOUT) if aiohttp else None


@dataclass(slots=True)
class CheckResult:
    name: str
    status: str
    detail: str
    duration_ms: float
    extra: Mapping[str, Any] | None = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "status": self.status,
            "detail": self.detail,
            "duration_ms": round(self.duration_ms, 3),
        }
        if self.extra:
            payload.update(dict(self.extra))
        return payload


def _now_ms() -> float:
    return time.perf_counter() * 1000.0


def _copy_status(source: Mapping[str, Any]) -> Dict[str, Any]:
    try:
        return json.loads(json.dumps(source))
    except Exception:  # pragma: no cover - defensive copy fallback
        return dict(source)


class SelfCheckRunner:
    """Coordinate asynchronous runtime self-check diagnostics."""

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
        seed_tokens: Iterable[str] | None = None,
    ) -> None:
        self._logger = logger or _LOG
        tokens = list(seed_tokens or _DEFAULT_SEED_TOKENS)
        self._seed_tokens: List[str] = [tok for tok in tokens if tok]
        if not self._seed_tokens:
            self._seed_tokens = list(_DEFAULT_SEED_TOKENS)
        self._status: MutableMapping[str, Any] = {
            "status": "pending",
            "detail": "Self-check pending",
            "checks": {},
            "started_at": None,
            "completed_at": None,
        }
        self._lock = threading.Lock()
        self._task: Optional[asyncio.Task[Any]] = None

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return _copy_status(self._status)

    def _mutate(self, **fields: Any) -> None:
        with self._lock:
            self._status.update(fields)

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run(), name="self-check")

    async def stop(self) -> None:
        task = self._task
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        self._task = None

    async def _run(self) -> None:
        self._mutate(status="running", detail="Self-check running", started_at=time.time())
        start = _now_ms()
        results: List[CheckResult] = []
        try:
            if aiohttp is None:
                self._logger.warning("Self-check running without aiohttp; HTTP checks will be marked degraded")
                session = None
            else:
                session = aiohttp.ClientSession(timeout=_SESSION_TIMEOUT)
            try:
                results.extend(await self._execute_checks(session))
            finally:
                if session is not None:
                    await session.close()
        except Exception as exc:  # pragma: no cover - defensive guard
            self._logger.exception("Self-check execution failed: %s", exc)
        duration = max(0.0, (_now_ms() - start) / 1000.0)
        checks_payload = {result.name: result.to_dict() for result in results}
        overall_status, detail = self._reduce_status(results)
        self._mutate(
            status=overall_status,
            detail=detail,
            checks=checks_payload,
            completed_at=time.time(),
            duration_s=round(duration, 3),
        )
        self._logger.info("Self-check completed: %s (%s)", overall_status.upper(), detail)

    async def _execute_checks(
        self, session: aiohttp.ClientSession | None
    ) -> List[CheckResult]:
        checks = {
            "event_bus": self._check_event_bus(),
            "pyth_prices": self._check_pyth_prices(),
            "helius_das": self._check_das(session),
            "trending": self._check_trending(session),
            "synthetic_depth": self._check_synthetic_depth(),
        }
        tasks = {name: asyncio.create_task(coro) for name, coro in checks.items()}
        results: List[CheckResult] = []
        for name, task in tasks.items():
            try:
                result = await asyncio.wait_for(task, timeout=_CHECK_TIMEOUT)
            except asyncio.TimeoutError:
                task.cancel()
                with contextlib.suppress(Exception):
                    await task
                result = CheckResult(
                    name=name,
                    status="failed",
                    detail="timed out",
                    duration_ms=_CHECK_TIMEOUT * 1000.0,
                )
            except Exception as exc:  # pragma: no cover - defensive
                self._logger.exception("Self-check task crashed: %s", exc)
                result = CheckResult(
                    name=name,
                    status="failed",
                    detail=str(exc),
                    duration_ms=_CHECK_TIMEOUT * 1000.0,
                )
            results.append(result)
        return results

    async def _check_event_bus(self) -> CheckResult:
        start = _now_ms()
        try:
            ok = await verify_broker_connection(timeout=1.5)
        except Exception as exc:
            detail = f"broker ping failed: {exc}"
            status = "failed"
        else:
            status = "ok" if ok else "failed"
            detail = "broker round-trip succeeded" if ok else "broker round-trip missed"
        duration = _now_ms() - start
        return CheckResult("event_bus", status, detail, duration)

    async def _check_pyth_prices(self) -> CheckResult:
        start = _now_ms()
        tokens = self._seed_tokens[:2]
        missing: List[str] = []
        try:
            quotes = await prices.fetch_price_quotes_async(tokens)
        except Exception as exc:
            detail = f"price fetch failed: {exc}"[:240]
            status = "failed"
        else:
            for token in tokens:
                quote = quotes.get(token)
                price_ok = bool(getattr(quote, "price_usd", 0.0))
                if not price_ok:
                    missing.append(token)
            if missing and len(missing) == len(tokens):
                status = "failed"
                detail = f"no Pyth quotes for {', '.join(missing)}"
            elif missing:
                status = "degraded"
                detail = f"missing quote for {', '.join(missing)}"
            else:
                status = "ok"
                detail = "Pyth quotes resolved"
        duration = _now_ms() - start
        extra = {"tokens": tokens, "missing": missing} if missing else {"tokens": tokens}
        return CheckResult("pyth_prices", status, detail, duration, extra=extra)

    async def _check_das(
        self, session: aiohttp.ClientSession | None
    ) -> CheckResult:
        start = _now_ms()
        if session is None:
            return CheckResult(
                "helius_das",
                "degraded",
                "aiohttp unavailable; DAS check skipped",
                _now_ms() - start,
            )
        rpc_url = os.getenv("SOLANA_RPC_URL") or os.getenv("DAS_BASE_URL", "")
        limit = max(5, int(os.getenv("DAS_DISCOVERY_LIMIT", "10") or 10))
        try:
            search_fn = getattr(token_scanner, "_helius_search_assets")
        except AttributeError:
            entries = []
            status = "degraded"
            detail = "helius search helper unavailable"
        else:
            try:
                entries = await search_fn(
                    session,
                    limit=limit,
                    rpc_url=rpc_url,
                )
            except RuntimeError as exc:
                lowered = str(exc).lower()
                if "429" in lowered or "rate" in lowered or "timeout" in lowered:
                    status = "degraded"
                    detail = f"helius DAS degraded: {exc}"[:240]
                else:
                    status = "failed"
                    detail = f"helius DAS failed: {exc}"[:240]
                entries = []
            except Exception as exc:  # pragma: no cover - unexpected failure
                status = "failed"
                detail = f"helius DAS error: {exc}"[:240]
                entries = []
            else:
                if entries:
                    status = "ok"
                    detail = f"helius DAS returned {len(entries)} assets"
                else:
                    status = "degraded"
                    detail = "helius DAS returned no assets"
        duration = _now_ms() - start
        extra = {"count": len(entries)} if entries else {}
        if rpc_url:
            extra["rpc_url"] = rpc_url
        return CheckResult("helius_das", status, detail, duration, extra=extra)

    async def _check_trending(
        self, session: aiohttp.ClientSession | None
    ) -> CheckResult:
        start = _now_ms()
        if session is None:
            return CheckResult(
                "trending",
                "degraded",
                "aiohttp unavailable; trending check skipped",
                _now_ms() - start,
            )
        try:
            response = await session.get(_DEXSCREENER_TRENDING, headers={"User-Agent": "solhunter-self-check"})
        except Exception as exc:
            status = "failed"
            detail = f"dexscreener trending request failed: {exc}"[:240]
            count = 0
        else:
            if response.status == 200:
                try:
                    payload = await response.json(content_type=None)
                except Exception:
                    payload = {}
                entries: Optional[List[Any]] = None
                if isinstance(payload, Mapping):
                    for key in ("tokens", "pairs", "data", "results"):
                        candidate = payload.get(key)
                        if isinstance(candidate, list) and candidate:
                            entries = candidate
                            break
                if entries:
                    status = "ok"
                    detail = f"dexscreener trending returned {len(entries)} tokens"
                    count = len(entries)
                else:
                    status = "degraded"
                    detail = "dexscreener trending empty"
                    count = 0
            elif response.status in {429, 520, 521, 522, 523}:
                status = "degraded"
                detail = f"dexscreener trending rate limited ({response.status})"
                count = 0
            else:
                status = "failed"
                detail = f"dexscreener trending HTTP {response.status}"
                count = 0
        duration = _now_ms() - start
        return CheckResult("trending", status, detail, duration, extra={"count": count})

    async def _check_synthetic_depth(self) -> CheckResult:
        start = _now_ms()
        token = self._seed_tokens[0]
        try:
            delta = await synthetic_depth.compute_depth_change(token)
        except Exception as exc:
            status = "degraded"
            detail = f"synthetic depth failed: {exc}"[:240]
            delta = None
        else:
            status = "ok"
            detail = f"synthetic depth updated (Δ={delta:.2f})"
        duration = _now_ms() - start
        extra: Dict[str, Any] = {"token": token}
        if delta is not None:
            extra["delta_usd"] = round(float(delta), 6)
        return CheckResult("synthetic_depth", status, detail, duration, extra=extra)

    def _reduce_status(self, results: Iterable[CheckResult]) -> tuple[str, str]:
        counts = {"ok": 0, "degraded": 0, "failed": 0}
        messages: List[str] = []
        for result in results:
            status = result.status
            if status not in counts:
                counts.setdefault(status, 0)
            counts[status] = counts.get(status, 0) + 1
            messages.append(f"{result.name}:{status}")
        ok = counts.get("ok", 0)
        degraded = counts.get("degraded", 0)
        failed = counts.get("failed", 0)
        if failed == 0 and degraded == 0 and ok > 0:
            overall = "ok"
        elif failed == 0 and degraded > 0 and ok > 0:
            overall = "partial"
        elif failed > 0 and ok > 0:
            overall = "partial"
        elif ok == 0 and (failed > 0 or degraded > 0):
            overall = "degraded"
        else:
            overall = "partial"
        detail = ", ".join(messages) if messages else "no checks"
        return overall, detail


__all__ = ["SelfCheckRunner"]
