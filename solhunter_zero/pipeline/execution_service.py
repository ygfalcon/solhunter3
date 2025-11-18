from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
import time
from typing import Any, Awaitable, Callable, List, Mapping, Optional, Sequence

from ..event_bus import publish
from ..lru import TTLCache
from ..schemas import ActionExecuted
from .types import ActionBundle, ExecutionReceipt

log = logging.getLogger(__name__)

_DEFAULT_VENUES: list[str] = ["raydium", "orca", "jupiter", "phoenix"]


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


class _TransientExecutionError(RuntimeError):
    """Internal marker for retryable failures."""


class _RetryExhausted(RuntimeError):
    """Raised when retry budget is exhausted."""

    def __init__(self, attempts: int, cause: Exception) -> None:
        super().__init__(str(cause))
        self.attempts = attempts
        self.cause = cause



class ExecutionService:
    """Execute action bundles using agent manager's execution agent."""

    def __init__(
        self,
        input_queue: "asyncio.Queue[ActionBundle]",
        agent_manager,
        *,
        lane_workers: int = 2,
        on_receipt: Optional[Callable[[ExecutionReceipt], Awaitable[None] | None]] = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self.input_queue = input_queue
        self.agent_manager = agent_manager
        self._config = dict(config) if config is not None else {}
        env_venues = os.getenv("EXECUTION_VENUES")
        self._configured_venues = self._normalize_venues(
            self._config.get("execution_venues")
            or self._config.get("venue_chain")
            or self._config.get("venues")
            or env_venues
        )
        self._fast_mode = os.getenv("FAST_PIPELINE_MODE", "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        lane_override: int | None
        if lane_workers is not None:
            try:
                lane_override = int(lane_workers)
            except (TypeError, ValueError):
                lane_override = None
        else:
            env_lane = os.getenv("EXECUTION_LANE_WORKERS")
            if env_lane in (None, ""):
                lane_override = None
            else:
                try:
                    lane_override = int(env_lane)
                except (TypeError, ValueError):
                    lane_override = None
        if lane_override is not None:
            lane_count = lane_override
        else:
            if self._fast_mode:
                lane_count = 2
            else:
                cpu_count = os.cpu_count() or 4
                lane_count = min(max(4, cpu_count), 12)
        lane_count = max(1, min(24, lane_count))
        self._lane_count = lane_count
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._worker_tasks: list[asyncio.Task] = []
        self._on_receipt = on_receipt
        ttl_raw = os.getenv("EXECUTION_IDEMPOTENCY_TTL", "600")
        try:
            ttl_val = float(ttl_raw or 600.0)
        except (TypeError, ValueError):
            ttl_val = 600.0
        self._seen_bundles = TTLCache(maxsize=4096, ttl=max(1.0, ttl_val))
        default_receipt_timeout = 0.5 if self._fast_mode else 2.0
        self._receipt_timeout = _env_float(
            "EXECUTION_RECEIPT_TIMEOUT", default_receipt_timeout
        )
        self._max_pos_usd = _env_float("MAX_POSITION_USD_PER_TOKEN", 5000.0)
        self._max_notional_usd = _env_float("MAX_NOTIONAL_USD_PER_ORDER", 2000.0)
        self._max_open_orders = _env_int("MAX_OPEN_ORDERS", 32)
        self._allow_partial_sell = _env_bool("ALLOW_PARTIAL_SELL", True)
        self._dry_run = _env_bool("TRADING_DRY_RUN", False)

    async def start(self) -> None:
        if self._task is None:
            self._stopped.clear()
            log.info("ExecutionService started", extra={"lanes": self._lane_count})
            self._task = asyncio.create_task(self._run(), name="execution_service")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        log.info("ExecutionService stopped", extra={"lanes": self._lane_count})

    async def _run(self) -> None:
        workers = [
            asyncio.create_task(self._worker_loop(idx), name=f"exec-lane:{idx}")
            for idx in range(self._lane_count)
        ]
        self._worker_tasks = workers
        try:
            await self._stopped.wait()
        except asyncio.CancelledError:
            pass
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            self._worker_tasks.clear()

    async def _worker_loop(self, idx: int) -> None:
        lane_name = f"lane-{idx}"
        while not self._stopped.is_set():
            bundle: ActionBundle | None = None
            try:
                bundle = await self.input_queue.get()
            except asyncio.CancelledError:
                break
            try:
                bundle_id = self._bundle_id(bundle)
                log.debug(
                    "Execution lane %s received bundle %s token=%s",
                    lane_name,
                    bundle_id,
                    getattr(bundle, "token", None),
                )
                if self._seen_bundles.get(bundle_id) is not None:
                    log.debug(
                        "Execution lane %s skipping duplicate bundle %s", lane_name, bundle_id
                    )
                    now = time.perf_counter()
                    dup = ExecutionReceipt(
                        token=bundle.token,
                        success=True,
                        results=[{"status": "duplicate"}],
                        errors=[],
                        started_at=now,
                        finished_at=now,
                        lane=lane_name,
                        metadata={
                            "idempotent_hit": True,
                            "bundle_id": bundle_id,
                            "trace_id": (bundle.metadata or {}).get("trace_id"),
                        },
                    )
                    await self._notify_receipt(dup)
                    continue
                self._seen_bundles.set(bundle_id, time.time())
                receipt = await self._execute_bundle(
                    bundle, lane=lane_name, bundle_id=bundle_id
                )
                await self._notify_receipt(receipt)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("ExecutionService %s failure: %s", lane_name, exc)
            finally:
                if bundle is not None:
                    self.input_queue.task_done()

    async def _notify_receipt(self, receipt: ExecutionReceipt) -> None:
        if not self._on_receipt:
            return
        try:
            maybe = self._on_receipt(receipt)
            if inspect.isawaitable(maybe):
                try:
                    await asyncio.wait_for(maybe, timeout=self._receipt_timeout)
                except asyncio.TimeoutError:
                    log.warning("Receipt callback timeout")
        except Exception:  # pragma: no cover - defensive logging
            log.exception("Receipt callback failed")

    @staticmethod
    def _normalize_venues(raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, Mapping):
            for key in ("execution_venues", "venue_chain", "venues"):
                if key in raw:
                    return ExecutionService._normalize_venues(raw.get(key))
            return []
        if isinstance(raw, Sequence) and not isinstance(raw, (bytes, bytearray, str)):
            return [str(item).strip() for item in raw if str(item).strip()]
        if isinstance(raw, str):
            raw_list = raw.replace(";", ",").split(",")
            return [item.strip() for item in raw_list if item.strip()]
        return []

    def _build_venue_chain(self, bundle_metadata: Mapping[str, Any] | None) -> list[str]:
        venues: list[str] = []
        seen: set[str] = set()

        def _extend(candidates: Sequence[str] | None) -> None:
            if not candidates:
                return
            for venue in candidates:
                try:
                    candidate = str(venue).strip()
                except Exception:
                    continue
                if not candidate:
                    continue
                key = candidate.lower()
                if key in seen:
                    continue
                venues.append(candidate)
                seen.add(key)

        metadata = bundle_metadata or {}
        venue_hint = metadata.get("venue_hint")
        if venue_hint:
            _extend([str(venue_hint)])

        bundle_chain = self._normalize_venues(
            metadata.get("venue_chain") or metadata.get("venues")
        )
        _extend(bundle_chain)

        manager_meta = getattr(self.agent_manager, "metadata", None)
        _extend(self._normalize_venues(manager_meta))

        _extend(self._configured_venues)
        _extend(_DEFAULT_VENUES)

        return venues

    async def _execute_bundle(
        self,
        bundle: ActionBundle,
        *,
        lane: str | None = None,
        bundle_id: str | None = None,
    ) -> ExecutionReceipt:
        started = time.perf_counter()
        bundle_id = bundle_id or self._bundle_id(bundle)
        metadata = dict(bundle.metadata or {})
        trace_id = metadata.get("trace_id")
        ok, pretrade_errors, adjustments = await self._pretrade_checks(bundle)
        actions = [dict(action) for action in bundle.actions]
        if adjustments.get("qty") is not None:
            qty_val = adjustments["qty"]
            for action in actions:
                for key in ("qty", "quantity", "size"):
                    if key in action:
                        action[key] = qty_val
                        break
                else:
                    continue
                break
        if not ok:
            finished = time.perf_counter()
            return ExecutionReceipt(
                token=bundle.token,
                success=False,
                results=[],
                errors=pretrade_errors,
                started_at=started,
                finished_at=finished,
                lane=lane,
                metadata={
                    "bundle_id": bundle_id,
                    "trace_id": trace_id,
                    "pretrade_rejected": True,
                    "adjustments": adjustments or None,
                },
            )

        if self._dry_run:
            finished = time.perf_counter()
            receipt_metadata = {
                "bundle_id": bundle_id,
                "trace_id": trace_id,
                "simulated": True,
                "adjustments": adjustments or None,
            }
            return ExecutionReceipt(
                token=bundle.token,
                success=True,
                results=[{"status": "simulated"}],
                errors=[],
                started_at=started,
                finished_at=finished,
                lane=lane,
                metadata=receipt_metadata,
            )

        limit_price = metadata.get("limit_price")
        slippage_bps = metadata.get("slippage_bps")
        commitment = metadata.get("commitment")
        try:
            total_budget = float(os.getenv("ROUTE_TIMEOUT_TOTAL", "6.0") or 6.0)
        except (TypeError, ValueError):
            total_budget = 6.0
        deadline = time.perf_counter() + max(0.1, total_budget)
        venues = self._build_venue_chain(metadata)
        memory_agent = getattr(self.agent_manager, "memory_agent", None)

        results: List[Any] = []
        errors: List[str] = []
        venue_errors: list[dict[str, Any]] = []
        attempts_total = 0
        selected_venue: str | None = None
        effective_limit: float | None = None
        price_impact_bps: float | None = None
        signature: str | None = None
        confirmed = False

        for venue in venues:
            if time.perf_counter() >= deadline:
                break

            async def _attempt() -> dict[str, Any]:
                remaining = deadline - time.perf_counter()
                return await self._try_venue(
                    bundle,
                    actions,
                    venue=venue,
                    budget_seconds=max(0.1, remaining),
                    limit_price=limit_price,
                    slippage_bps=slippage_bps,
                    lane=lane,
                    memory_agent=memory_agent,
                    bundle_id=bundle_id,
                )

            try:
                attempt_count, attempt_result = await self._retry_transient(
                    _attempt, deadline=deadline
                )
            except _RetryExhausted as exc:
                attempts_total += exc.attempts
                venue_errors.append({"venue": venue, "error": str(exc.cause)})
                errors.append(str(exc.cause))
                continue
            except Exception as exc:
                attempts_total += 1
                venue_errors.append({"venue": venue, "error": str(exc)})
                errors.append(str(exc))
                continue

            attempts_total += attempt_count
            if attempt_result.get("success"):
                selected_venue = venue
                results = attempt_result.get("results", [])
                errors = attempt_result.get("errors", [])
                effective_limit = attempt_result.get("effective_limit")
                price_impact_bps = attempt_result.get("price_impact_bps")
                signature = attempt_result.get("signature")
                break

            venue_errors.append({
                "venue": venue,
                "error": attempt_result.get("errors", []),
            })
            errors.extend(str(err) for err in attempt_result.get("errors", []))

        if selected_venue and signature:
            try:
                confirmed = await self._confirm_sig(signature, commitment)
            except Exception:
                log.exception("Signature confirmation failed for %s", bundle.token)
                confirmed = False

        finished = time.perf_counter()

        if not selected_venue and not errors:
            errors.append("no_venue_available")

        gross_notional = 0.0
        fees_paid = 0.0
        net_notional = 0.0
        for entry in results:
            if not isinstance(entry, dict):
                continue
            try:
                gross_notional += float(entry.get("gross_notional_usd", 0.0) or 0.0)
                fees_paid += float(entry.get("fees_usd", 0.0) or 0.0)
                net_notional += float(entry.get("net_notional_usd", 0.0) or 0.0)
                price_impact_bps = price_impact_bps or entry.get("price_impact_bps")
                signature = signature or entry.get("signature") or entry.get("txid")
                if effective_limit is None:
                    eff = entry.get("effective_limit") or entry.get("limit_price")
                    if eff is not None:
                        effective_limit = float(eff)
            except Exception:
                continue

        confirmed_flag = confirmed or not signature

        receipt_metadata: dict[str, Any] = {
            "bundle_id": bundle_id,
            "trace_id": trace_id,
            "venue": selected_venue,
            "attempts": attempts_total,
            "pending": bool(signature and not confirmed),
            "deferred_settlement": bool(signature and not confirmed),
            "effective_limit": effective_limit,
            "price_impact_bps": price_impact_bps,
            "adjustments": adjustments or None,
            "venue_errors": venue_errors or None,
            "signature": signature,
            "commitment": commitment,
            "confirmed": confirmed_flag,
        }
        if gross_notional:
            receipt_metadata["gross_notional_usd"] = gross_notional
        if fees_paid:
            receipt_metadata["fees_usd"] = fees_paid
        if net_notional:
            receipt_metadata["net_notional_usd"] = net_notional

        return ExecutionReceipt(
            token=bundle.token,
            success=selected_venue is not None and not errors,
            results=results,
            errors=errors,
            started_at=started,
            finished_at=finished,
            lane=lane,
            metadata=receipt_metadata,
        )

    def _bundle_id(self, bundle: ActionBundle) -> str:
        bid = getattr(bundle, "bundle_id", None) or (bundle.metadata or {}).get("bundle_id")
        if isinstance(bid, str) and bid:
            return bid
        base = (
            f"{bundle.token}|{(bundle.metadata or {}).get('side')}|"
            f"{(bundle.metadata or {}).get('qty')}|"
            f"{int((bundle.metadata or {}).get('ts', 0))}|"
            f"{(bundle.metadata or {}).get('venue_hint')}"
        )
        return hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()

    async def _pretrade_checks(self, bundle: ActionBundle) -> tuple[bool, list[str], dict]:
        errors: list[str] = []
        adjustments: dict[str, Any] = {}
        port = getattr(self.agent_manager, "portfolio", None) or getattr(self, "portfolio", None)
        try:
            side = (bundle.metadata or {}).get("side")
            qty = float((bundle.metadata or {}).get("qty") or 0.0)
            notional = float((bundle.metadata or {}).get("notional_usd") or 0.0)
        except Exception:
            side, qty, notional = None, 0.0, 0.0

        if notional and notional > self._max_notional_usd:
            errors.append(
                f"order notional exceeds cap: {notional} > {self._max_notional_usd}"
            )

        open_orders = getattr(port, "open_orders", 0) if port else 0
        if isinstance(open_orders, int) and open_orders >= self._max_open_orders:
            errors.append("max open orders reached")

        try:
            pos_usd = (getattr(port, "position_value_usd", {}) or {}).get(bundle.token, 0.0) if port else 0.0
            if pos_usd and (pos_usd + max(notional, 0.0)) > self._max_pos_usd and side in {"buy", "long"}:
                errors.append(
                    f"position cap would be exceeded: {pos_usd}+{notional} > {self._max_pos_usd}"
                )
        except Exception:
            pass

        if side in {"sell", "short"} and self._allow_partial_sell and port:
            try:
                held_qty = (getattr(port, "position_qty", {}) or {}).get(bundle.token, 0.0)
                if held_qty is not None and qty > held_qty > 0:
                    adjustments["qty"] = held_qty
            except Exception:
                pass

        return (len(errors) == 0), errors, adjustments

    async def _try_venue(
        self,
        bundle: ActionBundle,
        actions: list[dict],
        *,
        venue: str,
        budget_seconds: float,
        limit_price: Any,
        slippage_bps: Any,
        lane: str | None,
        memory_agent: Any | None,
        bundle_id: str,
    ) -> dict[str, Any]:
        results: list[Any] = []
        errors: list[str] = []
        signature: str | None = None
        effective_limit: float | None = None
        price_impact: float | None = None
        side = (bundle.metadata or {}).get("side")
        try:
            slip = float(slippage_bps) if slippage_bps is not None else None
        except (TypeError, ValueError):
            slip = None
        if slip is None:
            slip = 100.0 if side in {"buy", "long"} else 80.0

        start = time.perf_counter()
        for action in actions:
            payload = dict(action)
            payload.setdefault("metadata", {})
            try:
                payload_metadata = dict(payload["metadata"])
            except Exception:
                payload_metadata = {}
            payload_metadata.update(
                {
                    "venue": venue,
                    "bundle_id": bundle_id,
                    "lane": lane,
                }
            )
            payload["metadata"] = payload_metadata

            if limit_price is not None:
                payload.setdefault("limit_price", limit_price)
            elif "price" in payload:
                try:
                    base_price = float(payload.get("price") or payload.get("quote_price"))
                    if side in {"buy", "long"}:
                        payload["limit_price"] = base_price * (1 + (slip / 10_000))
                    elif side in {"sell", "short"}:
                        payload["limit_price"] = base_price * (1 - (slip / 10_000))
                except Exception:
                    pass

            elapsed = time.perf_counter() - start
            remaining = max(0.05, budget_seconds - elapsed)
            try:
                result = await asyncio.wait_for(
                    self.agent_manager.executor.execute(payload),
                    timeout=remaining,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._is_transient(exc):
                    raise _TransientExecutionError(str(exc)) from exc
                errors.append(str(exc))
                break

            if memory_agent:
                try:
                    await memory_agent.log(payload)
                except Exception:
                    log.exception("memory log failed for %s", bundle.token)

            publish(
                "action_executed",
                ActionExecuted(action=payload, result=result),
            )

            results.append(result)
            if isinstance(result, dict):
                signature = signature or result.get("signature") or result.get("txid")
                if effective_limit is None:
                    eff = result.get("effective_limit") or result.get("limit_price")
                    if eff is not None:
                        try:
                            effective_limit = float(eff)
                        except (TypeError, ValueError):
                            pass
                if price_impact is None and result.get("price_impact_bps") is not None:
                    try:
                        price_impact = float(result.get("price_impact_bps"))
                    except (TypeError, ValueError):
                        price_impact = None

        return {
            "success": not errors,
            "results": results,
            "errors": errors,
            "signature": signature,
            "effective_limit": effective_limit,
            "price_impact_bps": price_impact,
        }

    async def _retry_transient(
        self,
        coro_factory: Callable[[], Awaitable[dict[str, Any]]],
        *,
        deadline: float,
        backoffs: list[float] | tuple[float, ...] = (0.15, 0.35, 0.75),
    ) -> tuple[int, dict[str, Any]]:
        attempts = 0
        last_exc: Exception | None = None
        backoff_list = list(backoffs)
        while True:
            attempts += 1
            try:
                result = await coro_factory()
                return attempts, result
            except Exception as exc:
                if not (isinstance(exc, _TransientExecutionError) or self._is_transient(exc)):
                    raise
                last_exc = exc
            remaining = deadline - time.perf_counter()
            if remaining <= 0 or attempts >= len(backoff_list) + 1:
                cause = last_exc or _TransientExecutionError("transient failure")
                raise _RetryExhausted(attempts, cause)
            await asyncio.sleep(min(backoff_list[attempts - 1], max(0.0, remaining)))

    async def _confirm_sig(self, signature: str | None, commitment: Any) -> bool:
        if not signature:
            return True
        executor = getattr(self.agent_manager, "executor", None)
        if executor is None:
            return self._fast_mode
        confirm = getattr(executor, "confirm", None) or getattr(
            executor, "confirm_transaction", None
        )
        if confirm is None:
            return self._fast_mode
        try:
            result = confirm(signature, commitment=commitment)  # type: ignore[call-arg]
        except TypeError:
            result = confirm(signature)  # type: ignore[misc]
        if inspect.isawaitable(result):
            try:
                await result
            except Exception:
                log.exception("Confirmation await failed for signature %s", signature)
                return False
        return True

    def _is_transient(self, exc: Exception) -> bool:
        transient_types = (asyncio.TimeoutError, TimeoutError)
        if isinstance(exc, transient_types):
            return True
        message = str(exc).lower()
        for token in ("timeout", "temporar", "429", "too many", "overload"):
            if token in message:
                return True
        return False
