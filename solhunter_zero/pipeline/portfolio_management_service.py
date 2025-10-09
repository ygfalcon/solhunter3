from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Optional

from .types import EvaluationResult, ExecutionReceipt

log = logging.getLogger(__name__)


class PortfolioManagementService:
    """Maintain portfolio metrics concurrently with pipeline activity."""

    def __init__(
        self,
        portfolio,
        *,
        input_queue: "asyncio.Queue[Any]" | None = None,
        workers: Optional[int] = None,
        update_interval: float = 15.0,
    ) -> None:
        self.portfolio = portfolio
        self.input_queue: asyncio.Queue[Any] = input_queue or asyncio.Queue(maxsize=256)
        self.update_interval = max(1.0, float(update_interval or 0.0))
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._ticker_task: Optional[asyncio.Task] = None
        self._worker_tasks: list[asyncio.Task] = []
        self._last_update = 0.0

        env_workers: Optional[int] = None
        raw_env = os.getenv("PORTFOLIO_WORKERS")
        if raw_env:
            try:
                env_workers = int(raw_env)
            except ValueError:
                log.warning("Invalid PORTFOLIO_WORKERS=%r; ignoring", raw_env)
        chosen = workers if workers is not None else env_workers
        if chosen is None or chosen <= 0:
            chosen = os.cpu_count() or 5
        self._worker_limit = max(5, int(chosen))

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="portfolio_management_service")
        if self._ticker_task is None:
            self._ticker_task = asyncio.create_task(self._ticker_loop(), name="portfolio_ticker")

    async def stop(self) -> None:
        self._stopped.set()
        if self._ticker_task:
            self._ticker_task.cancel()
            try:
                await self._ticker_task
            except asyncio.CancelledError:
                pass
            self._ticker_task = None
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._worker_tasks:
            for task in self._worker_tasks:
                task.cancel()
            await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            self._worker_tasks.clear()

    async def put(self, payload: Any) -> None:
        await self.input_queue.put(payload)

    async def _run(self) -> None:
        workers = [
            asyncio.create_task(self._worker_loop(idx), name=f"portfolio_worker:{idx}")
            for idx in range(self._worker_limit)
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
        name = f"portfolio-{idx}"
        while not self._stopped.is_set():
            payload: Any = None
            try:
                payload = await self.input_queue.get()
            except asyncio.CancelledError:
                break
            try:
                await self._handle_payload(payload)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("PortfolioManagementService %s failure: %s", name, exc)
            finally:
                if payload is not None:
                    self.input_queue.task_done()

    async def _ticker_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                await asyncio.sleep(self.update_interval)
                await self.put({"type": "tick"})
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("PortfolioManagementService ticker failure")

    async def _handle_payload(self, payload: Any) -> None:
        force_update = False
        prices: dict[str, float] | None = None
        if isinstance(payload, ExecutionReceipt):
            force_update = payload.success
            metadata = getattr(payload, "results", None)
            if isinstance(metadata, list):
                for item in metadata:
                    price = self._extract_price(item)
                    token = getattr(payload, "token", None)
                    if price is not None and isinstance(token, str):
                        prices = prices or {}
                        prices[token] = price
        elif isinstance(payload, EvaluationResult):
            force_update = True
            for action in payload.actions:
                price = self._extract_price(action)
                token = action.get("token") if isinstance(action, dict) else None
                if price is not None and isinstance(token, str):
                    prices = prices or {}
                    prices[token] = price
        elif isinstance(payload, dict):
            if payload.get("type") == "tick":
                force_update = True
            maybe_prices = payload.get("prices")
            if isinstance(maybe_prices, dict):
                prices = {str(k): float(v) for k, v in maybe_prices.items() if self._is_number(v)}
                force_update = True
        else:
            force_update = True

        if prices:
            try:
                self.portfolio.record_prices(prices)
            except Exception:
                log.exception("Failed to record portfolio prices")

        await self._maybe_update_risk_metrics(force=force_update)

    async def _maybe_update_risk_metrics(self, *, force: bool = False) -> None:
        now = time.perf_counter()
        if not force and (now - self._last_update) < self.update_interval:
            return
        self._last_update = now
        try:
            await asyncio.to_thread(self.portfolio.update_risk_metrics)
        except Exception:
            log.exception("Portfolio risk metric update failed")

    @staticmethod
    def _is_number(value: Any) -> bool:
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _extract_price(obj: Any) -> float | None:
        if isinstance(obj, dict):
            for key in ("price", "avg_price", "fill_price", "execution_price"):
                value = obj.get(key)
                try:
                    if value is None:
                        continue
                    price = float(value)
                    if price > 0:
                        return price
                except (TypeError, ValueError):
                    continue
        return None
