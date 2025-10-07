from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from typing import Any, Awaitable, Callable, Dict, Optional

from .types import ActionBundle, EvaluationResult, ScoredToken

log = logging.getLogger(__name__)


class EvaluationService:
    """Run agent evaluations in parallel with memoisation."""

    def __init__(
        self,
        input_queue: "asyncio.Queue[ScoredToken]",
        output_queue: "asyncio.Queue[ActionBundle]",
        agent_manager,
        portfolio,
        *,
        default_workers: Optional[int] = None,
        cache_ttl: float = 10.0,
        on_result: Optional[Callable[[EvaluationResult], Awaitable[None] | None]] = None,
        should_skip: Optional[Callable[[str], bool]] = None,
    ) -> None:
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        self.cache_ttl = max(0.0, cache_ttl)
        self._cache: Dict[str, tuple[float, EvaluationResult]] = {}
        self._stopped = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._worker_tasks: list[asyncio.Task] = []
        workers = default_workers if default_workers is not None else (os.cpu_count() or 4)
        try:
            worker_count = int(workers)
        except (TypeError, ValueError):
            worker_count = os.cpu_count() or 4
        self._worker_limit = max(5, worker_count)
        self._on_result = on_result
        self._should_skip = should_skip
        self._min_volume = self._parse_threshold("DISCOVERY_MIN_VOLUME_USD")
        self._min_liquidity = self._parse_threshold("DISCOVERY_MIN_LIQUIDITY_USD")

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="evaluation_service")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        workers = [
            asyncio.create_task(self._worker_loop(idx), name=f"evaluation_worker:{idx}")
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
        name = f"worker-{idx}"
        while not self._stopped.is_set():
            scored: ScoredToken | None = None
            try:
                scored = await self.input_queue.get()
            except asyncio.CancelledError:
                break
            try:
                if self._should_skip and self._should_skip(scored.token):
                    log.debug("EvaluationService %s skipped %s via should_skip", name, scored.token)
                    continue
                result = await self._evaluate_token(scored)
                if result:
                    await self._notify_result(result)
                    if result.actions:
                        bundle = ActionBundle(
                            token=result.token,
                            actions=result.actions,
                            created_at=time.time(),
                            metadata={"latency": result.latency, "cached": result.cached},
                        )
                        await self.output_queue.put(bundle)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log.exception("EvaluationService %s failure: %s", name, exc)
            finally:
                if scored is not None:
                    self.input_queue.task_done()

    async def _notify_result(self, result: EvaluationResult) -> None:
        if not self._on_result:
            return
        try:
            maybe = self._on_result(result)
            if inspect.isawaitable(maybe):
                await maybe  # pragma: no branch - best effort support
        except Exception:  # pragma: no cover - defensive logging
            log.exception("Result callback failed")

    async def _evaluate_token(self, scored: ScoredToken) -> Optional[EvaluationResult]:
        now = time.time()
        cached = self._cache.get(scored.token)
        if cached and (now - cached[0]) < self.cache_ttl:
            return EvaluationResult(
                token=scored.token,
                actions=list(cached[1].actions),
                latency=0.0,
                cached=True,
                metadata=dict(cached[1].metadata),
            )
        start = time.perf_counter()
        errors: list[str] = []
        actions: list[Dict] = []
        try:
            ctx = await self.agent_manager.evaluate_with_swarm(scored.token, self.portfolio)
            actions = list(ctx.actions)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            errors.append(str(exc))
            log.exception("Evaluation failed for %s", scored.token)
        exploratory_action = None
        if not actions:
            exploratory_action = self._maybe_build_exploratory_action(scored)
            if exploratory_action:
                actions.append(exploratory_action)
                log.info(
                    "EvaluationService generated exploratory action for %s (score=%.4f)",
                    scored.token,
                    scored.score,
                )
        latency = time.perf_counter() - start
        metadata = {"score": scored.score, "rank": scored.rank}
        if exploratory_action:
            metadata["exploratory"] = True
        result = EvaluationResult(
            token=scored.token,
            actions=actions,
            latency=latency,
            cached=False,
            errors=errors,
            metadata=metadata,
        )
        if self.cache_ttl and not errors:
            self._cache[scored.token] = (time.time(), result)
        return result

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value)
            text = str(value).strip()
            if not text:
                return None
            return float(text)
        except Exception:
            return None

    def _parse_threshold(self, env_key: str) -> float:
        value = self._coerce_float(os.getenv(env_key, "0"))
        return float(value) if value is not None and value > 0 else 0.0

    def _maybe_build_exploratory_action(self, scored: ScoredToken) -> Optional[Dict[str, Any]]:
        meta = scored.candidate.metadata or {}
        price = self._coerce_float(
            meta.get("price")
            or meta.get("usd_price")
            or meta.get("price_usd")
            or meta.get("last_price")
        )
        if price is None or price <= 0:
            return None

        volume = self._coerce_float(meta.get("volume"))
        if self._min_volume and (volume is None or volume < self._min_volume):
            return None

        liquidity = self._coerce_float(meta.get("liquidity"))
        if self._min_liquidity and (liquidity is None or liquidity < self._min_liquidity):
            return None

        action_metadata: Dict[str, Any] = {
            "predicted_score": float(scored.score),
            "rank": int(scored.rank),
            "discovered_at": scored.candidate.discovered_at,
            "source": scored.candidate.source,
            "exploratory": True,
        }
        if volume is not None:
            action_metadata["volume"] = volume
        if liquidity is not None:
            action_metadata["liquidity"] = liquidity

        for label in ("symbol", "name"):
            value = meta.get(label)
            if isinstance(value, str) and value:
                action_metadata[label] = value

        discovery_score = self._coerce_float(meta.get("discovery_score") or meta.get("score"))
        if discovery_score is not None:
            action_metadata["discovery_score"] = discovery_score

        sources = meta.get("sources")
        if isinstance(sources, list):
            action_metadata["sources"] = [str(src) for src in sources if isinstance(src, str)]

        trending_rank = meta.get("trending_rank") or meta.get("rank")
        try:
            if trending_rank is not None:
                action_metadata["trending_rank"] = int(trending_rank)
        except (TypeError, ValueError):
            pass

        action: Dict[str, Any] = {
            "token": scored.token,
            "side": "buy",
            "type": "exploratory",
            "price": float(price),
            "metadata": action_metadata,
        }

        amount = self._coerce_float(meta.get("probe_amount") or meta.get("amount"))
        if amount is not None and amount > 0:
            action["amount"] = amount

        notional = self._coerce_float(meta.get("notional_usd") or meta.get("budget"))
        if notional is not None and notional > 0:
            action["notional_usd"] = notional

        return action
