from __future__ import annotations

import asyncio
import inspect
import logging
import os
import time
from typing import Any, Awaitable, Callable, Dict, Optional

from ..util import parse_bool_env
from ..prices import fetch_token_prices_async
try:
    from ..portfolio import dynamic_order_size
except ImportError as exc:  # pragma: no cover - optional dependency guard
    logging.getLogger(__name__).warning(
        "portfolio.dynamic_order_size unavailable (%s); using simplified fallback",
        exc,
    )

    def dynamic_order_size(
        balance: float,
        expected_roi: float,
        predicted_roi: float | None = None,
        volatility: float = 0.0,
        drawdown: float = 0.0,
        *,
        risk_tolerance: float = 0.1,
        max_allocation: float = 0.2,
        max_risk_per_token: float = 0.1,
        max_drawdown: float = 1.0,
        volatility_factor: float = 1.0,
        gas_cost: float | None = None,
        current_allocation: float = 0.0,
        min_portfolio_value: float = 0.0,
        correlation: float | None = None,
        var: float | None = None,
        var_threshold: float | None = None,
    ) -> float:
        del (
            predicted_roi,
            volatility,
            drawdown,
            risk_tolerance,
            max_allocation,
            max_risk_per_token,
            max_drawdown,
            volatility_factor,
            gas_cost,
            current_allocation,
            min_portfolio_value,
            correlation,
            var,
            var_threshold,
        )
        return max(0.0, balance * max(expected_roi, 0.0))

SOL_MINT = "So11111111111111111111111111111111111111112"

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
        self._exploratory_ignore_metrics = parse_bool_env("EXPLORATORY_IGNORE_METRICS", False)
        budget = self._coerce_float(os.getenv("EXPLORATORY_BUDGET_USD"))
        self._exploratory_budget = float(budget) if budget is not None and budget > 0 else 50.0

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
        ctx = None
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
        if actions:
            await self._enrich_action_sizing(scored, actions)
        simulated_actions: list[Dict[str, Any]] = [
            dict(action) for action in actions if action.get("simulated")
        ]
        actions = [action for action in actions if not action.get("simulated")]
        latency = time.perf_counter() - start
        metadata: Dict[str, Any] = {
            "score": scored.score,
            "rank": scored.rank,
            "candidate": dict(scored.candidate.metadata or {}),
        }
        if ctx is not None:
            agents_list: list[str] = []
            if getattr(ctx, "agents", None):
                try:
                    agents_list = [agent.name for agent in ctx.agents]
                except Exception:
                    agents_list = []
            if agents_list:
                metadata["agents"] = agents_list
            if getattr(ctx, "weights", None):
                try:
                    metadata["weights"] = {str(k): float(v) for k, v in ctx.weights.items()}
                except Exception:
                    metadata["weights"] = {str(k): v for k, v in ctx.weights.items()}
            ctx_meta = getattr(ctx, "metadata", None)
            if ctx_meta:
                try:
                    metadata["swarm"] = dict(ctx_meta)
                except Exception:
                    metadata["swarm"] = ctx_meta
        if exploratory_action:
            metadata["exploratory"] = True
        if simulated_actions:
            metadata["simulated_actions"] = simulated_actions
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

    @staticmethod
    def _extract_price(metadata: Dict[str, Any]) -> float | None:
        keys = (
            "price",
            "usd_price",
            "price_usd",
            "last_price",
            "notional_price",
        )
        for key in keys:
            value = metadata.get(key)
            if value is None and "metadata" in metadata and isinstance(metadata.get("metadata"), dict):
                nested = metadata["metadata"].get(key)
                if nested is not None:
                    value = nested
            result = EvaluationService._coerce_float(value)
            if result is not None and result > 0:
                return result
        return None

    async def _enrich_action_sizing(
        self,
        scored: ScoredToken,
        actions: list[Dict[str, Any]],
    ) -> None:
        relevant = [action for action in actions if not action.get("simulated")]
        if not relevant:
            return

        price_tokens: set[str] = set()
        needs_sol_price = False
        for action in relevant:
            side = str(action.get("side", "")).lower()
            if side == "buy":
                needs_sol_price = True
            price = self._coerce_float(action.get("price"))
            if price is None or price <= 0:
                metadata = action.get("metadata")
                if isinstance(metadata, dict) and self._extract_price(metadata) is not None:
                    continue
                price_tokens.add(action.get("token", scored.token))

        if needs_sol_price:
            price_tokens.add(SOL_MINT)

        price_map: Dict[str, float] = {}
        if price_tokens:
            try:
                price_map = await fetch_token_prices_async(price_tokens)
            except Exception as exc:  # pragma: no cover - defensive logging
                log.debug("Price lookup failed for sizing %s: %s", scored.token, exc)
                price_map = {}

        sol_price = self._coerce_float(price_map.get(SOL_MINT)) or 0.0
        sol_position = self.portfolio.balances.get(SOL_MINT)
        available_sol = sol_position.amount if sol_position else 0.0

        valid_actions: list[Dict[str, Any]] = []
        for action in actions:
            if action.get("simulated"):
                valid_actions.append(action)
                continue

            token = str(action.get("token", scored.token))
            metadata = action.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {}

            price = self._coerce_float(action.get("price"))
            if price is None or price <= 0:
                price = self._extract_price(metadata)
            if (price is None or price <= 0) and token in price_map:
                price = self._coerce_float(price_map.get(token))
            if price is not None and price > 0:
                action["price"] = float(price)

            side = str(action.get("side", "")).lower()

            if side == "sell":
                pos = self.portfolio.balances.get(token)
                holdings = pos.amount if pos else 0.0
                desired = self._coerce_float(action.get("amount"))
                amount = min(desired if desired and desired > 0 else holdings, holdings)
                if amount and amount > 0:
                    action["amount"] = float(amount)
                    if price and price > 0:
                        action.setdefault("notional_usd", float(amount * price))
                    valid_actions.append(action)
                else:
                    log.debug("Skipping sell action for %s: no holdings", token)
                continue

            if side == "buy":
                budget = self._coerce_float(action.get("notional_usd"))
                if budget is None:
                    budget = self._coerce_float(metadata.get("notional_usd") or metadata.get("budget"))
                if budget is None and price and price > 0:
                    amount_hint = self._coerce_float(action.get("amount"))
                    if amount_hint and amount_hint > 0:
                        budget = amount_hint * price
                if budget is None or budget <= 0:
                    budget = self._exploratory_budget

                if available_sol > 0 and sol_price > 0:
                    max_notional = available_sol * sol_price
                    budget = max(0.0, min(budget, max_notional))
                elif available_sol <= 0:
                    log.debug("No SOL balance available; using default budget for %s", token)
                    budget = min(budget, self._exploratory_budget)

                if price and price > 0 and budget > 0:
                    amount = budget / price
                    action["amount"] = float(amount)
                    action["notional_usd"] = float(budget)
                    valid_actions.append(action)
                    if sol_price > 0:
                        spent_sol = budget / sol_price
                        available_sol = max(0.0, available_sol - spent_sol)
                else:
                    log.debug("Skipping buy action for %s: missing price or budget", token)
                continue

            # For unsupported sides keep the action as-is
            valid_actions.append(action)

        actions[:] = valid_actions

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
        liquidity = self._coerce_float(meta.get("liquidity"))

        if not self._exploratory_ignore_metrics:
            if self._min_volume and (volume is None or volume < self._min_volume):
                return None
            if self._min_liquidity and (liquidity is None or liquidity < self._min_liquidity):
                return None
        else:
            if volume is None:
                volume = 0.0
            if liquidity is None:
                liquidity = 0.0

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
