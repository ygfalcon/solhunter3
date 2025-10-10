"""Swarm voting stage."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Mapping, Optional, Tuple

from .contracts import vote_dedupe_key
from .kv import KeyValueStore
from .types import Decision, TradeSuggestion
from .utils import now_ts


class VotingStage:
    """Aggregate TradeSuggestion events into Decisions."""

    def __init__(
        self,
        emit: Callable[[Decision], Awaitable[None]],
        *,
        window_ms: int = 400,
        min_window_ms: int | None = None,
        max_window_ms: int | None = None,
        quorum: int = 2,
        min_score: float = 0.04,
        rl_weights: Mapping[str, float] | None = None,
        kv: KeyValueStore | None = None,
        dedupe_ttl: float = 300.0,
        conflict_delta: float = 0.05,
    ) -> None:
        self._emit = emit
        if min_window_ms is None and max_window_ms is None:
            min_window_ms = 300 if window_ms < 300 else window_ms
            max_window_ms = max(500, window_ms)
        elif min_window_ms is None:
            min_window_ms = min(window_ms, max_window_ms)
        elif max_window_ms is None:
            max_window_ms = max(window_ms, min_window_ms)
        if min_window_ms > max_window_ms:
            min_window_ms, max_window_ms = max_window_ms, min_window_ms
        self._min_window_sec = max(0.0, min_window_ms / 1000.0)
        self._max_window_sec = max(self._min_window_sec, max_window_ms / 1000.0)
        self._window_sec = (self._min_window_sec + self._max_window_sec) / 2.0
        self._quorum = quorum
        self._min_score = min_score
        self._pending: Dict[Tuple[str, str, str], List[TradeSuggestion]] = defaultdict(list)
        self._locks: Dict[Tuple[str, str, str], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._timers: Dict[Tuple[str, str, str], asyncio.Task] = {}
        self._weights: Dict[str, float] = {
            agent: float(weight)
            for agent, weight in (rl_weights.items() if rl_weights else [])
        }
        self._kv = kv
        self._dedupe_ttl = dedupe_ttl
        self._conflict_delta = max(0.0, float(conflict_delta))
        self._conflict_scores: Dict[Tuple[str, str], Tuple[str, float]] = {}
        self._decision_buffers: Dict[Tuple[str, str], Decision] = {}
        self._decision_tasks: Dict[Tuple[str, str], asyncio.Task] = {}
        self._decision_lock = asyncio.Lock()
        self._conflict_hold = 0.05

    async def submit(self, suggestion: TradeSuggestion) -> None:
        if suggestion.must:
            await self._emit_must_exit(suggestion)
            return
        key = (suggestion.mint, suggestion.side, suggestion.inputs_hash)
        async with self._locks[key]:
            self._pending[key].append(suggestion)
            if key not in self._timers:
                self._timers[key] = asyncio.create_task(self._finalise_later(key))

    async def _finalise_later(self, key: Tuple[str, str, str]) -> None:
        if self._max_window_sec <= self._min_window_sec:
            await asyncio.sleep(self._min_window_sec)
        else:
            await asyncio.sleep(
                random.uniform(self._min_window_sec, self._max_window_sec)
            )
        async with self._locks[key]:
            suggestions = self._pending.pop(key, [])
            self._timers.pop(key, None)
        if not suggestions:
            return
        now = now_ts()
        valid = [s for s in suggestions if now <= s.generated_at + s.ttl_sec]
        weighted = [
            (s, max(self._weights.get(s.agent, 1.0), 0.0)) for s in valid
        ]
        weighted = [(s, w) for s, w in weighted if w > 0]
        if len(weighted) < self._quorum:
            return
        total_weight = sum(weight for _, weight in weighted)
        if total_weight <= 0:
            return
        total_conf = sum(max(s.confidence, 0.0) * weight for s, weight in weighted)
        score = total_conf / total_weight
        if score < self._min_score:
            return
        weighted_notional = sum(s.notional_usd * weight for s, weight in weighted)
        notional = weighted_notional / total_weight
        agents = sorted({s.agent for s, _ in weighted})
        time_bucket = int(
            min(s.generated_at for s, _ in weighted) / self._window_sec
        )
        order_id = self._build_order_id(
            mint=key[0],
            side=key[1],
            notional=notional,
            snapshot_hash=key[2],
            agents=agents,
            bucket=time_bucket,
        )
        decision = Decision(
            mint=key[0],
            side=key[1],
            notional_usd=notional,
            score=score,
            snapshot_hash=key[2],
            client_order_id=order_id,
            agents=agents,
            ts=now_ts(),
        )
        if self._is_conflicted(decision):
            return
        if self._kv:
            stored = await self._kv.set_if_absent(
                vote_dedupe_key(order_id),
                "1",
                ttl=self._dedupe_ttl,
            )
            if not stored:
                return
        await self._queue_decision(decision)

    async def _emit_must_exit(self, suggestion: TradeSuggestion) -> None:
        now = now_ts()
        score = max(1.0, suggestion.confidence)
        order_id = self._build_order_id(
            mint=suggestion.mint,
            side=suggestion.side,
            notional=suggestion.notional_usd,
            snapshot_hash=suggestion.inputs_hash,
            agents=[suggestion.agent],
            bucket=int(now / max(self._window_sec, 0.001)),
        )
        decision = Decision(
            mint=suggestion.mint,
            side=suggestion.side,
            notional_usd=suggestion.notional_usd,
            score=score,
            snapshot_hash=suggestion.inputs_hash,
            client_order_id=order_id,
            agents=[suggestion.agent],
            ts=now,
        )
        if self._kv:
            stored = await self._kv.set_if_absent(
                vote_dedupe_key(order_id),
                "1",
                ttl=self._dedupe_ttl,
            )
            if not stored:
                return
        self._conflict_scores[(decision.mint, decision.snapshot_hash)] = (
            decision.side,
            decision.score,
        )
        await self._emit(decision)

    def _is_conflicted(self, decision: Decision) -> bool:
        key = (decision.mint, decision.snapshot_hash)
        existing = self._conflict_scores.get(key)
        if existing is None:
            self._conflict_scores[key] = (decision.side, decision.score)
            return False
        side, score = existing
        if side == decision.side:
            if decision.score >= score:
                self._conflict_scores[key] = (decision.side, decision.score)
            return False
        if abs(score - decision.score) <= self._conflict_delta:
            if side == "sell" and decision.side != "sell":
                return True
            self._conflict_scores[key] = (decision.side, decision.score)
            return False
        if decision.score > score:
            self._conflict_scores[key] = (decision.side, decision.score)
        return False

    async def _queue_decision(self, decision: Decision) -> None:
        key = (decision.mint, decision.snapshot_hash)
        emit_now: Optional[Decision] = None
        cancel_task: Optional[asyncio.Task] = None
        async with self._decision_lock:
            existing = self._decision_buffers.get(key)
            if existing is None:
                self._decision_buffers[key] = decision
                task = asyncio.create_task(self._emit_after_delay(key))
                self._decision_tasks[key] = task
            else:
                chosen = self._resolve_conflict(existing, decision)
                cancel_task = self._decision_tasks.pop(key, None)
                self._decision_buffers.pop(key, None)
                if chosen is not None:
                    emit_now = chosen
        if cancel_task:
            cancel_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await cancel_task
        if emit_now is not None:
            await self._emit(emit_now)

    async def _emit_after_delay(self, key: Tuple[str, str]) -> None:
        await asyncio.sleep(self._conflict_hold)
        decision: Optional[Decision] = None
        async with self._decision_lock:
            decision = self._decision_buffers.pop(key, None)
            self._decision_tasks.pop(key, None)
        if decision is not None:
            await self._emit(decision)

    def _resolve_conflict(
        self, existing: Decision, incoming: Decision
    ) -> Optional[Decision]:
        if existing.side == incoming.side:
            return existing if existing.score >= incoming.score else incoming
        if abs(existing.score - incoming.score) <= self._conflict_delta:
            if existing.side == "sell":
                return existing
            if incoming.side == "sell":
                return incoming
            return None
        return existing if existing.score > incoming.score else incoming

    def set_rl_weights(self, weights: Mapping[str, float]) -> None:
        """Update reinforcement learning weights applied during voting."""

        self._weights = {
            agent: float(weight)
            for agent, weight in weights.items()
            if weight is not None
        }

    @staticmethod
    def _build_order_id(
        *,
        mint: str,
        side: str,
        notional: float,
        snapshot_hash: str,
        agents: List[str],
        bucket: int,
    ) -> str:
        rounded = round(notional, 2)
        payload = f"{mint}|{side}|{rounded}|{snapshot_hash}|{','.join(agents)}|{bucket}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
