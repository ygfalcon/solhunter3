"""Swarm voting stage."""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Mapping, Tuple

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
        quorum: int = 2,
        min_score: float = 0.04,
        rl_weights: Mapping[str, float] | None = None,
        kv: KeyValueStore | None = None,
        dedupe_ttl: float = 300.0,
    ) -> None:
        self._emit = emit
        self._window_sec = window_ms / 1000.0
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

    async def submit(self, suggestion: TradeSuggestion) -> None:
        key = (suggestion.mint, suggestion.side, suggestion.inputs_hash)
        async with self._locks[key]:
            self._pending[key].append(suggestion)
            if key not in self._timers:
                self._timers[key] = asyncio.create_task(self._finalise_later(key))

    async def _finalise_later(self, key: Tuple[str, str, str]) -> None:
        await asyncio.sleep(self._window_sec)
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
        if self._kv:
            stored = await self._kv.set_if_absent(
                vote_dedupe_key(order_id),
                "1",
                ttl=self._dedupe_ttl,
            )
            if not stored:
                return
        await self._emit(decision)

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
