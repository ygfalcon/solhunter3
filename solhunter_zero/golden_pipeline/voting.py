"""Swarm voting stage."""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Tuple

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
    ) -> None:
        self._emit = emit
        self._window_sec = window_ms / 1000.0
        self._quorum = quorum
        self._min_score = min_score
        self._pending: Dict[Tuple[str, str, str], List[TradeSuggestion]] = defaultdict(list)
        self._locks: Dict[Tuple[str, str, str], asyncio.Lock] = defaultdict(asyncio.Lock)
        self._timers: Dict[Tuple[str, str, str], asyncio.Task] = {}

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
        if len(valid) < self._quorum:
            return
        confidences = [s.confidence for s in valid]
        total_conf = sum(confidences)
        if total_conf <= 0:
            return
        score = total_conf / len(valid)
        if score < self._min_score:
            return
        notional = sum(s.notional_usd for s in valid) / len(valid)
        agents = sorted({s.agent for s in valid})
        time_bucket = int(min(s.generated_at for s in valid) / self._window_sec)
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
        await self._emit(decision)

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
