"""Swarm voting stage with exit-aware bias."""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Mapping, Tuple

from .contracts import vote_dedupe_key
from .kv import KeyValueStore
from .types import Decision, TradeSuggestion
from .utils import now_ts

CONFLICT_DELTA = 0.05


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
        conflict_delta: float = CONFLICT_DELTA,
    ) -> None:
        self._emit = emit
        window_sec = window_ms / 1000.0
        self._window_sec = min(max(window_sec, 0.3), 0.5)
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
        self._exit_bias: Dict[Tuple[str, str], float] = {}
        self.conflict_delta = float(conflict_delta)
        self._recent_scores: Dict[Tuple[str, str], Dict[str, float]] = {}

    @property
    def window_sec(self) -> float:
        return self._window_sec

    async def submit(self, suggestion: TradeSuggestion) -> None:
        key = (suggestion.mint, suggestion.side, suggestion.inputs_hash)
        immediate = suggestion.side == "sell" and suggestion.must_exit
        async with self._locks[key]:
            self._pending[key].append(suggestion)
            if suggestion.side == "sell" and suggestion.must_exit:
                expiry = now_ts() + max(self._window_sec * 2.0, 0.25)
                self._exit_bias[(suggestion.mint, suggestion.inputs_hash)] = expiry
            if immediate:
                timer = self._timers.pop(key, None)
                if timer:
                    timer.cancel()
            elif key not in self._timers:
                self._timers[key] = asyncio.create_task(
                    self._finalise_later(key, self._window_sec)
                )
        if immediate:
            await self._process_key(key)

    async def _finalise_later(self, key: Tuple[str, str, str], delay: float) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:  # pragma: no cover - cancelled timers
            return
        await self._process_key(key)

    async def _process_key(self, key: Tuple[str, str, str]) -> None:
        async with self._locks[key]:
            suggestions = self._pending.pop(key, [])
            self._timers.pop(key, None)
        if not suggestions:
            return
        await self._finalise_suggestions(key, suggestions)

    async def _finalise_suggestions(
        self, key: Tuple[str, str, str], suggestions: List[TradeSuggestion]
    ) -> None:
        now = now_ts()
        self._prune_exit_bias(now)
        self._prune_recent_scores(now)
        if key[1] == "buy":
            bias = self._exit_bias.get((key[0], key[2]))
            if bias and bias > now:
                return
        valid = [s for s in suggestions if now <= s.generated_at + s.ttl_sec]
        weighted = [
            (s, max(self._weights.get(s.agent, 1.0), 0.0)) for s in valid
        ]
        weighted = [(s, w) for s, w in weighted if w > 0]
        exit_priority = key[1] == "sell" and any(s.must_exit for s, _ in weighted)
        effective_quorum = 1 if exit_priority else self._quorum
        if len(weighted) < effective_quorum:
            return
        total_weight = sum(weight for _, weight in weighted)
        if total_weight <= 0:
            return
        total_conf = sum(max(s.confidence, 0.0) * weight for s, weight in weighted)
        score = total_conf / total_weight
        effective_min_score = 0.0 if exit_priority else self._min_score
        if score < effective_min_score:
            return
        if self._should_block_buy(key, score, exit_priority):
            return
        weighted_notional = sum(s.notional_usd * weight for s, weight in weighted)
        notional = weighted_notional / total_weight
        agents = sorted({s.agent for s, _ in weighted})
        time_bucket = int(
            min(s.generated_at for s, _ in weighted) / max(self._window_sec, 1e-6)
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
        if exit_priority:
            expiry = max(
                self._exit_bias.get((key[0], key[2]), 0.0),
                now + max(self._window_sec * 2.0, 0.25),
            )
            self._exit_bias[(key[0], key[2])] = expiry
        slot = self._recent_scores.setdefault((key[0], key[2]), {"ts": now})
        slot[key[1]] = score
        slot["ts"] = now
        await self._emit(decision)

    def _should_block_buy(
        self,
        key: Tuple[str, str, str],
        score: float,
        exit_priority: bool,
    ) -> bool:
        if key[1] != "buy" or exit_priority:
            return False
        entry = self._recent_scores.get((key[0], key[2]))
        if not entry:
            return False
        sell_score = entry.get("sell")
        if sell_score is None:
            return False
        return abs(sell_score - score) <= self.conflict_delta

    def _prune_exit_bias(self, now: float) -> None:
        expired = [key for key, expiry in self._exit_bias.items() if expiry <= now]
        for key in expired:
            self._exit_bias.pop(key, None)

    def _prune_recent_scores(self, now: float) -> None:
        ttl = max(self._window_sec * 4.0, 1.0)
        expired = [key for key, entry in self._recent_scores.items() if now - entry.get("ts", 0.0) > ttl]
        for key in expired:
            self._recent_scores.pop(key, None)

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


__all__ = ["VotingStage", "CONFLICT_DELTA"]
