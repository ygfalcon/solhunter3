"""Agent execution stage for Golden Snapshots."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Sequence, Set

from .types import GoldenSnapshot, TradeSuggestion
from .utils import TTLCache, now_ts

log = logging.getLogger(__name__)


class BaseAgent:
    """Base class for agents that consume Golden Snapshots."""

    name: str

    def __init__(self, name: str) -> None:
        self.name = name

    async def generate(self, snapshot: GoldenSnapshot) -> Sequence[TradeSuggestion]:
        return []

    def build_suggestion(
        self,
        *,
        snapshot: GoldenSnapshot,
        side: str,
        notional_usd: float,
        max_slippage_bps: float,
        risk: dict,
        confidence: float,
        ttl_sec: float,
        gating: Dict[str, Any] | None = None,
        slices: Sequence[Dict[str, Any]] | None = None,
        must_exit: bool = False,
        hot_watch: bool = False,
        diagnostics: Dict[str, Any] | None = None,
    ) -> TradeSuggestion:
        generated_at = snapshot.asof if snapshot.asof else now_ts()
        return TradeSuggestion(
            agent=self.name,
            mint=snapshot.mint,
            side=side,
            notional_usd=notional_usd,
            max_slippage_bps=max_slippage_bps,
            risk=risk,
            confidence=confidence,
            inputs_hash=snapshot.hash,
            ttl_sec=ttl_sec,
            generated_at=generated_at,
            gating=dict(gating or {}),
            slices=list(slices or []),
            must_exit=must_exit,
            hot_watch=hot_watch,
            exit_diagnostics=dict(diagnostics or {}),
        )


class AgentStage:
    """Evaluates registered agents against Golden Snapshots."""

    def __init__(
        self,
        emit: Callable[[TradeSuggestion], Awaitable[None]],
        *,
        agents: Iterable[BaseAgent] | None = None,
        max_spread_bps: float = 80.0,
        min_depth1_pct_usd: float = 8_000.0,
        blacklist: Iterable[str] | None = None,
        cooldown_sec: float = 0.0,
    ) -> None:
        self._emit = emit
        self._agents: List[BaseAgent] = list(agents or [])
        self._max_spread = max_spread_bps
        self._min_depth = min_depth1_pct_usd
        self._lock = asyncio.Lock()
        self._blacklist: Set[str] = {str(m).lower() for m in (blacklist or [])}
        self._cooldown = TTLCache()
        self._cooldown_sec = max(0.0, cooldown_sec)

    def register_agent(self, agent: BaseAgent) -> None:
        self._agents.append(agent)

    async def submit(self, snapshot: GoldenSnapshot) -> None:
        if snapshot.px.get("spread_bps", 0.0) > self._max_spread:
            return
        depth1 = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0))
        if depth1 < self._min_depth:
            return
        mint_key = snapshot.mint.lower()
        if mint_key in self._blacklist:
            return
        if self._cooldown_sec > 0 and not self._cooldown.add(mint_key, self._cooldown_sec):
            return
        async with self._lock:
            for agent in self._agents:
                try:
                    suggestions = await agent.generate(snapshot)
                except Exception:  # pragma: no cover - defensive
                    log.exception("Agent %s failed", agent.name)
                    continue
                for suggestion in suggestions or []:
                    if suggestion.inputs_hash != snapshot.hash:
                        log.warning(
                            "Agent %s emitted suggestion with mismatched hash %s != %s",
                            agent.name,
                            suggestion.inputs_hash,
                            snapshot.hash,
                        )
                        continue
                    await self._emit(suggestion)
