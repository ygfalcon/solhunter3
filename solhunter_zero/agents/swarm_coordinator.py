from __future__ import annotations

"""Swarm coordinator that aggregates action proposals into decisions.

This minimal version windows proposals briefly and emits a simple decision.
Enabled when EVENT_DRIVEN=1. It can be extended to use RL weights and
confidence scoring from the existing RL daemon and memory system.
"""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List

from .. import event_bus


@dataclass
class _Prop:
    token: str
    side: str
    size: float
    score: float
    agent: str
    ts: float


class SwarmCoordinator:
    def __init__(self, window_ms: int = 250) -> None:
        self.window_ms = window_ms
        self._props: List[_Prop] = []
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        event_bus.subscribe("action_proposal", self._on_proposal)
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._tick())

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    def _on_proposal(self, payload: Dict[str, Any]) -> None:
        try:
            self._props.append(
                _Prop(
                    token=str(payload.get("token")),
                    side=str(payload.get("side", "hold")),
                    size=float(payload.get("size", 0.0)),
                    score=float(payload.get("score", 0.0)),
                    agent=str(payload.get("agent", "?")),
                    ts=time.time(),
                )
            )
        except Exception:
            pass

    async def _tick(self) -> None:
        while True:
            await asyncio.sleep(self.window_ms / 1000.0)
            if not self._props:
                continue
            now = time.time()
            cut = now - (self.window_ms / 1000.0)
            # collect windowed proposals
            window = [p for p in self._props if p.ts >= cut]
            self._props = [p for p in self._props if p.ts >= cut]
            if not window:
                continue
            by_token: Dict[str, List[_Prop]] = defaultdict(list)
            for p in window:
                by_token[p.token].append(p)
            for tok, ps in by_token.items():
                # naive: choose the highest score non-hold proposal; else hold
                best = max(ps, key=lambda x: x.score)
                side = best.side if best.side in {"buy", "sell"} else "hold"
                decision = {
                    "token": tok,
                    "side": side,
                    "size": best.size,
                    "rationale": {"agent": best.agent, "score": best.score},
                }
                try:
                    event_bus.publish("action_decision", decision)
                except Exception:
                    pass

