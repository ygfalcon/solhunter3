"""Circuit breaker helpers for Golden Stream network integrations."""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Mapping


@dataclass(slots=True)
class CircuitState:
    failures: int = 0
    opened_until: float = 0.0


class HostCircuitBreaker:
    """Circuit breaker with consecutive failure threshold and cooldown."""

    def __init__(self, *, threshold: int = 3, cooldown: float = 30.0) -> None:
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        if cooldown <= 0:
            raise ValueError("cooldown must be positive")
        self._threshold = threshold
        self._cooldown = cooldown
        self._states: Dict[str, CircuitState] = {}
        self._locks: Dict[str, asyncio.Lock] = {}

    def _state(self, host: str) -> CircuitState:
        state = self._states.get(host)
        if state is None:
            state = CircuitState()
            self._states[host] = state
            self._locks[host] = asyncio.Lock()
        return state

    async def record_success(self, host: str) -> None:
        state = self._state(host)
        async with self._locks[host]:
            state.failures = 0
            if state.opened_until and state.opened_until <= time.monotonic():
                state.opened_until = 0.0

    async def record_failure(self, host: str) -> None:
        state = self._state(host)
        async with self._locks[host]:
            now = time.monotonic()
            if state.opened_until and state.opened_until > now:
                return
            state.failures += 1
            if state.failures >= self._threshold:
                state.opened_until = now + self._cooldown
                state.failures = 0

    async def is_open(self, host: str) -> bool:
        state = self._state(host)
        async with self._locks[host]:
            if state.opened_until and state.opened_until > time.monotonic():
                return True
            state.opened_until = 0.0
            return False

    async def snapshot(self) -> Dict[str, Dict[str, float]]:
        info: Dict[str, Dict[str, float]] = {}
        for host, state in self._states.items():
            async with self._locks[host]:
                remaining = max(0.0, state.opened_until - time.monotonic())
                info[host] = {
                    "failures": float(state.failures),
                    "opened_until": state.opened_until,
                    "cooldown_remaining": remaining,
                }
        return info

    async def restore(self, snapshot: Mapping[str, Mapping[str, float]]) -> None:
        for host, payload in snapshot.items():
            state = self._state(host)
            async with self._locks[host]:
                failures = payload.get("failures") if isinstance(payload, Mapping) else None
                remaining = payload.get("cooldown_remaining") if isinstance(payload, Mapping) else None
                try:
                    state.failures = int(float(failures)) if failures is not None else 0
                except (TypeError, ValueError):
                    state.failures = 0
                try:
                    remaining_float = float(remaining) if remaining is not None else 0.0
                except (TypeError, ValueError):
                    remaining_float = 0.0
                if remaining_float > 0:
                    state.opened_until = time.monotonic() + remaining_float
                else:
                    state.opened_until = 0.0


