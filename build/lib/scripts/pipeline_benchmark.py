#!/usr/bin/env python3
"""Quick benchmark harness for the new pipeline."""
from __future__ import annotations

import asyncio
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List

from solhunter_zero.pipeline import PipelineCoordinator


@dataclass
class DummyContext:
    actions: List[Dict[str, Any]]


class DummyExecutor:
    async def execute(self, action: Dict[str, Any]) -> Dict[str, Any]:
        await asyncio.sleep(0.01)
        return {"txid": f"{action['token']}-{time.time():.0f}"}


class DummyAgentManager:
    def __init__(self) -> None:
        self.executor = DummyExecutor()
        self.memory_agent = None

    async def evaluate_with_swarm(self, token: str, portfolio: Any) -> DummyContext:
        await asyncio.sleep(0.01)
        if random.random() < 0.2:
            return DummyContext([])
        action = {
            "token": token,
            "side": "buy" if random.random() < 0.5 else "sell",
            "amount": random.uniform(1, 5),
            "price": random.uniform(1, 10),
            "agent": "benchmark_agent",
        }
        return DummyContext([action])


class DummyPortfolio:
    def __init__(self) -> None:
        self.balances = {}
        self.price_history = {}
        self.risk_metrics = {}


async def _main(duration: float = 5.0) -> None:
    agent_manager = DummyAgentManager()
    portfolio = DummyPortfolio()
    coordinator = PipelineCoordinator(
        agent_manager,
        portfolio,
        discovery_interval=0.5,
        discovery_cache_ttl=1.0,
        scoring_batch=16,
        evaluation_cache_ttl=1.0,
        evaluation_workers=8,
        execution_lanes=4,
    )
    start_ts = time.time()
    await coordinator.start()
    try:
        while time.time() - start_ts < duration:
            await asyncio.sleep(0.5)
    finally:
        await coordinator.stop()
    samples = await coordinator.snapshot_telemetry()
    print(f"Processed {len(samples)} telemetry events in {time.time() - start_ts:.2f}s")
    if samples:
        print(samples[-5:])


def main() -> None:
    duration = float(os.getenv("PIPELINE_BENCH_DURATION", "5") or 5.0)
    asyncio.run(_main(duration=duration))


if __name__ == "__main__":
    main()
