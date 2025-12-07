import asyncio
import dataclasses

import pytest

from solhunter_zero.golden_pipeline.contracts import (
    STREAMS,
    vote_dedupe_key,
    vote_input_key,
)

from .conftest import run_golden_harness


@pytest.mark.anyio
async def test_vote_manager_drops_replayed_discovery_events() -> None:
    """Flood the vote manager with duplicate discovery outputs and enforce idempotency."""

    with run_golden_harness() as harness:
        pipeline = harness.pipeline
        stage = pipeline._voting_stage  # type: ignore[attr-defined]
        kv = pipeline._kv  # type: ignore[attr-defined]

        baseline_decisions = list(harness.decisions)
        baseline_vote_events = list(harness.bus.events[STREAMS.vote_decisions])
        baseline_metrics = pipeline.metrics_snapshot()
        baseline_vote_kv = set(await kv.scan_prefix("vote:"))

        now = harness.clock.time()

        async def replay(suggestion):
            duplicate = dataclasses.replace(
                suggestion,
                generated_at=now,
                ttl_sec=max(suggestion.ttl_sec, stage.window_sec * 2.0),
            )
            await stage.submit(duplicate)

        await asyncio.gather(
            *(
                replay(suggestion)
                for suggestion in harness.trade_suggestions
                for _ in range(3)
            )
        )

        await asyncio.sleep(stage.window_sec + 0.05)

        assert harness.decisions == baseline_decisions
        assert harness.bus.events[STREAMS.vote_decisions] == baseline_vote_events

        vote_entries = set(await kv.scan_prefix("vote:"))
        assert vote_entries == baseline_vote_kv

        input_keys = {
            vote_input_key(item.mint, item.side, item.inputs_hash)
            for item in harness.trade_suggestions
        }
        dedupe_keys = {vote_dedupe_key(decision.client_order_id) for decision in baseline_decisions}

        stored_vote_keys = {key for key, _ in vote_entries}
        assert input_keys.issubset(stored_vote_keys)
        assert dedupe_keys.issubset(stored_vote_keys)

        assert pipeline.metrics_snapshot() == baseline_metrics
