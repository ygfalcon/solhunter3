import json

from .conftest import run_golden_harness
from solhunter_zero.golden_pipeline.contracts import STREAMS


def _serialise_run() -> tuple[tuple[bytes, ...], tuple[bytes, ...]]:
    with run_golden_harness() as harness:
        hash_bytes = tuple(
            snapshot.hash.encode("utf-8") for snapshot in harness.golden_snapshots
        )
        vote_events = harness.bus.events.get(STREAMS.vote_decisions, [])
        vote_bytes = tuple(
            json.dumps(event, sort_keys=True, separators=(",", ":")).encode("utf-8")
            for event in vote_events
        )
    return hash_bytes, vote_bytes


def test_golden_pipeline_is_deterministic_across_runs() -> None:
    first_hashes, first_votes = _serialise_run()
    second_hashes, second_votes = _serialise_run()

    assert first_hashes, "expected at least one golden snapshot hash to be produced"
    assert first_votes, "expected at least one vote output to be produced"

    assert (
        first_hashes == second_hashes
    ), "golden snapshot hashes diverged between identical seeded runs"
    assert first_votes == second_votes, "vote outputs diverged between identical seeded runs"
