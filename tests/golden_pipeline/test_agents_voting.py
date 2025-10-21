from collections import defaultdict

from tests.golden_pipeline.conftest import approx


def test_agents_generate_suggestions_and_vote(golden_harness):
    snapshots = {snap.hash: snap for snap in golden_harness.golden_snapshots}
    suggestions_by_hash = defaultdict(list)
    for suggestion in golden_harness.trade_suggestions:
        assert suggestion.inputs_hash in snapshots
        assert suggestion.side == "buy"
        suggestions_by_hash[suggestion.inputs_hash].append(suggestion)

    assert len(suggestions_by_hash) == len(snapshots)
    for bucket in suggestions_by_hash.values():
        assert len(bucket) == 2

    assert len(golden_harness.decisions) == len(suggestions_by_hash)
    for decision in golden_harness.decisions:
        group = suggestions_by_hash[decision.snapshot_hash]
        expected_notional = sum(item.notional_usd for item in group) / len(group)
        expected_score = sum(item.confidence for item in group) / len(group)
        assert decision.notional_usd == approx(expected_notional)
        assert decision.score == approx(expected_score)
        assert decision.agents == sorted(item.agent for item in group)
