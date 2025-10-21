from tests.golden_pipeline.conftest import STREAMS


def test_golden_materialization_hash_stability(golden_harness, fake_broker):
    snapshots = golden_harness.golden_snapshots
    assert len(snapshots) == 3

    initial = snapshots[1]
    mutated = snapshots[1]
    bumped = snapshots[-1]

    assert initial.hash == mutated.hash
    assert bumped.hash != initial.hash

    golden_events = fake_broker.events[STREAMS.golden_snapshot]
    assert len(golden_events) == len(snapshots)

    inspector_counts = [event["hash"] for event in golden_events]
    assert inspector_counts == [snap.hash for snap in snapshots]
