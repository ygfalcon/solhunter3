from tests.golden_pipeline.conftest import STREAMS


def test_resilience_and_idempotency(golden_harness, fake_broker):
    summary = golden_harness.summary()

    assert summary["discoveries_by_source"] == {"das": 2}
    assert all(count == 1 for count in summary["snapshots_by_hash"].values())
    assert summary["suggestions_by_agent"] == {"momentum_v1": 3, "meanrev_v1": 3}
    assert summary["decisions_by_side"] == {"buy": 3}

    decision_ids = {decision.client_order_id for decision in golden_harness.decisions}
    assert len(decision_ids) == len(golden_harness.decisions)

    golden_events = fake_broker.events[STREAMS.golden_snapshot]
    assert len(golden_events) == len(golden_harness.golden_snapshots)

    trade_events = fake_broker.events[STREAMS.trade_suggested]
    assert len(trade_events) == len(golden_harness.trade_suggestions)
