from tests.golden_pipeline.conftest import BASE58_MINTS, STREAMS


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


def test_discovery_breaker_metrics_and_recovery(
    golden_harness, fake_broker, das_failure_stub
):
    states = golden_harness.discovery_breaker_states
    assert states, "breaker states were not recorded"
    opened = [state for state in states if state.get("open")]
    assert opened, "breaker never opened"
    assert opened[0]["failure_count"] >= opened[0]["threshold"]

    journal = golden_harness.discovery_breaker_journal
    submission = next(entry for entry in journal if entry["event"] == "submission_while_open")
    assert submission["accepted"] is False
    recovered = next(entry for entry in journal if entry["event"] == "recovered_submission")
    assert recovered["accepted"] is True
    assert recovered["state"]["open"] is False

    metrics = fake_broker.events["metrics.discovery.breaker"]
    assert metrics, "no discovery metrics published"
    assert any(event["event"] == "breaker_open" and event["open"] for event in metrics)
    final_metric = metrics[-1]
    assert final_metric["event"].startswith("recovered")
    assert final_metric["open"] is False
    assert final_metric["failure_count"] == 0

    failure_events = [entry for entry in das_failure_stub.events if entry["event"] != "success"]
    assert failure_events and failure_events[0]["event"] == "timeout"
    assert das_failure_stub.events[-1]["event"] == "success"
    assert das_failure_stub.events[-1]["state"]["open"] is False


def test_price_provider_fallback_and_metrics(
    golden_harness, fake_broker, price_provider_stub
):
    events = price_provider_stub.events
    assert golden_harness.price_quotes, "price failure simulation did not execute"
    assert len(events) >= 8, "expected multiple provider events"

    first = events[0:3]
    assert [entry["provider"] for entry in first] == ["birdeye", "jupiter", "dexscreener"]
    assert first[0]["outcome"] == "timeout"
    assert first[1]["outcome"] == "disconnect"
    assert first[2]["outcome"] == "success"

    second = events[3:5]
    assert [entry["provider"] for entry in second] == ["birdeye", "jupiter"]
    assert second[0]["outcome"] == "http"
    assert second[1]["outcome"] == "success"

    third = events[5:7]
    assert [entry["provider"] for entry in third] == ["birdeye", "jupiter"]
    assert third[0]["outcome"] == "disconnect"
    assert third[1]["outcome"] == "success"

    final_request = events[7:]
    assert final_request[0]["provider"] == "birdeye"
    assert final_request[0]["outcome"] == "success"

    health_snapshots = golden_harness.price_health_snapshots
    assert health_snapshots, "no provider health snapshots recorded"
    assert health_snapshots[0]["birdeye"]["consecutive_failures"] >= 1
    assert not health_snapshots[0]["birdeye"]["healthy"]
    assert health_snapshots[-1]["birdeye"]["healthy"] is True
    assert health_snapshots[-1]["birdeye"]["consecutive_failures"] == 0

    metrics = fake_broker.events["metrics.prices.providers"]
    assert metrics, "no price provider metrics emitted"
    assert any(
        not provider["healthy"]
        for provider in metrics[0]["providers"].values()
        if isinstance(provider, dict)
    )
    assert metrics[-1]["providers"]["birdeye"]["healthy"] is True

    sources = golden_harness.price_quote_sources
    probe_mint = golden_harness.price_probe_token
    assert sources, "price quote sources missing"
    assert sources[0][probe_mint] == "dexscreener"
    assert sources[-1][probe_mint] == "birdeye"
