import random
from typing import Any, Dict

pytest_plugins = ["tests.golden_pipeline.synth_seed"]

from solhunter_zero import event_bus

_MINT = "MintSeqGuard1111111111111111111111111111111"


def _suggestion_payload(sequence: int) -> Dict[str, Any]:
    return {
        "agent": "tester",
        "mint": _MINT,
        "side": "buy",
        "notional_usd": 125.0,
        "max_slippage_bps": 40.0,
        "risk": {"expected_edge_bps": 42.0},
        "confidence": 0.2,
        "inputs_hash": "snapshot-hash",
        "ttl_sec": 30.0,
        "generated_at": 1_700_000_000.0 + sequence,
        "gating": {},
        "slices": [],
        "must_exit": False,
        "hot_watch": False,
        "exit_diagnostics": {},
        "sequence": sequence,
    }


def _decision_payload(sequence: int) -> Dict[str, Any]:
    return {
        "mint": _MINT,
        "side": "buy",
        "notional_usd": 250.0,
        "score": 0.4,
        "snapshot_hash": "snapshot-hash",
        "client_order_id": f"order-{sequence}",
        "agents": ["tester"],
        "ts": 1_700_000_500.0 + sequence,
        "sequence": sequence,
    }


def test_ui_sequence_guard(runtime) -> None:
    events = [
        ("x:trade.suggested", _suggestion_payload(100)),
        ("x:vote.decisions", _decision_payload(102)),
        ("x:vote.decisions", _decision_payload(103)),
        ("x:trade.suggested", _suggestion_payload(101)),
        ("x:vote.decisions", _decision_payload(101)),
        ("x:trade.suggested", _suggestion_payload(104)),
    ]

    head, *tail = events
    random.Random(1337).shuffle(tail)
    ordered = [head, *tail]

    for topic, payload in ordered:
        dedupe = f"{topic}:{_MINT}:{payload['sequence']}"
        event_bus.publish(topic, dict(payload), dedupe_key=dedupe, _broadcast=False)

    collector = runtime.wiring.collectors
    with collector._swarm_lock:
        suggestion_seqs = [entry.get("sequence") for entry in list(collector._agent_suggestions)]
        decision_seqs = [entry.get("sequence") for entry in list(collector._vote_decisions)]
        last_sequence = collector._mint_sequences.get(_MINT)

    assert suggestion_seqs[:2] == [104, 100]
    assert 101 not in suggestion_seqs
    assert len(suggestion_seqs) == 2

    assert decision_seqs, "no decisions captured"
    assert decision_seqs[0] == 103
    if len(decision_seqs) > 1:
        assert decision_seqs[1] == 102
    assert decision_seqs == sorted(decision_seqs, reverse=True)
    assert 101 not in decision_seqs
    assert len(decision_seqs) <= 2

    assert last_sequence == 104
