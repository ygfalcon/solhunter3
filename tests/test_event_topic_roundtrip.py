from __future__ import annotations

import ast
import pathlib
from typing import Any, Dict

import pytest

from solhunter_zero import event_bus
from solhunter_zero.event_bus import _decode_payload, _encode_event, _maybe_decompress, pb


def _has_real_protobuf() -> bool:
    try:
        event = pb.Event()
    except Exception:  # pragma: no cover - stub without callable constructor
        return False
    return hasattr(event, "WhichOneof")


HAS_PROTOBUF = _has_real_protobuf()

SAMPLE_PAYLOADS: Dict[str, Any] = {
    "action_decision": {
        "token": "SOL",
        "side": "buy",
        "size": 1.5,
        "price": 42.0,
        "rationale": {
            "agent": "alpha",
            "agents": ["alpha", "beta"],
            "score": 0.9,
            "confidence": 0.85,
            "conviction_delta": 0.1,
            "snapshot_hash": "deadbeef",
        },
    },
    "action_executed": {"action": {"foo": 1}, "result": {"bar": 2}},
    "action_proposal": {
        "token": "SOL",
        "side": "buy",
        "size": 2.0,
        "score": 0.75,
        "agent": "proposal_agent",
        "price": 41.0,
    },
    "decision_metrics": {
        "window_sec": 60.0,
        "count": 5,
        "decision_rate": 5.0,
        "buys": 3,
        "sells": 2,
        "avg_size": 12.5,
        "ttf_decision": 4.2,
    },
    "decision_summary": {"token": "SOL", "count": 4, "buys": 2, "sells": 2},
    "depth_service_status": {"status": "ok"},
    "dex_latency_update": {"orca": 0.12, "raydium": 0.34},
    "heartbeat": {"service": "runtime"},
    "memory_sync_request": {"last_id": 7},
    "memory_sync_response": {
        "trades": [
            {
                "token": "SOL",
                "direction": "buy",
                "amount": 1.0,
                "price": 10.0,
                "reason": "reason",
                "context": "ctx",
                "emotion": "calm",
                "simulation_id": 1,
                "uuid": "uuid",
                "trade_id": 2,
            }
        ],
        "index": b"abc",
    },
    "metrics.schema_mismatch_total": {
        "stream": "x:market.depth",
        "reader": "test_reader",
        "missing": ["mid_usd"],
        "fallback": [],
        "ts": 1_700_000_000.0,
    },
    "pending_swap": {
        "token": "SOL",
        "address": "addr",
        "size": 1.1,
        "slippage": 0.05,
    },
    "portfolio_updated": {"balances": {"SOL": 10.0, "USDC": 5.0}},
    "price_update": {"venue": "dex", "token": "SOL", "price": 39.5},
    "remote_system_metrics": {"cpu": 12.0, "memory": 50.0},
    "resource_alert": {
        "resource": "cpu",
        "ceiling": 90.0,
        "action": "warn",
        "active": True,
        "breach_samples": 3,
        "grace_samples": 2,
        "last_violation": 1_700_000_000.0,
        "last_event": 1_700_000_001.0,
        "value": 95.0,
        "timestamp": 1_700_000_002.0,
    },
    "resource_update": {
        "cpu": 42.0,
        "proc_cpu": 12.5,
        "memory": 68.0,
        "budgets": {
            "cpu": {
                "resource": "cpu",
                "ceiling": 90.0,
                "action": "warn",
                "active": False,
                "breach_samples": 0,
                "grace_samples": 3,
                "last_violation": None,
                "last_event": None,
                "value": 42.0,
            }
        },
        "timestamp": 1_700_000_003.0,
    },
    "resource_alert": {
        "resource": "cpu",
        "ceiling": 90.0,
        "action": "warn",
        "active": True,
        "breach_samples": 3,
        "grace_samples": 2,
        "last_violation": 1_700_000_000.0,
        "last_event": 1_700_000_001.0,
        "value": 95.0,
        "timestamp": 1_700_000_002.0,
    },
    "resource_update": {
        "cpu": 42.0,
        "proc_cpu": 12.5,
        "memory": 68.0,
        "budgets": {
            "cpu": {
                "resource": "cpu",
                "ceiling": 90.0,
                "action": "warn",
                "active": False,
                "breach_samples": 0,
                "grace_samples": 3,
                "last_violation": None,
                "last_event": None,
                "value": 42.0,
            }
        },
        "timestamp": 1_700_000_003.0,
    },
    "risk_metrics": {
        "covariance": 1.0,
        "portfolio_cvar": 2.0,
        "portfolio_evar": 3.0,
        "correlation": 0.5,
        "cov_matrix": [[1.0, 0.1], [0.1, 1.2]],
        "corr_matrix": [[1.0, 0.2], [0.2, 1.0]],
    },
    "risk_updated": {"multiplier": 1.3},
    "rl_metrics": {"loss": 0.5, "reward": 1.2},
    "rl_weights": {"weights": {"SOL": 0.6}, "risk": {"beta": 0.1}},
    "runtime.log": {
        "stage": "loop",
        "detail": "tick",
        "ts": 123.4,
        "level": "INFO",
        "actions": 3,
    },
    "runtime.stage_changed": {
        "stage": "runtime:ready",
        "ok": True,
        "detail": "done",
        "timestamp": 1_700_000_010.0,
        "elapsed": 5.0,
    },
    "startup_complete": {"startup_duration_ms": 512.0},
    "startup_config_load_duration": 1.2,
    "startup_connectivity_check_duration": 2.3,
    "startup_depth_service_start_duration": 3.4,
    "system_metrics": {"cpu": 55.0, "memory": 66.0},
    "system_metrics_combined": {"cpu": 40.0, "memory": 42.0, "iter_ms": 7.5},
    "token_discovered": [
        {
            "mint": "SOL",
            "source": "rpc_logs",
            "score": 0.82,
            "tx": "abc123",
            "ts": 1_700_000_000.5,
            "tags": ["pump", "dex"],
            "interface": "FungibleToken",
            "discovery": {"method": "rpc_logs", "confidence": 0.82},
            "attributes": {"preconfirm": True, "program": "Tokenkeg"},
        },
        {"mint": "USDC"},
    ],
    "x:discovery.candidates": {
        "mint": "MintExample111111111111111111111111111111",
        "asof": 1_700_000_000.0,
        "source": "seeded",
        "sources": ["seeded"],
        "v": "1.0",
    },
    "x:mint.golden.__meta": {"type": "golden_heartbeat", "ts": 1_700_000_000.0},
    "trade_logged": {
        "token": "SOL",
        "direction": "buy",
        "amount": 2.0,
        "price": 11.0,
        "reason": "rebalance",
        "context": "ctx",
        "emotion": "calm",
        "simulation_id": 4,
        "uuid": "abc",
        "trade_id": 5,
        "created_at": 1700000000.0,
    },
    "virtual_pnl": {
        "order_id": "order1",
        "mint": "SOL",
        "snapshot_hash": "snapshot",
        "realized_usd": 10.5,
        "unrealized_usd": 2.5,
        "ts": 1700000100.0,
    },
    "weights_updated": {"weights": {"SOL": 0.4, "USDC": 0.6}},
}

EXPECTED_DECODED: Dict[str, Any] = {
    "action_decision": {
        "token": "SOL",
        "side": "buy",
        "size": 1.5,
        "price": 42.0,
        "rationale": {
            "agent": "alpha",
            "agents": ["alpha", "beta"],
            "score": 0.9,
            "confidence": 0.85,
            "conviction_delta": 0.1,
            "snapshot_hash": "deadbeef",
        },
    },
    "action_executed": {"action": {"foo": 1}, "result": {"bar": 2}},
    "action_proposal": {
        "token": "SOL",
        "side": "buy",
        "size": 2.0,
        "score": 0.75,
        "agent": "proposal_agent",
        "price": 41.0,
    },
    "decision_metrics": {
        "window_sec": 60.0,
        "count": 5,
        "decision_rate": 5.0,
        "buys": 3,
        "sells": 2,
        "avg_size": 12.5,
        "ttf_decision": 4.2,
    },
    "decision_summary": {"token": "SOL", "count": 4, "buys": 2, "sells": 2},
    "depth_service_status": {"status": "ok"},
    "dex_latency_update": {"orca": 0.12, "raydium": 0.34},
    "heartbeat": {"service": "runtime"},
    "memory_sync_request": {"last_id": 7},
    "memory_sync_response": {
        "trades": [
            {
                "token": "SOL",
                "direction": "buy",
                "amount": 1.0,
                "price": 10.0,
                "reason": "reason",
                "context": "ctx",
                "emotion": "calm",
                "simulation_id": 1,
                "uuid": "uuid",
                "trade_id": 2,
            }
        ],
        "index": b"abc",
    },
    "pending_swap": {
        "token": "SOL",
        "address": "addr",
        "size": 1.1,
        "slippage": 0.05,
    },
    "portfolio_updated": {"balances": {"SOL": 10.0, "USDC": 5.0}},
    "price_update": {"venue": "dex", "token": "SOL", "price": 39.5},
    "remote_system_metrics": {"cpu": 12.0, "memory": 50.0},
    "risk_metrics": {
        "covariance": 1.0,
        "portfolio_cvar": 2.0,
        "portfolio_evar": 3.0,
        "correlation": 0.5,
        "cov_matrix": [[1.0, 0.1], [0.1, 1.2]],
        "corr_matrix": [[1.0, 0.2], [0.2, 1.0]],
    },
    "risk_updated": {"multiplier": 1.3},
    "rl_metrics": {"loss": 0.5, "reward": 1.2},
    "rl_weights": {"weights": {"SOL": 0.6}, "risk": {"beta": 0.1}},
    "runtime.log": {
        "stage": "loop",
        "detail": "tick",
        "ts": 123.4,
        "level": "INFO",
        "actions": 3,
    },
    "runtime.stage_changed": {
        "stage": "runtime:ready",
        "ok": True,
        "detail": "done",
        "timestamp": 1_700_000_010.0,
        "elapsed": 5.0,
    },
    "startup_complete": {"startup_duration_ms": 512.0},
    "startup_config_load_duration": 1.2,
    "startup_connectivity_check_duration": 2.3,
    "startup_depth_service_start_duration": 3.4,
    "system_metrics": {"cpu": 55.0, "memory": 66.0},
    "system_metrics_combined": {"cpu": 40.0, "memory": 42.0, "iter_ms": 7.5},
    "token_discovered": [
        {
            "mint": "SOL",
            "source": "rpc_logs",
            "score": 0.82,
            "tx": "abc123",
            "ts": 1_700_000_000.5,
            "tags": ["pump", "dex"],
            "interface": "FungibleToken",
            "discovery": {"method": "rpc_logs", "confidence": 0.82},
            "attributes": {"preconfirm": True, "program": "Tokenkeg"},
        },
        {"mint": "USDC"},
    ],
    "trade_logged": {
        "token": "SOL",
        "direction": "buy",
        "amount": 2.0,
        "price": 11.0,
        "reason": "rebalance",
        "context": "ctx",
        "emotion": "calm",
        "simulation_id": 4,
        "uuid": "abc",
        "trade_id": 5,
    },
    "virtual_pnl": {
        "order_id": "order1",
        "mint": "SOL",
        "snapshot_hash": "snapshot",
        "realized_usd": 10.5,
        "unrealized_usd": 2.5,
        "ts": 1700000100.0,
    },
    "weights_updated": {"weights": {"SOL": 0.4, "USDC": 0.6}},
}


@pytest.fixture(scope="module")
def published_topics() -> set[str]:
    base = pathlib.Path("solhunter_zero")
    topics: set[str] = set()
    for path in base.rglob("*.py"):
        if "build" in path.parts:
            continue
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not node.args:
                continue
            func = node.func
            is_publish = False
            if isinstance(func, ast.Attribute) and func.attr == "publish":
                is_publish = True
            elif isinstance(func, ast.Name) and func.id == "publish":
                is_publish = True
            if not is_publish:
                continue
            arg = node.args[0]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                topics.add(arg.value)
    return topics


def test_all_publish_topics_have_samples(published_topics: set[str]) -> None:
    missing = sorted(topic for topic in published_topics if topic not in SAMPLE_PAYLOADS)
    assert not missing, f"missing sample payloads for: {missing}"


def _roundtrip_topic(topic: str, payload: Any) -> Any:
    encoded = _encode_event(topic, payload)
    assert isinstance(encoded, (bytes, bytearray)), f"topic {topic} did not serialize to protobuf"
    data = _maybe_decompress(bytes(encoded))
    event = pb.Event()
    event.ParseFromString(data)
    assert event.topic == topic
    decoded = _decode_payload(event)
    reencoded = _encode_event(topic, decoded)
    assert isinstance(reencoded, (bytes, bytearray)), f"topic {topic} failed to re-encode"
    return decoded


@pytest.mark.skipif(not HAS_PROTOBUF, reason="protobuf runtime not available")
def test_topic_roundtrip_decoding(published_topics: set[str]) -> None:
    for topic in sorted(published_topics):
        decoded = _roundtrip_topic(topic, SAMPLE_PAYLOADS[topic])
        assert decoded == EXPECTED_DECODED[topic]
