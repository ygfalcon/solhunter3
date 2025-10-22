from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, List

import pytest

pytest_plugins = ["tests.golden_pipeline.fixtures_demo"]

from tools.demo_payloads import (
    ARTIFACT_DIR,
    REPORT_JSON_PATH,
    REPORT_MARKDOWN_PATH,
    summarise,
    write_jsonl,
    write_summary_json,
    write_summary_markdown,
)
from .fixtures_demo import STREAM_DISCOVERY, STREAM_GOLDEN, STREAM_SUGGESTED


def _depth_from_event(event: Dict[str, Any]) -> float:
    depth = event.get("liq_depth_1pct_usd")
    if depth is not None:
        try:
            return float(depth)
        except (TypeError, ValueError):
            return 0.0
    liq = event.get("liq")
    if isinstance(liq, dict):
        depth_map = liq.get("depth_usd_by_pct") or liq.get("depth_pct")
        if isinstance(depth_map, dict):
            for key in ("1", "1.0"):
                if key in depth_map:
                    try:
                        return float(depth_map[key])
                    except (TypeError, ValueError):
                        continue
    return 0.0


def test_golden_demo(demo_context, demo_tokens, caplog):
    caplog.set_level("WARNING")
    websockets = pytest.importorskip("websockets")

    demo_context.mark_runtime_ready()

    ws_url = (
        os.getenv("UI_WS_URL")
        or os.getenv("UI_EVENTS_WS_URL")
        or os.getenv("UI_EVENTS_WS")
    )
    assert ws_url, "UI websocket URL not configured"

    async def _handshake(ws_mod):
        async with ws_mod.connect(ws_url) as conn:
            meta_raw = await asyncio.wait_for(conn.recv(), timeout=5.0)
            meta = json.loads(meta_raw)
            await conn.send(json.dumps({"event": "hello", "client": "golden-demo", "version": 3}))
            hello_frame: Dict[str, Any] | None = None
            loop = asyncio.get_event_loop()
            deadline = loop.time() + 2.0
            while loop.time() < deadline and hello_frame is None:
                try:
                    raw = await asyncio.wait_for(conn.recv(), timeout=1.0)
                except asyncio.TimeoutError:
                    break
                try:
                    obj = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(obj, dict) and (obj.get("event") == "hello" or obj.get("type") == "hello"):
                    hello_frame = obj
                    break
            return meta, hello_frame

    handshake, hello_frame = asyncio.run(_handshake(websockets))

    assert handshake.get("type") == "UI_META"
    broker = handshake.get("broker", {})
    assert broker.get("channel") == os.getenv("BROKER_CHANNEL")
    assert broker.get("kind")
    bus_info = handshake.get("event_bus", {})
    assert bus_info.get("url_ws") == os.getenv("EVENT_BUS_URL")
    pipeline_info = handshake.get("pipeline", {})
    stages = pipeline_info.get("stages", {})
    discovery_topics = stages.get("discovery", {}).get("topics", [])
    golden_topics = stages.get("golden", {}).get("topics", [])
    agent_topics = stages.get("agents", {}).get("topics", [])
    assert STREAM_DISCOVERY in discovery_topics
    assert STREAM_GOLDEN in golden_topics
    assert STREAM_SUGGESTED in agent_topics
    lag_metrics = handshake.get("lag", {})
    for key in ("bus_ms", "depth_ms", "golden_ms"):
        assert key in lag_metrics
    assert hello_frame is not None, "expected hello acknowledgement from UI websocket"

    discovered_path = ARTIFACT_DIR / f"{STREAM_DISCOVERY.replace(':', '_')}.jsonl"
    golden_path = ARTIFACT_DIR / f"{STREAM_GOLDEN.replace(':', '_')}.jsonl"
    suggestions_path = ARTIFACT_DIR / f"{STREAM_SUGGESTED.replace(':', '_')}.jsonl"
    report_json_path = REPORT_JSON_PATH
    summary_path = REPORT_MARKDOWN_PATH

    discovered_events: List[Dict[str, Any]] = []
    golden_events: List[Dict[str, Any]] = []
    suggestion_events: List[Dict[str, Any]] = []
    summary: Dict[str, Any] | None = None

    golden_hashes: set[str | None] = set()
    status_snapshot: Dict[str, Any] = {}
    bus_lag: float | None = None
    depth_lag: float | None = None
    try:
        demo_context.feed_tokens()
        demo_context.loop.run_until_complete(asyncio.sleep(0.1))

        discovered_events = demo_context.loop.run_until_complete(
            demo_context.recorder.wait_for(STREAM_DISCOVERY, len(demo_tokens), timeout=5.0)
        )
        golden_events = demo_context.loop.run_until_complete(
            demo_context.recorder.wait_for(STREAM_GOLDEN, 15, timeout=5.0)
        )
        golden_hashes = {entry.get("hash") for entry in golden_events}
        suggestion_events = demo_context.loop.run_until_complete(
            demo_context.recorder.wait_for(STREAM_SUGGESTED, 6, timeout=5.0)
        )

        summary = summarise(
            discovered=discovered_events,
            golden=golden_events,
            suggestions=suggestion_events,
        )
    finally:
        summary_payload = summary or summarise(
            discovered=discovered_events,
            golden=golden_events,
            suggestions=suggestion_events,
        )
        handshake_version = handshake.get("v")
        ack_version = None
        if isinstance(hello_frame, dict):
            ack_version = hello_frame.get("v") or hello_frame.get("version")
        summary_payload["handshake"] = {
            "schema_version": handshake_version,
            "ack_version": ack_version,
            "event_bus": bus_info.get("url_ws"),
            "broker_channel": broker.get("channel"),
            "version": handshake.get("version"),
        }
        write_jsonl(discovered_path, discovered_events)
        write_jsonl(golden_path, golden_events)
        write_jsonl(suggestions_path, suggestion_events)
        from solhunter_zero import token_scanner

        das_until = float(getattr(token_scanner, "_DAS_CIRCUIT_OPEN_UNTIL", 0.0) or 0.0)
        das_circuit_closed = das_until <= time.monotonic()

        status_snapshot = demo_context.wiring.collectors.status_snapshot()
        bus_lag_raw = status_snapshot.get("bus_latency_ms")
        depth_lag_raw = status_snapshot.get("depth_lag_ms")
        try:
            bus_lag = float(bus_lag_raw) if bus_lag_raw is not None else None
        except Exception:
            bus_lag = None
        try:
            depth_lag = float(depth_lag_raw) if depth_lag_raw is not None else None
        except Exception:
            depth_lag = None

        checks = {
            "discovery_minimum_met": len(discovered_events) >= 20,
            "golden_minimum_met": len(golden_events) >= 15,
            "agent_minimum_met": len(suggestion_events) >= 6,
            "integrity_hashes_present": all(
                (entry.get("integrity", {}).get("golden_hash") in golden_hashes)
                for entry in suggestion_events
            ),
            "ui_handshake_version_match": bool(handshake_version)
            and (ack_version is None or str(ack_version) == str(handshake_version)),
            "lag_within_demo_threshold": (
                (bus_lag is None or bus_lag < 1000.0)
                and (depth_lag is None or depth_lag < 2500.0)
            ),
            "das_circuit_closed": das_circuit_closed,
        }
        summary_payload["checks"] = checks
        summary_payload["status"] = "PASS" if all(checks.values()) else "FAIL"
        summary_payload["metrics"] = {
            "bus_latency_ms": bus_lag,
            "depth_latency_ms": depth_lag,
            "golden_lag_ms": lag_metrics.get("golden_ms"),
            "golden_count": len(golden_events),
            "suggestion_count": len(suggestion_events),
        }
        summary_payload["artifacts"] = {
            "frames": str(ARTIFACT_DIR.resolve()),
            "report_json": str(report_json_path.resolve()),
            "report_markdown": str(summary_path.resolve()),
        }
        summary_payload["streams"] = {
            "discovered": STREAM_DISCOVERY,
            "golden": STREAM_GOLDEN,
            "suggested": STREAM_SUGGESTED,
        }
        write_summary_json(report_json_path, summary_payload)
        write_summary_markdown(summary_path, summary_payload)

    assert len(discovered_events) >= 20
    for entry in discovered_events:
        source = entry.get("source")
        assert isinstance(source, dict)
        assert source.get("kind") == "synthetic:demo"
        assert int(entry.get("schema_version", 0)) >= 3

    assert len(golden_events) >= 15, f"too few golden snapshots: {len(golden_events)}"
    hash_by_mint: Dict[str, str] = {}
    for entry in golden_events:
        mint = str(entry.get("mint"))
        depth = _depth_from_event(entry)
        assert depth > 0.0
        gh = entry.get("hash")
        assert gh, f"missing hash for {mint}"
        previous = hash_by_mint.setdefault(mint, gh)
        assert previous == gh, f"unstable hash for {mint}"

    assert len(suggestion_events) >= 6, f"expected at least 6 suggestions, got {len(suggestion_events)}"
    golden_hashes = {entry.get("hash") for entry in golden_events}
    for entry in suggestion_events:
        integrity = entry.get("integrity", {})
        assert integrity.get("golden_hash") in golden_hashes
        edge_value = float(entry.get("edge") or 0.0)
        breakeven_value = float(entry.get("breakeven_bp") or 0.0)
        assert edge_value > 0.0
        assert breakeven_value > 0.0

    status_snapshot = demo_context.wiring.collectors.status_snapshot()
    assert status_snapshot.get("event_bus") is True
    assert status_snapshot.get("trading_loop") is True
    bus_lag = status_snapshot.get("bus_latency_ms")
    if isinstance(bus_lag, (int, float)):
        assert bus_lag < 1000.0
    depth_lag = status_snapshot.get("depth_lag_ms")
    if isinstance(depth_lag, (int, float)):
        assert depth_lag < 2500.0

    for record in caplog.records:
        message = record.getMessage()
        assert "Dropping redis event with incompatible protobuf schema" not in message
        assert "schema mismatch for x:market.ohlcv.5m" not in message

    assert discovered_path.exists()
    assert golden_path.exists()
    assert suggestions_path.exists()
    assert summary_path.exists()
    assert report_json_path.exists()
    payload = json.loads(report_json_path.read_text(encoding="utf-8"))
    assert payload.get("status") == "PASS"
    summary_text = summary_path.read_text(encoding="utf-8")
    assert "Golden Demo Summary" in summary_text
    assert "Checks:" in summary_text
