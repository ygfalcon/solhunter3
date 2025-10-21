import asyncio
import json
import sys
import types
from collections import defaultdict
from types import SimpleNamespace
from typing import Mapping

import pytest

from tests.stubs import stub_sqlalchemy

stub_sqlalchemy()

if "base58" not in sys.modules:
    base58_mod = types.ModuleType("base58")
    base58_mod.b58decode = lambda *a, **k: b""
    base58_mod.b58encode = lambda *a, **k: b""
    sys.modules["base58"] = base58_mod

from solhunter_zero.golden_pipeline.bus import EventBusAdapter, InMemoryBus, MessageBus
from solhunter_zero.golden_pipeline.contracts import STREAMS
from solhunter_zero.golden_pipeline.pipeline import GoldenPipeline
from solhunter_zero.golden_pipeline.service import AgentManagerAgent
from solhunter_zero.golden_pipeline.types import GoldenSnapshot
from solhunter_zero.event_bus import BUS as RUNTIME_EVENT_BUS
from solhunter_zero.production.env import (
    Provider,
    format_configured_providers,
    load_env_file,
    validate_providers,
    write_env_manifest,
)

from tests.golden_pipeline.conftest import BASE58_MINTS, SCENARIO_PAYLOADS, STREAMS, approx


@pytest.mark.parametrize("secret_value", ["super-secret-value"])
def test_env_manifest_and_secret_controls(tmp_path, monkeypatch, caplog, secret_value):
    """Environment manifests must validate keys and avoid leaking secrets."""

    provider = Provider("Runtime", ("SECRET_KEY",))
    manifest_file = tmp_path / "manifest.json"

    monkeypatch.setenv("SECRET_KEY", secret_value)
    caplog.set_level("INFO")
    manifest_path = write_env_manifest(
        manifest_file,
        [provider],
        source_map={"SECRET_KEY": "vault"},
    )
    payload = json.loads(manifest_path.read_text())
    assert payload["entries"], "manifest did not record any keys"
    entry = payload["entries"][0]
    assert entry["configured"] is True
    assert entry["source"] == "vault"
    assert entry["key_hash"] != "SECRET_KEY"
    assert secret_value not in manifest_path.read_text()
    assert secret_value not in caplog.text

    # Placeholders or missing values should be flagged by provider validation.
    monkeypatch.delenv("SECRET_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("SECRET_KEY=REDACTED\n")
    loaded = load_env_file(env_file, overwrite=True)
    assert loaded["SECRET_KEY"] == "REDACTED"

    missing, placeholders = validate_providers([provider], env={"SECRET_KEY": ""})
    assert missing and missing[0].reason == "missing"

    missing, placeholders = validate_providers([provider], env=loaded)
    assert not missing and placeholders
    assert "REDACTED" in placeholders[0].reason

    monkeypatch.setenv("SECRET_KEY", "actual-value")
    assert "Runtime" in format_configured_providers([provider])


def test_clock_ids_and_broker_health(golden_harness, fake_broker):
    """Clock alignment, idempotency keys, and broker ordering stay consistent."""

    snapshots = golden_harness.golden_snapshots
    assert len(snapshots) >= 3

    asofs = [snap.asof for snap in snapshots]
    assert asofs == sorted(asofs), "snapshot timestamps were not monotonic"

    emitted_times = [snap.metrics["emitted_at"] for snap in snapshots]
    assert emitted_times == sorted(emitted_times)

    known_hashes = {snap.hash: snap for snap in snapshots}
    suggestions = golden_harness.trade_suggestions
    assert suggestions, "agents never produced suggestions"
    for suggestion in suggestions:
        assert suggestion.inputs_hash in known_hashes
        assert suggestion.generated_at == known_hashes[suggestion.inputs_hash].asof
        assert suggestion.ttl_sec > 0.0

    decisions = golden_harness.decisions
    assert decisions, "no voting decisions were produced"
    order_ids = {decision.client_order_id for decision in decisions}
    assert len(order_ids) == len(decisions)
    for decision in decisions:
        assert len(decision.client_order_id) == 64
        assert decision.snapshot_hash in known_hashes
        assert decision.ts >= known_hashes[decision.snapshot_hash].asof

    replay_events = [e for e in golden_harness.discovery_events if e["source"] == "replay"]
    assert replay_events and all(not e["accepted"] for e in replay_events)

    stage = golden_harness.pipeline._discovery_stage  # type: ignore[attr-defined]
    for _ in range(5):
        stage.mark_failure()
    assert stage.circuit_open
    from solhunter_zero.golden_pipeline.types import DiscoveryCandidate
    import asyncio

    fresh_mint = "MintFresh11111111111111111111111111111111"
    candidate = DiscoveryCandidate(mint=fresh_mint, asof=snapshots[-1].asof + 1.0)
    accepted = asyncio.run(golden_harness.pipeline.submit_discovery(candidate))
    assert not accepted
    stage.mark_success()

    publish_order = [stream for stream, _ in fake_broker.published]
    assert publish_order[0] == STREAMS.discovery_candidates
    assert publish_order.index(STREAMS.vote_decisions) > publish_order.index(STREAMS.trade_suggested)

    golden_events = fake_broker.events[STREAMS.golden_snapshot]
    timeline = [event["metrics"]["emitted_at"] for event in golden_events]
    assert timeline == sorted(timeline)


def test_discovery_fallback_and_source_coverage(golden_harness):
    """Discovery covers every source and tolerates degraded providers."""

    events = golden_harness.discovery_events
    sources_seen = {event["source"] for event in events}
    assert {"das", "fallback", "mempool", "amm", "pumpfun", "das_timeout", "replay"}.issubset(sources_seen)

    timeouts = [idx for idx, event in enumerate(events) if event["source"] == "das_timeout"]
    assert timeouts == [2, 3, 4], "DAS degradation sequence was not captured"

    plan = SCENARIO_PAYLOADS["discovery_plan"]
    for source, mints in plan.items():
        accepted = {
            event["mint"]
            for event in events
            if event["source"] == source and event.get("accepted")
        }
        if source == "das":
            assert accepted == set(mints)
        else:
            assert not accepted

    stage = golden_harness.pipeline._discovery_stage  # type: ignore[attr-defined]
    for mint in plan["das"]:
        assert stage.seen_recently(mint)

    token_events = golden_harness.bus.events[STREAMS.token_snapshot]  # type: ignore[attr-defined]
    assert token_events
    target_mint = BASE58_MINTS["alpha"]
    latest_token = next(event for event in token_events if event["mint"] == target_mint)
    assert latest_token["token_program"].startswith("Tokenkeg")
    flags = latest_token.get("flags", {})
    assert flags.get("program", "").startswith("pumpfun")
    assert set(flags.get("sources", ())) >= {"rpc", "das", "mempool", "amm", "pumpfun"}

    batches = {tuple(batch) for batch in golden_harness.metadata_requests}
    expected_batches = {
        (SCENARIO_PAYLOADS["discovery_plan"]["das"][0],),
        (SCENARIO_PAYLOADS["discovery_plan"]["das"][1],),
    }
    assert batches == expected_batches


def test_pricing_execution_risk_and_observability(golden_harness, fake_broker):
    """Pricing blend, execution sims, risk gates, and telemetry remain consistent."""

    latest_snapshot = golden_harness.golden_snapshots[-1]
    assert latest_snapshot.px["spread_bps"] > 0.0
    assert latest_snapshot.meta["decimals"] == 6

    depth_events = fake_broker.events[STREAMS.market_depth]
    assert depth_events
    venues = {event["venue"] for event in depth_events}
    assert venues == {"aggregated"}
    assert all(event["mid_usd"] > 0 for event in depth_events)
    assert latest_snapshot.px["mid_usd"] == depth_events[-1]["mid_usd"]

    ohlcv_events = fake_broker.events[STREAMS.market_ohlcv]
    assert ohlcv_events and ohlcv_events[0]["vol_usd"] == approx(
        sum(abs(evt.amount_quote) for evt in SCENARIO_PAYLOADS["tape_events"])
    )

    fills_by_order = {fill.order_id: fill for fill in golden_harness.virtual_fills}
    for decision in golden_harness.decisions:
        fill = fills_by_order[decision.client_order_id]
        implied_mid = decision.notional_usd / fill.qty_base
        assert fill.price_usd >= implied_mid
        assert fill.fees_usd == approx(decision.notional_usd * 0.0004)
        assert fill.slippage_bps > 0.0

    suggestions_by_hash = defaultdict(set)
    for suggestion in golden_harness.trade_suggestions:
        suggestions_by_hash[suggestion.inputs_hash].add(suggestion.agent)
        gating = suggestion.gating or {}
        expected = gating.get("expected_edge_bps") or suggestion.risk.get("expected_edge_bps")
        breakeven = gating.get("breakeven_bps") or suggestion.risk.get("breakeven_bps")
        assert gating.get("edge_pass") is True
        assert expected is not None and breakeven is not None
        assert expected >= breakeven + 20.0

    for decision in golden_harness.decisions:
        assert suggestions_by_hash[decision.snapshot_hash] == {"momentum_v1", "meanrev_v1"}

    metrics = golden_harness.pipeline.metrics_snapshot()
    for key in ("latency_ms", "depth_staleness_ms", "candle_age_ms"):
        summary = metrics[key]
        assert summary["count"] >= 3.0
        if summary["p95"] is not None:
            assert summary["p95"] >= summary["p50"]

    for stream in (
        STREAMS.trade_suggested,
        STREAMS.vote_decisions,
        STREAMS.virtual_fills,
        STREAMS.live_fills,
    ):
        assert fake_broker.events.get(stream), f"missing events for {stream}"

    summary = golden_harness.summary()
    assert summary["paper_pnl"]["count"] == len(golden_harness.virtual_pnls)
    assert summary["shadow_fills_by_venue"].get("VIRTUAL") == len(golden_harness.virtual_fills)

    live_events = fake_broker.events[STREAMS.live_fills]
    decision_ids = {decision.client_order_id for decision in golden_harness.decisions}
    assert all(event["sig"] in decision_ids for event in live_events)
    assert all(event["fees_usd"] <= 3.0 for event in live_events)


def test_pipeline_uses_injected_bus_in_live_mode(monkeypatch):
    """GoldenPipeline must emit via the injected bus when running live."""

    monkeypatch.setenv("SOLHUNTER_MODE", "live")
    created_inmemory = {"count": 0}
    original_init = InMemoryBus.__init__

    def _tracking_init(self) -> None:  # type: ignore[override]
        created_inmemory["count"] += 1
        original_init(self)

    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.pipeline.InMemoryBus.__init__",
        _tracking_init,
    )

    adapter = EventBusAdapter(RUNTIME_EVENT_BUS)

    class SpyBus(MessageBus):
        def __init__(self) -> None:
            self.sent: list[tuple[str, dict[str, object]]] = []

        async def publish(
            self,
            stream: str,
            payload: Mapping[str, object],
            *,
            dedupe_key: str | None = None,
        ) -> None:
            record = (stream, dict(payload), dedupe_key)
            self.sent.append(record)
            await adapter.publish(stream, payload, dedupe_key=dedupe_key)

    spy_bus = SpyBus()

    async def _fetcher(mints):
        return {}

    pipeline = GoldenPipeline(enrichment_fetcher=_fetcher, bus=spy_bus)
    asyncio.run(pipeline._publish(STREAMS.golden_snapshot, {"mint": "MintLive"}))

    assert spy_bus.sent and spy_bus.sent[0][0] == STREAMS.golden_snapshot
    assert created_inmemory["count"] == 0


def test_pipeline_requires_bus_in_live_mode(monkeypatch):
    """Constructing the pipeline without a bus in live mode should fail fast."""

    monkeypatch.setenv("SOLHUNTER_MODE", "live")

    async def _fetcher(mints):
        return {}

    with pytest.raises(RuntimeError):
        GoldenPipeline(enrichment_fetcher=_fetcher)


class _StubPortfolio:
    pass


class _StaticManager:
    def __init__(self, actions: list[dict[str, object]]) -> None:
        self._actions = actions

    async def evaluate_with_swarm(self, mint: str, portfolio: _StubPortfolio) -> SimpleNamespace:
        return SimpleNamespace(actions=list(self._actions))


def _build_guard_snapshot(
    *,
    mint: str = "GuardMint11111111111111111111111111111111",
    depth1: float = 20_000.0,
    buyers: int = 32,
    zret: float = 3.2,
    zvol: float = 3.5,
    spread: float = 32.0,
) -> GoldenSnapshot:
    now = 1_700_000_000.0
    return GoldenSnapshot(
        mint=mint,
        asof=now,
        meta={
            "symbol": "GUARD",
            "decimals": 6,
            "token_program": "Tokenkeg11111111111111111111111111111111",
        },
        px={"mid_usd": 1.0, "spread_bps": spread},
        liq={
            "depth_pct": {
                "1": depth1,
                "2": depth1 * 1.4,
                "5": depth1 * 2.2,
            },
            "asof": now,
        },
        ohlcv5m={
            "o": 1.0,
            "h": 1.01,
            "l": 0.99,
            "c": 1.0,
            "vol_usd": 250_000.0,
            "buyers": buyers,
            "trades": 120,
            "flow_usd": 0.0,
            "zret": zret,
            "zvol": zvol,
            "asof_close": now,
        },
        hash=f"hash-{mint}",
        metrics={},
    )


def _run_agent(agent: AgentManagerAgent, snapshot: GoldenSnapshot) -> list:
    async def _inner() -> list:
        return await agent.generate(snapshot)

    return asyncio.run(_inner())


def test_budget_guard_emits_global_cap_event():
    bus = InMemoryBus()
    raw_action = {
        "side": "buy",
        "notional_usd": 15_000.0,
        "pattern": "first_pullback",
        "expected_roi": 0.12,
        "agent": "momentum",
    }
    manager = _StaticManager([raw_action])
    agent = AgentManagerAgent(
        manager,
        _StubPortfolio(),
        bus=bus,
        global_notional_cap=5_000.0,
    )
    snapshot = _build_guard_snapshot()
    agent._last_buyers[snapshot.mint] = 12

    suggestions = _run_agent(agent, snapshot)
    assert suggestions == []

    events = bus.events.get(STREAMS.trade_rejected, [])
    assert len(events) == 1
    event = events[0]
    assert event["guard"] == "budget"
    assert event["scope"] == "global"
    assert pytest.approx(event["cap_usd"]) == 5_000.0
    assert pytest.approx(event["notional_usd"]) == 15_000.0
    assert "global cap" in event["reason"].lower()


def test_budget_guard_emits_agent_cap_event():
    bus = InMemoryBus()
    raw_action = {
        "side": "buy",
        "notional_usd": 6_500.0,
        "pattern": "first_pullback",
        "expected_roi": 0.15,
        "agent": "scalper",
    }
    manager = _StaticManager([raw_action])
    agent = AgentManagerAgent(
        manager,
        _StubPortfolio(),
        bus=bus,
        global_notional_cap=25_000.0,
        agent_notional_caps={"scalper": 4_000.0},
    )
    snapshot = _build_guard_snapshot(mint="GuardMintAgent1111111111111111111111111111")
    agent._last_buyers[snapshot.mint] = 18

    suggestions = _run_agent(agent, snapshot)
    assert suggestions == []

    events = bus.events.get(STREAMS.trade_rejected, [])
    assert len(events) == 1
    event = events[0]
    assert event["guard"] == "budget"
    assert event["scope"] == "agent"
    assert event["source_agent"] == "scalper"
    assert pytest.approx(event["cap_usd"]) == 4_000.0
    assert pytest.approx(event["notional_usd"]) == 6_500.0
    assert "cap" in event["reason"].lower()


def test_slippage_guard_blocks_thin_books():
    bus = InMemoryBus()
    raw_action = {
        "side": "buy",
        "notional_usd": 45_000.0,
        "pattern": "first_pullback",
        "expected_roi": 0.2,
        "max_slippage_bps": 40.0,
        "agent": "momentum",
    }
    manager = _StaticManager([raw_action])
    agent = AgentManagerAgent(
        manager,
        _StubPortfolio(),
        bus=bus,
        global_notional_cap=90_000.0,
    )
    snapshot = _build_guard_snapshot(
        mint="GuardMintThin111111111111111111111111111111",
        depth1=18_000.0,
    )
    agent._last_buyers[snapshot.mint] = 20

    suggestions = _run_agent(agent, snapshot)
    assert suggestions == []

    events = bus.events.get(STREAMS.trade_rejected, [])
    assert len(events) == 1
    event = events[0]
    assert event["guard"] == "slippage"
    assert pytest.approx(event["notional_usd"]) == 45_000.0
    available = event.get("available_notional_usd")
    assert available is not None and available < event["notional_usd"]
    assert "slippage" in (event.get("reason") or "").lower()
