import asyncio
from collections import Counter
from types import SimpleNamespace

import pytest

from solhunter_zero.golden_pipeline.discovery import DiscoveryStage
from solhunter_zero.golden_pipeline.service import AgentManagerAgent
from solhunter_zero.golden_pipeline.types import DiscoveryCandidate
from solhunter_zero.golden_pipeline.types import GoldenSnapshot

from tests.golden_pipeline.conftest import BASE58_MINTS


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def test_discovery_sources_deduplicate(golden_harness):
    events = golden_harness.discovery_events
    sources = Counter(event["source"] for event in events)

    assert sources["das"] == 2  # two unique mints via DAS
    assert sources["das_timeout"] == 3

    accepted_mints = {
        event["mint"]
        for event in events
        if event.get("accepted")
    }
    assert accepted_mints == {
        BASE58_MINTS["alpha"],
        BASE58_MINTS["beta"],
    }

    for source in ("fallback", "mempool", "amm", "pumpfun", "replay"):
        rejected = [event for event in events if event["source"] == source]
        assert rejected and all(event.get("accepted") is False for event in rejected)

    stage = golden_harness.pipeline._discovery_stage  # type: ignore[attr-defined]
    assert stage.seen_recently(BASE58_MINTS["alpha"]) is True
    assert stage.seen_recently(BASE58_MINTS["beta"]) is True

    metrics = golden_harness.pipeline.metrics_snapshot()
    success_stats = metrics.get("discovery.success_total", {})
    failure_stats = metrics.get("discovery.failure_total", {})
    dedupe_stats = metrics.get("discovery.dedupe_drops", {})
    breaker_stats = metrics.get("discovery.breaker_openings", {})

    assert (success_stats.get("max") or 0.0) >= 2.0
    assert (failure_stats.get("max") or 0.0) >= 3.0
    assert (dedupe_stats.get("max") or 0.0) >= 1.0
    assert (breaker_stats.get("max") or 0.0) >= 1.0


@pytest.mark.anyio
async def test_discovery_candidates_emit_concurrently():
    gate = asyncio.Event()
    concurrency = 0
    max_concurrency = 0

    async def emit(_: DiscoveryCandidate) -> None:
        nonlocal concurrency, max_concurrency
        await gate.wait()
        concurrency += 1
        max_concurrency = max(max_concurrency, concurrency)
        await asyncio.sleep(0.05)
        concurrency -= 1

    stage = DiscoveryStage(emit)
    candidates = [
        DiscoveryCandidate(mint=BASE58_MINTS["alpha"], asof=0.0),
        DiscoveryCandidate(mint=BASE58_MINTS["beta"], asof=0.0),
    ]

    tasks = [asyncio.create_task(stage.submit(candidate)) for candidate in candidates]

    # Ensure both submissions reach the emit gate before releasing them.
    await asyncio.sleep(0.01)
    gate.set()

    results = await asyncio.gather(*tasks)

    assert results == [True, True]
    assert max_concurrency >= 2


class _RecordingAgent(AgentManagerAgent):
    """AgentManagerAgent variant that records gate reports for assertions."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.reports: list[tuple[str, dict]] = []

    def _apply_entry_gates(self, snapshot, raw_action, action):  # type: ignore[override]
        report = super()._apply_entry_gates(snapshot, raw_action, action)
        self.reports.append((snapshot.hash, report))
        return report


class _StubManager:
    def __init__(self, actions):
        self.actions = actions

    async def evaluate_with_swarm(self, mint, portfolio):  # pragma: no cover - test stub
        return SimpleNamespace(actions=self.actions)


class _StubPortfolio:
    pass


def _snapshot(hash_id: str, spread_bps: float, depth_usd: float, *, buyers: int) -> GoldenSnapshot:
    return GoldenSnapshot(
        mint=BASE58_MINTS["alpha"],
        asof=1_700_000_000.0,
        meta={"decimals": 6},
        px={"mid_usd": 1.0, "spread_bps": spread_bps},
        liq={"depth_pct": {"1": depth_usd}},
        ohlcv5m={"zret": 3.0, "zvol": 3.1, "buyers": buyers},
        hash=hash_id,
    )


@pytest.mark.anyio
async def test_micro_mode_depth_and_spread_gates_suggestions() -> None:
    """Micro-mode gates suppress or allow suggestions based on orderbook quality."""

    actions = [
        {
            "side": "buy",
            "price": 1.01,
            "pattern": "first_pullback",
            "notional_usd": 5_000.0,
            "fees_bps": 4.0,
        }
    ]
    agent = _RecordingAgent(
        _StubManager(actions),
        _StubPortfolio(),
        max_micro_spread_bps=30.0,
        min_depth_usd=25_000.0,
    )

    agent._last_buyers[_snapshot("seed", 0.0, 0.0, buyers=1).mint] = 1

    wide_spread = _snapshot("wide", 120.0, 50_000.0, buyers=2)
    shallow_depth = _snapshot("shallow", 10.0, 10_000.0, buyers=3)
    micro_ready = _snapshot("ready", 12.0, 40_000.0, buyers=4)

    assert await agent.generate(wide_spread) == []
    assert await agent.generate(shallow_depth) == []

    suggestions = await agent.generate(micro_ready)
    assert len(suggestions) == 1
    gating = suggestions[0].gating
    ruthless = gating.get("ruthless_filter", {})
    assert ruthless.get("passed") is True
    assert ruthless.get("spread_bps") == 12.0
    assert ruthless.get("depth_1pct_usd") == 40_000.0

    reports = {hash_id: report for hash_id, report in agent.reports}
    assert reports["wide"]["ruthless_filter"]["passed"] is False
    assert reports["wide"]["ruthless_filter"]["spread_bps"] == 120.0
    assert reports["shallow"]["ruthless_filter"]["passed"] is False
    assert reports["shallow"]["ruthless_filter"]["depth_1pct_usd"] == 10_000.0
