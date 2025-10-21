"""Price blend resilience and diagnostics tests."""

from __future__ import annotations

import asyncio
import copy
import dataclasses
from typing import Iterable, List

import pytest

from solhunter_zero.golden_pipeline.contracts import STREAMS

from .conftest import (
    FakeBroker,
    FrozenClock,
    GoldenPipelineHarness,
    PriceFixture,
    SCENARIO_PAYLOADS,
    approx,
)


_MONKEYPATCH_TARGETS = (
    "solhunter_zero.golden_pipeline.utils.now_ts",
    "solhunter_zero.golden_pipeline.market.now_ts",
    "solhunter_zero.golden_pipeline.coalescer.now_ts",
    "solhunter_zero.golden_pipeline.execution.now_ts",
    "solhunter_zero.golden_pipeline.agents.now_ts",
    "solhunter_zero.golden_pipeline.pricing.now_ts",
    "solhunter_zero.golden_pipeline.voting.now_ts",
)


def _clone_price_updates(fixtures: Iterable[PriceFixture]) -> List[PriceFixture]:
    return [dataclasses.replace(fixture) for fixture in fixtures]


def _jitter(fixtures: Iterable[PriceFixture], delta: float) -> List[PriceFixture]:
    mutated: List[PriceFixture] = []
    for fixture in fixtures:
        mutated.append(
            dataclasses.replace(
                fixture,
                bid=fixture.bid + delta,
                ask=fixture.ask + delta,
                mid=fixture.mid + delta,
            )
        )
    return mutated


def _with_outlier(fixtures: Iterable[PriceFixture], *, jump: float) -> List[PriceFixture]:
    mutated = _clone_price_updates(fixtures)
    if mutated:
        mutated[-1] = dataclasses.replace(
            mutated[-1],
            bid=mutated[-1].bid + jump,
            ask=mutated[-1].ask + jump,
            mid=mutated[-1].mid + jump,
        )
    return mutated


@pytest.fixture
def make_price_harness():
    def _builder(price_updates: Iterable[PriceFixture], *, idle_gap: float = 0.0):
        scenario = copy.deepcopy(SCENARIO_PAYLOADS)
        scenario["price_updates"] = list(price_updates)
        scenario["price_idle_gap"] = idle_gap
        clock = FrozenClock()
        monkeypatch = pytest.MonkeyPatch()
        for target in _MONKEYPATCH_TARGETS:
            monkeypatch.setattr(target, clock.time, raising=False)
        bus = FakeBroker()
        harness = GoldenPipelineHarness(clock=clock, bus=bus, scenario=scenario)
        try:
            asyncio.run(harness.run())
        finally:
            monkeypatch.undo()
        return harness, bus

    return _builder


def _acceptance_rate(harness: GoldenPipelineHarness) -> float:
    suggestions = len(harness.trade_suggestions)
    return len(harness.decisions) / suggestions if suggestions else 0.0


def _total_pnl(harness: GoldenPipelineHarness) -> float:
    if not harness.virtual_pnls:
        return 0.0
    latest = harness.virtual_pnls[-1]
    return float(latest.realized_usd + latest.unrealized_usd)


BASE_PRICE_UPDATES = tuple(SCENARIO_PAYLOADS["price_updates"])


@pytest.mark.parametrize(
    "label,price_updates,idle_gap,expected_alerts,expect_price_stream,check_metrics",
    [
        (
            "missing",
            (),
            0.0,
            {"price.missing", "price.depth_fallback"},
            False,
            False,
        ),
        (
            "stale",
            BASE_PRICE_UPDATES,
            3.5,
            {"price.stale"},
            True,
            False,
        ),
        (
            "noisy",
            tuple(_jitter(BASE_PRICE_UPDATES, 0.0006)),
            0.0,
            set(),
            True,
            True,
        ),
        (
            "outlier",
            tuple(_with_outlier(BASE_PRICE_UPDATES, jump=0.08)),
            0.0,
            {"price.outlier"},
            True,
            False,
        ),
    ],
    ids=["missing", "stale", "noisy", "outlier"],
)
def test_price_blend_noise_handling(
    make_price_harness,
    label,
    price_updates,
    idle_gap,
    expected_alerts,
    expect_price_stream,
    check_metrics,
) -> None:
    baseline_harness, _ = make_price_harness(BASE_PRICE_UPDATES, idle_gap=0.0)
    scenario_harness, scenario_bus = make_price_harness(price_updates, idle_gap=idle_gap)

    latest_baseline = baseline_harness.golden_snapshots[-1]
    latest_scenario = scenario_harness.golden_snapshots[-1]
    alerts = set(latest_scenario.px.get("diagnostics", {}).get("alerts", []))

    if expected_alerts:
        assert expected_alerts <= alerts
    else:
        assert not alerts

    if expect_price_stream:
        assert scenario_bus.events.get(STREAMS.market_price)
    else:
        assert not scenario_bus.events.get(STREAMS.market_price)

    if "price.outlier" in alerts:
        assert latest_scenario.px["mid_usd"] == approx(
            latest_baseline.px["mid_usd"], abs_tol=1e-2
        )

    if check_metrics:
        assert _acceptance_rate(scenario_harness) == approx(
            _acceptance_rate(baseline_harness), abs_tol=1e-6
        )
        assert _total_pnl(scenario_harness) == approx(
            _total_pnl(baseline_harness), abs_tol=5e-2
        )

    if "price.missing" in alerts:
        depth_events = scenario_bus.events.get(STREAMS.market_depth) or []
        assert depth_events
        assert latest_scenario.px["mid_usd"] == approx(depth_events[-1]["mid_usd"])

