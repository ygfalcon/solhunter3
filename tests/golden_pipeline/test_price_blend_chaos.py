from __future__ import annotations

from typing import Callable, Dict, Mapping

import pytest

from .conftest import PriceProviderStub, run_golden_harness

SOL_MINT = "So11111111111111111111111111111111111111112"
EPSILON = 1e-3


def _metrics(harness) -> Dict[str, float]:
    pnl = 0.0
    if harness.virtual_pnls:
        last = harness.virtual_pnls[-1]
        pnl = float(last.realized_usd) + float(last.unrealized_usd)
    suggestions = len(harness.trade_suggestions)
    decisions = len(harness.decisions)
    acceptance = decisions / suggestions if suggestions else 0.0
    suggestions_by_hash: Dict[str, list] = {}
    for suggestion in harness.trade_suggestions:
        suggestions_by_hash.setdefault(suggestion.inputs_hash, []).append(suggestion)
    vote_windows: list[float] = []
    for decision in harness.decisions:
        candidates = suggestions_by_hash.get(decision.snapshot_hash) or []
        if not candidates:
            continue
        origin = float(candidates[0].generated_at)
        vote_windows.append(float(decision.ts) - origin)
    avg_vote_window = sum(vote_windows) / len(vote_windows) if vote_windows else 0.0
    return {"pnl": pnl, "acceptance": acceptance, "vote_window": avg_vote_window}


def _quote_sequence(price: float, **overrides: float) -> dict:
    quotes: Dict[str, Mapping[str, float]] = {
        SOL_MINT: {"price": price, **overrides},
    }
    return {"kind": "success", "quotes": quotes}


def _configure_stale(stub: PriceProviderStub) -> None:
    stub.configure("birdeye", [_quote_sequence(1.01)] * 4)
    stub.configure("jupiter", [{"kind": "success", "omit": True}] * 4)
    stub.configure(
        "dexscreener",
        [_quote_sequence(1.02, asof_delta_ms=-60000)] * 4,
    )


def _configure_missing(stub: PriceProviderStub) -> None:
    stub.configure("birdeye", [_quote_sequence(1.01)] * 4)
    stub.configure("jupiter", [{"kind": "success", "omit": True}] * 4)
    stub.configure("dexscreener", [_quote_sequence(1.03)] * 4)


def _configure_outlier(stub: PriceProviderStub) -> None:
    stub.configure("birdeye", [{"kind": "success", "omit": True}] * 4)
    stub.configure("jupiter", [_quote_sequence(1.0)] * 4)
    stub.configure(
        "dexscreener",
        [
            _quote_sequence(1.18),
            _quote_sequence(1.17),
            _quote_sequence(1.16),
            _quote_sequence(1.15),
        ],
    )


CHAOS_SCENARIOS: list[
    tuple[str, Callable[[PriceProviderStub], None], Mapping[str, str], tuple[str, ...]]
] = [
    (
        "stale",
        _configure_stale,
        {
            "PRICE_BLEND_STALE_MS": "5000",
            "PRICE_BLEND_MIN_SOURCES": "2",
            "PRICE_BLEND_PROVIDER_LIMIT": "4",
        },
        ("stale_quote",),
    ),
    (
        "missing",
        _configure_missing,
        {"PRICE_BLEND_MIN_SOURCES": "1"},
        ("missing_providers",),
    ),
    (
        "outlier",
        _configure_outlier,
        {
            "PRICE_BLEND_MIN_SOURCES": "2",
            "PRICE_BLEND_PROVIDER_LIMIT": "4",
            "PRICE_BLEND_SPREAD_ALERT_BPS": "25",
            "PRICE_BLEND_SIGMA_ALERT": "0.9",
            "PRICE_BLEND_SKEW_ALERT_BPS": "20",
        },
        ("sigma_outlier", "asymmetric_consensus"),
    ),
]


@pytest.fixture(scope="module")
def baseline_metrics(golden_harness) -> Dict[str, float]:
    return _metrics(golden_harness)


@pytest.mark.parametrize("name, configurator, env, expected_alerts", CHAOS_SCENARIOS)
def test_price_blend_chaos_resilience(name, configurator, env, expected_alerts, baseline_metrics):
    with run_golden_harness(clock_seed=1_700_123_456.0, configure_prices=configurator, env=env) as harness:
        metrics = _metrics(harness)
        for key, base_value in baseline_metrics.items():
            assert abs(metrics[key] - base_value) <= EPSILON, f"{key} diverged under {name}"

        assert harness.price_blend_diagnostics, "no blend diagnostics captured"
        latest_diag = harness.price_blend_diagnostics[-1].get(SOL_MINT)
        assert latest_diag, "probe diagnostics missing"
        alerts = latest_diag.get("alerts", [])
        for expected_alert in expected_alerts:
            assert expected_alert in alerts, (expected_alert, alerts)

        if "asymmetric_consensus" in expected_alerts:
            assert latest_diag.get("skew_bps", 0.0) > 0.0
            assert any(obs.get("outlier") for obs in latest_diag.get("observations", []))

        metrics_events = harness.bus.events.get("metrics.prices.blend", [])
        assert metrics_events, "blend metrics not published"
        for expected_alert in expected_alerts:
            assert any(
                expected_alert
                in (event.get("diagnostics", {}).get(SOL_MINT, {}).get("alerts", []))
                for event in metrics_events
            ), f"expected alert {expected_alert} not propagated to metrics"
