"""Test the investor demo CLI wrapper."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import solhunter_zero.reports as report_schema

from tests.market_data import load_live_prices


def assert_demo_reports(
    summary: list[report_schema.StrategySummary],
    trade_hist: list[report_schema.TradeRecord],
) -> None:
    """Common assertions for demo and paper investor outputs.

    Ensures all expected default strategies are present with positive ROI and
    that corresponding trade history entries exist.
    """

    expected = {"buy_hold", "momentum", "mean_reversion"}

    for strat in expected:
        row = next((r for r in summary if r.config == strat), None)
        assert row is not None
        assert row.trades > 0
        assert row.roi > 0

    recorded = {t.strategy for t in trade_hist}
    assert expected.issubset(recorded)


pytestmark = pytest.mark.timeout(30)


def test_investor_demo(tmp_path: Path, monkeypatch, capsys) -> None:
    """Run the demo and verify strategy ROI and trade history."""

    reports = tmp_path / "reports"
    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    from tests import stubs

    stubs.install_stubs()
    import solhunter_zero.investor_demo as demo

    prices, dates = load_live_prices()
    monkeypatch.setattr(
        demo,
        "load_prices",
        lambda path=None, preset=None: (prices, dates),
    )

    demo.main(["--preset", "short", "--reports", str(reports)])
    captured = capsys.readouterr().out
    assert "Capital Summary:" in captured

    match = re.search(r"Trade type results: (\{.*\})", captured)
    assert match, "trade results missing from output"
    results = json.loads(match.group(1))
    assert results["flash_loan_signature"] == "sig"
    assert results["arbitrage_path"] == ["dex1", "dex2"]

    summary, trade_hist, highlights = report_schema.load_reports(reports)
    assert highlights.top_strategy
    assert_demo_reports(summary, trade_hist)
