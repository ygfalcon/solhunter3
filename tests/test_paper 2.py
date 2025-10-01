from __future__ import annotations

import json
from pathlib import Path

import pytest
import paper
import solhunter_zero.reports as report_schema
import solhunter_zero.investor_demo as investor_demo


def test_paper_generates_reports(tmp_path, monkeypatch):
    """Running the paper CLI should emit summaries based on strategy returns."""

    ticks = [
        {"timestamp": "2023-01-01", "price": 1.0},
        {"timestamp": "2023-01-02", "price": 2.0},
        {"timestamp": "2023-01-03", "price": 1.0},
    ]
    data_path = tmp_path / "ticks.json"
    data_path.write_text(json.dumps(ticks))

    reports = tmp_path / "reports"
    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    paper.run(["--reports", str(reports), "--ticks", str(data_path)])

    trade_path = reports / "trade_history.json"
    trade_data = json.loads(trade_path.read_text())
    # Validate trade log format mirrors live logging in solhunter_zero/memory.py
    for entry in trade_data:
        entry.setdefault("side", entry.get("action"))
        entry.setdefault("amount", entry.get("capital"))
        assert {"token", "side", "amount", "price"} <= entry.keys()

    summary, trade_hist, highlights = report_schema.load_reports(reports)

    prices = [t["price"] for t in ticks]
    strategy_names = {entry["strategy"] for entry in trade_data}
    for name, strat in investor_demo.DEFAULT_STRATEGIES:
        assert name in strategy_names
        actual_actions = [t.action for t in trade_hist if t.strategy == name]
        returns = strat(prices)
        expected_actions = ["buy"] + [
            "buy" if r > 0 else "sell" if r < 0 else "hold" for r in returns
        ]
        assert actual_actions == expected_actions

    momentum = next(r for r in summary if r.config == "momentum")
    assert momentum.roi == pytest.approx(1.0)
    assert highlights.top_strategy

