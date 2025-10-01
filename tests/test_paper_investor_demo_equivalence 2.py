from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import paper
import solhunter_zero.reports as report_schema

from tests.market_data import load_live_prices
from tests.test_investor_demo import assert_demo_reports

pytestmark = pytest.mark.timeout(60)


def test_paper_investor_demo_equivalence(tmp_path: Path, monkeypatch, capsys) -> None:
    """Ensure paper CLI mirrors investor demo output structure."""

    prices, dates = load_live_prices()
    ticks = [
        {"timestamp": d, "price": p}
        for p, d in zip(prices, dates)
    ]
    data_path = tmp_path / "ticks.json"
    data_path.write_text(json.dumps(ticks))

    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    from tests import stubs

    stubs.install_stubs()
    import solhunter_zero.investor_demo as demo

    paper_reports = tmp_path / "paper_reports"
    paper.run(["--reports", str(paper_reports), "--ticks", str(data_path)])
    captured = capsys.readouterr().out
    assert "Capital Summary:" in captured

    match = re.search(r"Trade type results: (\{.*\})", captured)
    assert match, "trade results missing from output"
    results = json.loads(match.group(1))
    assert isinstance(results.get("flash_loan_signature"), str)
    assert isinstance(results.get("arbitrage_path"), list)

    from paper import _ticks_to_price_file

    demo_reports = tmp_path / "demo_reports"
    price_file = _ticks_to_price_file(ticks)
    demo.main(["--data", str(price_file), "--reports", str(demo_reports)])

    paper_files = {p.name for p in paper_reports.glob("*.json")}
    demo_files = {p.name for p in demo_reports.glob("*.json")}
    assert paper_files == demo_files
    assert report_schema.REQUIRED_JSON.issubset(paper_files)

    p_summary, p_hist, p_high = report_schema.load_reports(paper_reports)
    d_summary, d_hist, d_high = report_schema.load_reports(demo_reports)
    assert_demo_reports(p_summary, p_hist)
    assert_demo_reports(d_summary, d_hist)
    p_map = {s.config: s for s in p_summary}
    d_map = {s.config: s for s in d_summary}
    assert p_map.keys() == d_map.keys()
    for strat in p_map:
        p_row = p_map[strat]
        d_row = d_map[strat]
        assert p_row.roi == d_row.roi
        assert p_row.trades == d_row.trades
        p_actions = [t.action for t in p_hist if t.strategy == strat]
        d_actions = [t.action for t in d_hist if t.strategy == strat]
        assert len(p_actions) == len(d_actions)
        assert p_actions == d_actions
    assert p_high.top_strategy and d_high.top_strategy
