from __future__ import annotations

import json
from pathlib import Path

import paper
import solhunter_zero.investor_demo as demo


def test_paper_preset_and_rl_demo(tmp_path: Path, monkeypatch, shared_prices):
    """Using preset datasets and RL flags should affect report outputs."""

    called: dict[str, object] = {}

    def fake_load_prices(path=None, preset=None):
        called["path"] = path
        called["preset"] = preset
        dates = list(range(len(shared_prices)))
        return shared_prices, dates

    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    monkeypatch.setattr(demo, "load_prices", fake_load_prices)

    base_reports = tmp_path / "base"
    paper.run(["--reports", str(base_reports), "--preset", "short"])
    assert called["preset"] == "short"
    assert not (base_reports / "rl_metrics.json").exists()

    rl_reports = tmp_path / "rl"
    paper.run(["--reports", str(rl_reports), "--preset", "short", "--rl-demo"])
    assert (rl_reports / "rl_metrics.json").exists()

    base_files = {p.name for p in base_reports.iterdir()}
    rl_files = {p.name for p in rl_reports.iterdir()}
    assert "rl_metrics.json" not in base_files
    assert "rl_metrics.json" in rl_files
    assert base_files != rl_files


def test_paper_learn_writes_metrics(tmp_path: Path, monkeypatch, capsys, shared_prices):
    """The --learn flag should trigger the learning loop and RL metrics."""

    def fake_load_prices(path=None, preset=None):
        dates = list(range(len(shared_prices)))
        return shared_prices, dates

    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    monkeypatch.setattr(demo, "load_prices", fake_load_prices)

    reports = tmp_path / "learn"
    paper.run(["--reports", str(reports), "--preset", "short", "--learn"])

    captured = capsys.readouterr().out
    assert "Learning iteration" in captured

    metrics = json.loads((reports / "rl_metrics.json").read_text())
    assert isinstance(metrics.get("loss"), list)
    assert isinstance(metrics.get("rewards"), list)

