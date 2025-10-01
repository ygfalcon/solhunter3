import json
import sys

from solhunter_zero import investor_demo


def test_run_rl_demo_fallback(monkeypatch, tmp_path):
    """Fallback path writes RL metrics when NumPy is unavailable."""

    monkeypatch.delitem(sys.modules, "numpy", raising=False)
    monkeypatch.setattr(investor_demo, "RL_DEMO", True)

    reward = investor_demo.run_rl_demo(tmp_path)

    assert reward != 0.0

    metrics = json.loads((tmp_path / "rl_metrics.json").read_text())
    assert isinstance(metrics["loss"], list)
    assert isinstance(metrics["rewards"], list)
