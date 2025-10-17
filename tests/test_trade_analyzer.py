from __future__ import annotations

from types import SimpleNamespace

import pytest

import solhunter_zero.trade_analyzer as trade_analyzer_module
from solhunter_zero.trade_analyzer import TradeAnalyzer, analyze_trades


class _FakeMemory:
    def __init__(self, trades):
        self._trades = trades

    def list_trades(self, limit=1000):  # noqa: ARG002 - signature compatibility
        return self._trades


class _AsyncMemory(_FakeMemory):
    async def list_trades(self, limit=1000):  # noqa: ARG002
        return self._trades


def _base_trades():
    return [
        SimpleNamespace(reason="a1", direction="buy", amount=1, price=1),
        SimpleNamespace(reason="a1", direction="sell", amount=1, price=2),
        SimpleNamespace(reason="a2", direction="buy", amount=1, price=2),
        SimpleNamespace(reason="a2", direction="sell", amount=1, price=1),
    ]


def test_trade_analyzer_recommend_weights():
    analyzer = TradeAnalyzer(_FakeMemory(_base_trades()))
    res = analyzer.recommend_weights({"a1": 1.0, "a2": 1.0})
    assert res["a1"] > 1.0
    assert res["a2"] < 1.0


def test_cli_analyze_trades(tmp_path, monkeypatch):
    mem = _FakeMemory(_base_trades())

    cfg = tmp_path / "cfg.toml"
    cfg.write_text("""[agent_weights]\na1 = 1.0\na2 = 1.0\n""")

    out = tmp_path / "weights" / "out.toml"

    monkeypatch.setattr("solhunter_zero.util.install_uvloop", lambda: None)
    monkeypatch.setattr(
        trade_analyzer_module,
        "_MEMORY_CLS",
        lambda *_a, **_k: mem,
    )
    from solhunter_zero import backtest_cli

    rc = backtest_cli.main([
        "--analyze-trades",
        "--memory",
        "sqlite:///ignored",
        "-c",
        str(cfg),
        "--weights-out",
        str(out),
    ])
    assert rc == 0
    assert out.exists()

    data = {
        line.split("=")[0].strip(): float(line.split("=")[1])
        for line in out.read_text().splitlines()
    }
    assert data["a1"] > 1.0
    assert data["a2"] < 1.0


def test_roi_handles_mixed_trade_sources():
    trades = [
        SimpleNamespace(reason="agent1", direction="BUY", amount="2", price="3"),
        {"reason": "agent1", "direction": "sell", "amount": 1, "price": 6},
        SimpleNamespace(reason="agent2", direction="hold", amount=1, price=1),
        SimpleNamespace(reason="agent2", direction="sell", amount=None, price=5),
    ]
    analyzer = TradeAnalyzer(_FakeMemory(trades))
    rois = analyzer.roi_by_agent()
    assert pytest.approx(rois["agent1"], rel=1e-6) == 0.0
    assert "agent2" not in rois


def test_roi_uses_realized_metrics_when_present():
    trades = [
        {
            "reason": "agent",
            "direction": "buy",
            "realized_notional": 100,
            "realized_roi": 0.0,
        },
        {
            "reason": "agent",
            "direction": "sell",
            "realized_notional": 100,
            "realized_roi": 0.2,
        },
    ]
    analyzer = TradeAnalyzer(_FakeMemory(trades))
    rois = analyzer.roi_by_agent()
    assert pytest.approx(rois["agent"], rel=1e-6) == 0.2


def test_roi_accepts_async_memory():
    trades = [SimpleNamespace(reason="agent", direction="sell", amount=1, price=2)]
    analyzer = TradeAnalyzer(_AsyncMemory(trades))
    rois = analyzer.roi_by_agent()
    assert rois == {}


def test_analyze_trades_merges_configs(monkeypatch, tmp_path):
    mem = _FakeMemory(_base_trades())
    monkeypatch.setattr(
        trade_analyzer_module,
        "_MEMORY_CLS",
        lambda *_a, **_k: mem,
    )

    cfg1 = tmp_path / "cfg1.toml"
    cfg1.write_text("""[agent_weights]\na1 = 0.5\n""")
    cfg2 = tmp_path / "cfg2.toml"
    cfg2.write_text("""[agent_weights]\na2 = 2.0\n""")

    weights = analyze_trades(
        "sqlite:///ignored", [str(cfg1), str(cfg2)], weights_out=None, step=0.2
    )
    assert set(weights) == {"a1", "a2"}
    assert weights["a1"] != 0
    assert weights["a2"] != 0
