from solhunter_zero.memory import Memory
from solhunter_zero.trade_analyzer import TradeAnalyzer
from solhunter_zero.util import run_coro

_async_list_trades = Memory.list_trades


def _list_trades_sync(self, *args, **kwargs):
    return run_coro(_async_list_trades(self, *args, **kwargs))


Memory.list_trades = _list_trades_sync  # type: ignore[assignment]


def _setup_memory(path):
    mem = Memory(f"sqlite:///{path}")
    run_coro(mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1"))
    run_coro(mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a1"))
    run_coro(mem.log_trade(token="tok", direction="buy", amount=1, price=2, reason="a2"))
    run_coro(mem.log_trade(token="tok", direction="sell", amount=1, price=1, reason="a2"))
    return mem


def test_trade_analyzer_recommend_weights(tmp_path):
    db = tmp_path / "m.db"
    mem = _setup_memory(db)
    analyzer = TradeAnalyzer(mem)
    res = analyzer.recommend_weights({"a1": 1.0, "a2": 1.0})
    assert res["a1"] > 1.0
    assert res["a2"] < 1.0


def test_cli_analyze_trades(tmp_path, monkeypatch):
    db = tmp_path / "m.db"
    mem = _setup_memory(db)

    cfg = tmp_path / "cfg.toml"
    cfg.write_text("""[agent_weights]\na1 = 1.0\na2 = 1.0\n""")

    out = tmp_path / "out.toml"

    # Avoid uvloop side effects during tests
    monkeypatch.setattr("solhunter_zero.util.install_uvloop", lambda: None)
    monkeypatch.setattr("solhunter_zero.trade_analyzer.Memory", lambda *_a, **_k: mem)
    from solhunter_zero import backtest_cli

    rc = backtest_cli.main([
        "--analyze-trades",
        "--memory",
        f"sqlite:///{db}",
        "-c",
        str(cfg),
        "--weights-out",
        str(out),
    ])
    assert rc == 0
    assert out.exists()

    data = {line.split("=")[0].strip(): float(line.split("=")[1]) for line in out.read_text().splitlines()}
    assert data["a1"] > 1.0
    assert data["a2"] < 1.0
