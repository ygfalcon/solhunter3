import json
import paper


def test_paper_trade_logging_without_sqlalchemy(tmp_path, monkeypatch):
    """Paper trading logs trades when SQLAlchemy is absent."""
    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    import tests.stubs  # installs lightweight stubs
    from solhunter_zero import investor_demo

    investor_demo.Memory = None
    paper.run(["--reports", str(tmp_path)])
    trade_json = tmp_path / "trade_history.json"
    assert trade_json.is_file()
    history = json.loads(trade_json.read_text())
    assert isinstance(history, list) and history
    first = history[0]
    assert {"strategy", "action", "price"} <= first.keys()
