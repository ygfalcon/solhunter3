import json

from solhunter_zero import investor_demo


def test_trade_logging_without_sqlalchemy(tmp_path):
    """Investor demo logs trades without SQLAlchemy present."""
    # Simulate SQLAlchemy not being installed
    investor_demo.Memory = None

    investor_demo.main(["--reports", str(tmp_path)])

    trade_json = tmp_path / "trade_history.json"
    assert trade_json.is_file()

    history = json.loads(trade_json.read_text())
    assert isinstance(history, list) and history
    first = history[0]
    assert {"strategy", "action", "price"} <= first.keys()
