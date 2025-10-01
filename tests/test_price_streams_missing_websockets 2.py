import json
import sys


def test_price_streams_missing_websockets_warns(tmp_path, monkeypatch, capsys):
    """Live stream request warns and proceeds when websockets is missing."""
    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    import tests.stubs  # installs lightweight stubs and patches demo.Memory
    from solhunter_zero import investor_demo

    # Remove websockets stubs to simulate missing dependency
    monkeypatch.setitem(sys.modules, "websockets", None)
    monkeypatch.setitem(sys.modules, "websockets.legacy", None)
    monkeypatch.setitem(sys.modules, "websockets.legacy.client", None)
    monkeypatch.delitem(sys.modules, "solhunter_zero.price_stream_manager", raising=False)

    investor_demo.main(
        ["--reports", str(tmp_path), "--price-streams", "orca=ws://localhost:1234"]
    )
    err = capsys.readouterr().err
    assert (
        "websockets package is required for --price-streams; skipping live price streams"
        in err
    )
    trade_json = tmp_path / "trade_history.json"
    assert trade_json.is_file()
    history = json.loads(trade_json.read_text())
    assert isinstance(history, list) and history
