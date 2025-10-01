import json
import io
import paper


def test_fetch_live_dataset_success(tmp_path, monkeypatch):
    """_fetch_live_dataset returns a file when urlopen provides prices."""
    payload = {"prices": [[1000, 1.0], [2000, 2.0]]}

    def fake_urlopen(url, timeout=10):
        return io.BytesIO(json.dumps(payload).encode())

    monkeypatch.setattr(paper, "urlopen", fake_urlopen)

    dataset = paper._fetch_live_dataset()
    assert dataset and dataset.exists()
    data = json.loads(dataset.read_text())
    assert data and all("date" in d and "price" in d for d in data)


def test_fetch_live_dataset_failure_fallback(tmp_path, monkeypatch):
    """When fetching fails paper.run uses sample ticks and writes reports."""
    def fake_urlopen(url, timeout=10):
        raise OSError("network down")

    monkeypatch.setattr(paper, "urlopen", fake_urlopen)
    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")

    # Track dataset produced from sample ticks
    captured = {}
    orig = paper._ticks_to_price_file

    def capture_ticks(ticks):
        path = orig(ticks)
        captured["path"] = path
        return path

    monkeypatch.setattr(paper, "_ticks_to_price_file", capture_ticks)

    assert paper._fetch_live_dataset() is None

    reports = tmp_path / "reports"
    paper.run(["--fetch-live", "--reports", str(reports)])

    dataset = captured.get("path")
    assert dataset and dataset.exists()
    data = json.loads(dataset.read_text())
    assert data and all("date" in d and "price" in d for d in data)

    trade_path = reports / "trade_history.json"
    assert trade_path.exists()
    trades = json.loads(trade_path.read_text())
    for t in trades:
        t.setdefault("side", t.get("action"))
        t.setdefault("amount", t.get("capital"))
        assert {"token", "side", "amount", "price"} <= t.keys()
