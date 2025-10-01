import json
import sys
import types
from pathlib import Path

import paper


def test_paper_test_flag(tmp_path, monkeypatch):
    # Stub heavy optional dependencies
    fake_crypto = types.ModuleType("cryptography")
    fake_fernet = types.SimpleNamespace(Fernet=object, InvalidToken=Exception)
    fake_crypto.fernet = fake_fernet
    sys.modules.setdefault("cryptography", fake_crypto)
    sys.modules.setdefault("cryptography.fernet", fake_fernet)

    class _BaseModel:  # noqa: D401 - simple stub
        pass

    class _AnyUrl(str):
        pass

    class _ValidationError(Exception):
        pass

    def field_validator(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    def model_validator(*args, **kwargs):
        def decorator(func):
            return func

        return decorator

    pydantic_stub = types.SimpleNamespace(
        BaseModel=_BaseModel,
        AnyUrl=_AnyUrl,
        ValidationError=_ValidationError,
        field_validator=field_validator,
        model_validator=model_validator,
    )
    sys.modules.setdefault("pydantic", pydantic_stub)

    from solhunter_zero import wallet, routeffi, depth_client, investor_demo

    calls = {"wallet": False}
    route_args = ()
    snapshot_token = None

    def fake_load_keypair(path: str):
        calls["wallet"] = True
        return object()

    async def fake_best_route(*args, **kwargs):
        nonlocal route_args
        route_args = args
        return {"path": ["A", "B"]}

    async def fake_snapshot(token: str):
        nonlocal snapshot_token
        snapshot_token = token
        return {}, 0.0

    monkeypatch.setattr(wallet, "load_keypair", fake_load_keypair)
    monkeypatch.setattr(routeffi, "best_route", fake_best_route)
    monkeypatch.setattr(depth_client, "snapshot", fake_snapshot)

    ticks = [{"price": 1.0, "timestamp": 1}]
    dataset = paper._ticks_to_price_file(ticks)
    monkeypatch.setattr(paper, "_fetch_live_dataset", lambda: dataset)

    monkeypatch.chdir(tmp_path)
    paper.run(["--test"])

    assert calls["wallet"], "wallet.load_keypair not called"
    assert route_args == ({}, 1.0), route_args
    assert snapshot_token == "FAKE", snapshot_token

    trade_path = Path("reports/trade_history.json")
    assert trade_path.exists(), "trade history not written"
    data = json.loads(trade_path.read_text())
    expected_len = 2 * len(investor_demo.DEFAULT_STRATEGIES)
    assert len(data) == expected_len
    for name, _ in investor_demo.DEFAULT_STRATEGIES:
        buys = [t for t in data if t["token"] == name and t["side"] == "buy"]
        sells = [t for t in data if t["token"] == name and t["side"] == "sell"]
        assert len(buys) == 1 and len(sells) == 1

