import json
import sys
import types

import paper

def test_paper_live_flow(tmp_path, monkeypatch):
    fake_crypto = types.ModuleType("cryptography")
    fake_fernet = types.SimpleNamespace(Fernet=object, InvalidToken=Exception)
    fake_crypto.fernet = fake_fernet
    sys.modules.setdefault("cryptography", fake_crypto)
    sys.modules.setdefault("cryptography.fernet", fake_fernet)

    class _BaseModel:
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

    flags = {"wallet": False}
    route_args = ()
    snapshot_token = None

    def fake_load_keypair(path: str):
        flags["wallet"] = True
        return object()

    async def fake_best_route(*args, **kwargs):
        nonlocal route_args
        route_args = args
        return {"path": ["A", "B"], "amount": kwargs.get("amount", 0)}

    async def fake_snapshot(token: str):
        nonlocal snapshot_token
        snapshot_token = token
        return {}, 0.0

    monkeypatch.setattr(wallet, "load_keypair", fake_load_keypair)
    monkeypatch.setattr(routeffi, "best_route", fake_best_route)
    monkeypatch.setattr(depth_client, "snapshot", fake_snapshot)

    ticks = [{"price": 1.0, "timestamp": 1}, {"price": 2.0, "timestamp": 2}]
    dataset = paper._ticks_to_price_file(ticks)
    monkeypatch.setattr(paper, "_fetch_live_dataset", lambda: dataset)

    reports = tmp_path / "reports"
    paper.run(["--live-flow", "--fetch-live", "--reports", str(reports)])

    assert flags["wallet"], "wallet.load_keypair not called"
    assert route_args == ({}, 1.0), route_args
    assert snapshot_token == "FAKE", snapshot_token

    trade_path = reports / "trade_history.json"
    assert trade_path.exists()
    data = json.loads(trade_path.read_text())
    expected_len = 2 * len(investor_demo.DEFAULT_STRATEGIES)
    assert len(data) == expected_len
    for name, _ in investor_demo.DEFAULT_STRATEGIES:
        buys = [t for t in data if t["token"] == name and t["side"] == "buy"]
        sells = [t for t in data if t["token"] == name and t["side"] == "sell"]
        assert len(buys) == 1 and len(sells) == 1

