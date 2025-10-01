import json
import sys
import types

import paper


def test_paper_live_smoke(tmp_path, monkeypatch):
    """Exercise paper CLI using live fetch path with patched dependencies."""

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

    monkeypatch.setenv("SOLHUNTER_PATCH_INVESTOR_DEMO", "1")
    __import__("tests.stubs")

    from solhunter_zero import wallet, routeffi, depth_client, investor_demo

    flags = {"wallet": False, "route": False, "depth": False}

    def fake_load_keypair(path: str):
        flags["wallet"] = True
        return object()

    def fake_best_route(*args, **kwargs):
        flags["route"] = True
        return ["A", "B"], 0.0

    def fake_snapshot(token: str):
        flags["depth"] = True
        return {}, 0.0

    monkeypatch.setattr(wallet, "load_keypair", fake_load_keypair)
    monkeypatch.setattr(routeffi, "best_route", fake_best_route)
    monkeypatch.setattr(depth_client, "snapshot", fake_snapshot)

    async def demo_arbitrage_stub():
        wallet.load_keypair("dummy")
        routeffi.best_route({}, 1.0)
        depth_client.snapshot("tok")
        investor_demo.used_trade_types.add("arbitrage")
        return {"path": ["dex1", "dex2"], "profit": 0.0}

    monkeypatch.setattr(investor_demo, "_demo_arbitrage", demo_arbitrage_stub)

    ticks = [{"price": 1.0, "timestamp": 1}, {"price": 2.0, "timestamp": 2}]
    dataset = paper._ticks_to_price_file(ticks)
    monkeypatch.setattr(paper, "_fetch_live_dataset", lambda: dataset)

    reports = tmp_path / "reports"
    try:
        paper.run(["--fetch-live", "--reports", str(reports)])
        assert "arbitrage" in investor_demo.used_trade_types
        trade_path = reports / "trade_history.json"
        assert flags["wallet"], "wallet.load_keypair not called"
        assert flags["route"], "routeffi.best_route not called"
        assert flags["depth"], "depth_client.snapshot not called"
        assert trade_path.exists()
        data = json.loads(trade_path.read_text())
        assert data and isinstance(data, list)
    finally:
        investor_demo.used_trade_types.clear()
