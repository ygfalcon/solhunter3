import importlib
import sys
import types
import pytest

pytest.importorskip("google.protobuf")

if importlib.util.find_spec("solana") is None:
    sys.modules.setdefault("solana", types.ModuleType("solana"))
    sys.modules.setdefault("solana.rpc", types.ModuleType("rpc"))
    sys.modules.setdefault("solana.rpc.api", types.SimpleNamespace(Client=object))
    sys.modules.setdefault(
        "solana.rpc.async_api", types.SimpleNamespace(AsyncClient=object)
    )
    sys.modules.setdefault(
        "solana.rpc.websocket_api",
        types.SimpleNamespace(connect=lambda *a, **k: None, RpcTransactionLogsFilterMentions=object),
    )
    sys.modules.setdefault(
        "solana.publickey", types.SimpleNamespace(PublicKey=lambda v: v)
    )
if importlib.util.find_spec("solders") is None:
    sys.modules.setdefault("solders", types.ModuleType("solders"))
    sys.modules["solders.keypair"] = types.SimpleNamespace(Keypair=object)
    sys.modules["solders.pubkey"] = types.SimpleNamespace(Pubkey=object)
    sys.modules["solders.hash"] = types.SimpleNamespace(Hash=object)
    sys.modules["solders.message"] = types.SimpleNamespace(MessageV0=object)
    sys.modules["solders.transaction"] = types.SimpleNamespace(VersionedTransaction=object)
    sys.modules["solders.instruction"] = types.SimpleNamespace(Instruction=object, AccountMeta=object)
try:
    importlib.import_module("google.protobuf")
except Exception:
    google = types.ModuleType("google")
    google.__path__ = []
    protobuf = types.ModuleType("protobuf")
    descriptor = types.ModuleType("descriptor")
    descriptor_pool = types.ModuleType("descriptor_pool")
    symbol_database = types.ModuleType("symbol_database")
    symbol_database.Default = lambda: object()
    internal = types.ModuleType("internal")
    internal.builder = types.ModuleType("builder")
    protobuf.descriptor = descriptor
    protobuf.descriptor_pool = descriptor_pool
    protobuf.symbol_database = symbol_database
    protobuf.internal = internal
    protobuf.runtime_version = types.SimpleNamespace(ValidateProtobufRuntimeVersion=lambda *a, **k: None)
    google.protobuf = protobuf
    sys.modules.setdefault("google", google)
    sys.modules.setdefault("google.protobuf", protobuf)
    sys.modules.setdefault("google.protobuf.descriptor", descriptor)
    sys.modules.setdefault("google.protobuf.descriptor_pool", descriptor_pool)
    sys.modules.setdefault("google.protobuf.symbol_database", symbol_database)
    sys.modules.setdefault("google.protobuf.internal", internal)
    sys.modules.setdefault("google.protobuf.internal.builder", internal.builder)

import solhunter_zero.arbitrage as arb

def test_best_route_parallel_called(monkeypatch):
    monkeypatch.setattr(arb._routeffi, "available", lambda: True)
    monkeypatch.setattr(arb, "USE_FFI_ROUTE", True)
    called = {}

    def fake_parallel(*a, **k):
        called["parallel"] = True
        return ["dex1", "dex2"], 0.0

    monkeypatch.setattr(arb._routeffi, "best_route_parallel", fake_parallel)
    monkeypatch.setattr(arb._routeffi, "best_route", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("serial")))
    monkeypatch.setattr(arb._routeffi, "parallel_enabled", lambda: True)

    prices = {"dex1": 1.0, "dex2": 1.1}
    arb._best_route(prices, 1.0)
    assert called.get("parallel", False)

