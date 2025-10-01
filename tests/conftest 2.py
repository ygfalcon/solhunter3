import os
import sys
import types
import importlib.util
import importlib.machinery
import pytest

_orig_find_spec = importlib.util.find_spec


def _safe_find_spec(name, package=None):
    mod = sys.modules.get(name)
    if mod is not None and getattr(mod, "__spec__", None) is None:
        mod.__spec__ = importlib.machinery.ModuleSpec(name, None)
    try:
        return _orig_find_spec(name, package)
    except ValueError:
        return None


importlib.util.find_spec = _safe_find_spec
# Install stubs for optional heavy dependencies before importing project modules
_stub_path = os.path.join(os.path.dirname(__file__), "stubs.py")
_spec = importlib.util.spec_from_file_location("stubs", _stub_path)
_stubs = importlib.util.module_from_spec(_spec)
assert _spec and _spec.loader
_spec.loader.exec_module(_stubs)
_stubs.install_stubs()


if importlib.util.find_spec("aiohttp") is None:
    aiohttp_mod = types.ModuleType("aiohttp")
    aiohttp_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp", None)
    aiohttp_mod.ClientSession = object
    aiohttp_mod.TCPConnector = object

    web_mod = types.ModuleType("aiohttp.web")
    web_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp.web", None)

    class Application:
        pass

    def json_response(data=None, status=200):
        return {"status": status, "data": data}

    web_mod.Application = Application
    web_mod.json_response = json_response

    aiohttp_mod.web = web_mod
    sys.modules["aiohttp.web"] = web_mod
    sys.modules["aiohttp"] = aiohttp_mod

# Stub other optional dependencies when unavailable
if importlib.util.find_spec("transformers") is None:
    trans_mod = types.ModuleType("transformers")
    trans_mod.__spec__ = importlib.machinery.ModuleSpec("transformers", None)
    trans_mod.pipeline = lambda *a, **k: lambda *a2, **k2: None
    sys.modules["transformers"] = trans_mod

if importlib.util.find_spec("sentence_transformers") is None:
    st_mod = types.ModuleType("sentence_transformers")
    st_mod.__spec__ = importlib.machinery.ModuleSpec("sentence_transformers", None)
    sys.modules.setdefault("sentence_transformers", st_mod)


try:
    _gp_spec = importlib.util.find_spec("google.protobuf")
except ModuleNotFoundError:
    _gp_spec = None
if _gp_spec is None:
    protobuf = types.ModuleType("protobuf")
    descriptor = types.ModuleType("descriptor")
    descriptor_pool = types.ModuleType("descriptor_pool")
    symbol_database = types.ModuleType("symbol_database")
    symbol_database.Default = lambda: object()
    internal = types.ModuleType("internal")
    internal.builder = types.ModuleType("builder")
    runtime_version = types.ModuleType("runtime_version")
    runtime_version.ValidateProtobufRuntimeVersion = lambda *a, **k: None
    protobuf.descriptor = descriptor
    protobuf.descriptor_pool = descriptor_pool
    protobuf.symbol_database = symbol_database
    protobuf.internal = internal
    protobuf.runtime_version = runtime_version
    try:
        import google
    except ModuleNotFoundError:
        google = types.ModuleType("google")
        google.__path__ = []
        sys.modules.setdefault("google", google)
    google.protobuf = protobuf
    sys.modules.setdefault("google.protobuf", protobuf)
    sys.modules.setdefault("google.protobuf.descriptor", descriptor)
    sys.modules.setdefault("google.protobuf.descriptor_pool", descriptor_pool)
    sys.modules.setdefault("google.protobuf.symbol_database", symbol_database)
    sys.modules.setdefault("google.protobuf.internal", internal)
    sys.modules.setdefault("google.protobuf.internal.builder", internal.builder)
    sys.modules.setdefault("google.protobuf.runtime_version", runtime_version)

event_pb2 = types.ModuleType("event_pb2")


class Message:
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)

    def SerializeToString(self) -> bytes:
        import json

        def encode(val):
            if isinstance(val, Message):
                return {k: encode(v) for k, v in val.__dict__.items()}
            if isinstance(val, (list, tuple)):
                return [encode(v) for v in val]
            return val

        return json.dumps({k: encode(v) for k, v in self.__dict__.items()}).encode()

    def ParseFromString(self, data: bytes) -> None:
        import json

        def decode(val):
            if isinstance(val, dict):
                msg = Message()
                for kk, vv in val.items():
                    setattr(msg, kk, decode(vv))
                return msg
            if isinstance(val, list):
                return [decode(v) for v in val]
            return val

        obj = json.loads(data)
        for k, v in obj.items():
            setattr(self, k, decode(v))


for name in [
    "ActionExecuted",
    "WeightsUpdated",
    "RLWeights",
    "RLCheckpoint",
    "PortfolioUpdated",
    "TokenInfo",
    "TokenAgg",
    "DepthUpdate",
    "DepthDiff",
    "DepthServiceStatus",
    "Heartbeat",
    "TradeLogged",
    "RLMetrics",
    "PriceUpdate",
    "SystemMetrics",
    "RouteRequest",
    "RouteResponse",
    "SystemMetricsCombined",
    "DoubleList",
    "RiskMetrics",
    "RiskUpdated",
    "RemoteSystemMetrics",
    "TokenDiscovered",
    "MemorySyncRequest",
    "MemorySyncResponse",
    "PendingSwap",
    "ConfigUpdated",
    "Event",
]:
    setattr(event_pb2, name, type(name, (Message,), {}))

sys.modules.setdefault("solhunter_zero.event_pb2", event_pb2)
sys.modules.setdefault("event_pb2", event_pb2)

if importlib.util.find_spec("solders") is None:
    solders_mod = types.ModuleType("solders")
    solders_mod.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    sys.modules["solders"] = solders_mod
    kp_mod = types.ModuleType("solders.keypair")

    class Keypair:
        def __init__(self, data: bytes | None = None):
            self._data = data or b"\x00" * 64

        @classmethod
        def from_bytes(cls, b: bytes):
            return cls(bytes(b))

        @classmethod
        def from_seed(cls, seed: bytes):
            return cls(seed.ljust(64, b"\0"))

        def to_bytes(self) -> bytes:
            return self._data

        def to_bytes_array(self) -> list[int]:
            return list(self._data)

        def sign_message(self, _msg: bytes) -> bytes:
            return b"sig"

        def pubkey(self) -> str:
            return "0" * 32

    kp_mod.Keypair = Keypair
    kp_mod.__spec__ = importlib.machinery.ModuleSpec("solders.keypair", None)
    sys.modules["solders.keypair"] = kp_mod
    for name in ["pubkey", "hash", "message", "transaction", "instruction", "signature"]:
        mod = types.ModuleType(f"solders.{name}")
        mod.__spec__ = importlib.machinery.ModuleSpec(f"solders.{name}", None)
        sys.modules.setdefault(f"solders.{name}", mod)
    sys.modules["solders.pubkey"].Pubkey = object
    sys.modules["solders.hash"].Hash = object
    sys.modules["solders.message"].MessageV0 = object
    sys.modules["solders.transaction"].VersionedTransaction = type("VersionedTransaction", (), {"populate": staticmethod(lambda *a, **k: object())})
    sys.modules["solders.instruction"].Instruction = object
    sys.modules["solders.instruction"].AccountMeta = object
    sys.modules["solders.signature"].Signature = type("Signature", (), {"default": staticmethod(lambda: object())})

if importlib.util.find_spec("solana") is None:
    sol_mod = types.ModuleType("solana")
    sol_mod.__spec__ = importlib.machinery.ModuleSpec("solana", None)
    sys.modules["solana"] = sol_mod
    pub_mod = types.ModuleType("solana.publickey")
    pub_mod.__spec__ = importlib.machinery.ModuleSpec("solana.publickey", None)

    class PublicKey:
        def __init__(self, value: str | None = None):
            self.value = value

        @staticmethod
        def default():
            return PublicKey("0" * 32)

        @staticmethod
        def from_string(val: str):
            return PublicKey(val)

    pub_mod.PublicKey = PublicKey
    sys.modules["solana.publickey"] = pub_mod
    rpc_mod = types.ModuleType("solana.rpc")
    rpc_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc", None)
    sys.modules.setdefault("solana.rpc", rpc_mod)
    sys.modules.setdefault("solana.rpc.api", types.SimpleNamespace(Client=object, __spec__=importlib.machinery.ModuleSpec("solana.rpc.api", None)))
    sys.modules.setdefault("solana.rpc.async_api", types.SimpleNamespace(AsyncClient=object, __spec__=importlib.machinery.ModuleSpec("solana.rpc.async_api", None)))
    ws_mod = types.ModuleType("solana.rpc.websocket_api")
    ws_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc.websocket_api", None)
    ws_mod.connect = lambda *a, **k: None
    ws_mod.RpcTransactionLogsFilterMentions = object
    sys.modules.setdefault("solana.rpc.websocket_api", ws_mod)


import pytest
import asyncio

from solhunter_zero.http import close_session


def pytest_addoption(parser):
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="run tests marked as slow",
    )


@pytest.fixture
def dummy_mem():
    calls: dict[str, object] = {}

    class DummyMem:
        def __init__(self, *a, **k):
            calls["mem_init"] = True
            self.trade: dict | None = None

        async def log_trade(self, **kwargs):
            self.trade = kwargs

        async def list_trades(self, token: str):
            return [self.trade] if self.trade else []

        def log_var(self, value: float) -> None:
            calls["mem_log_var"] = value

        async def close(self) -> None:  # pragma: no cover - simple stub
            calls["mem_closed"] = True

    DummyMem.calls = calls
    return DummyMem


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        return

    skip_slow = pytest.mark.skip(reason="slow test: pass --runslow to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


@pytest.fixture(autouse=True)
def _close_http_session():
    yield
    asyncio.run(close_session())


import solhunter_zero.event_bus as event_bus


@pytest.fixture(autouse=True)
def _reset_event_bus():
    """Ensure a clean event bus for each test."""
    event_bus.reset()
    yield
    event_bus.reset()


@pytest.fixture
def shared_prices() -> list[float]:
    """Deterministic price series used by both demo paths.

    The paper trading CLI and the investor demo share this fixture so that the
    underlying trading engine receives identical inputs in tests.
    """
    return [100.0, 90.0, 95.0, 105.0]
