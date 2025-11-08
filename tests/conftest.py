import json
import os
import sys
import types
from pathlib import Path
from typing import Any, Dict, Sequence
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

    abc_mod = types.ModuleType("aiohttp.abc")
    abc_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp.abc", None)

    class AbstractResolver:  # pragma: no cover - stub type
        async def close(self) -> None:
            return None

    abc_mod.AbstractResolver = AbstractResolver

    class ClientTimeout:  # pragma: no cover - stub type
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    class ClientError(Exception):  # pragma: no cover - stub type
        pass

    class ClientConnectionError(ClientError):  # pragma: no cover - stub type
        pass

    class ClientResponseError(ClientError):  # pragma: no cover - stub type
        pass

    class WSMsgType:  # pragma: no cover - stub type
        TEXT = "text"
        BINARY = "binary"
        ERROR = "error"

    resolver_mod = types.ModuleType("aiohttp.resolver")
    resolver_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp.resolver", None)

    class DefaultResolver(AbstractResolver):  # pragma: no cover - stub type
        async def resolve(self, *args, **kwargs):
            return []

    resolver_mod.DefaultResolver = DefaultResolver

    web_mod = types.ModuleType("aiohttp.web")
    web_mod.__spec__ = importlib.machinery.ModuleSpec("aiohttp.web", None)

    class Application:
        pass

    def json_response(data=None, status=200):
        return {"status": status, "data": data}

    web_mod.Application = Application
    web_mod.json_response = json_response

    aiohttp_mod.web = web_mod
    aiohttp_mod.abc = abc_mod
    aiohttp_mod.resolver = resolver_mod
    aiohttp_mod.ClientTimeout = ClientTimeout
    aiohttp_mod.ClientError = ClientError
    aiohttp_mod.ClientConnectionError = ClientConnectionError
    aiohttp_mod.ClientResponseError = ClientResponseError
    aiohttp_mod.WSMsgType = WSMsgType
    sys.modules["aiohttp.web"] = web_mod
    sys.modules["aiohttp.abc"] = abc_mod
    sys.modules["aiohttp.resolver"] = resolver_mod
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

if importlib.util.find_spec("pydantic") is None:
    pydantic_mod = types.ModuleType("pydantic")
    pydantic_mod.__spec__ = importlib.machinery.ModuleSpec("pydantic", None)

    class _ValidationError(Exception):  # pragma: no cover - stub type
        def __init__(self, message: str = "", *args: Any, **kwargs: Any) -> None:
            super().__init__(message)

    class _BaseModel:  # pragma: no cover - stub type
        def __init__(self, **kwargs: Any) -> None:
            for key, value in kwargs.items():
                setattr(self, key, value)

        def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

        def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return self.dict(*args, **kwargs)

    def _create_model(name: str, **fields: Any) -> type:  # pragma: no cover - stub
        return type(name, (object,), {})

    class _AnyUrl(str):  # pragma: no cover - stub type
        pass

    def _validator(*args: Any, **kwargs: Any):  # pragma: no cover - stub decorator
        def _wrap(func):
            return func

        return _wrap

    pydantic_mod.ValidationError = _ValidationError
    pydantic_mod.BaseModel = _BaseModel
    pydantic_mod.create_model = _create_model
    pydantic_mod.Field = lambda *a, **k: None
    pydantic_mod.AnyUrl = _AnyUrl
    pydantic_mod.field_validator = _validator
    pydantic_mod.model_validator = _validator
    pydantic_mod.ValidationInfo = object
    pydantic_mod.validator = _validator
    pydantic_mod.root_validator = _validator
    sys.modules.setdefault("pydantic", pydantic_mod)

if importlib.util.find_spec("solana") is None:
    solana_mod = types.ModuleType("solana")
    solana_mod.__spec__ = importlib.machinery.ModuleSpec("solana", None)
    solana_mod.__path__ = []  # pragma: no cover - package stub

    rpc_mod = types.ModuleType("solana.rpc")
    rpc_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc", None)

    api_mod = types.ModuleType("solana.rpc.api")
    api_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc.api", None)

    class _Client:  # pragma: no cover - stub type
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    api_mod.Client = _Client

    async_api_mod = types.ModuleType("solana.rpc.async_api")
    async_api_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc.async_api", None)

    class _AsyncClient:  # pragma: no cover - stub type
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        async def close(self) -> None:
            return None

    async_api_mod.AsyncClient = _AsyncClient

    core_mod = types.ModuleType("solana.rpc.core")
    core_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc.core", None)

    class RPCException(Exception):  # pragma: no cover - stub type
        pass

    core_mod.RPCException = RPCException

    types_mod = types.ModuleType("solana.rpc.types")
    types_mod.__spec__ = importlib.machinery.ModuleSpec("solana.rpc.types", None)

    class TokenAccountOpts:  # pragma: no cover - stub type
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

    types_mod.TokenAccountOpts = TokenAccountOpts

    rpc_mod.api = api_mod
    rpc_mod.async_api = async_api_mod
    rpc_mod.core = core_mod
    rpc_mod.types = types_mod

    solana_mod.rpc = rpc_mod

    sys.modules.setdefault("solana", solana_mod)
    sys.modules.setdefault("solana.rpc", rpc_mod)
    sys.modules.setdefault("solana.rpc.api", api_mod)
    sys.modules.setdefault("solana.rpc.async_api", async_api_mod)
    sys.modules.setdefault("solana.rpc.core", core_mod)
    sys.modules.setdefault("solana.rpc.types", types_mod)

if importlib.util.find_spec("solders") is None:
    solders_mod = types.ModuleType("solders")
    solders_mod.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    solders_mod.__path__ = []  # pragma: no cover - package stub

    pubkey_mod = types.ModuleType("solders.pubkey")
    pubkey_mod.__spec__ = importlib.machinery.ModuleSpec("solders.pubkey", None)

    class Pubkey(str):  # pragma: no cover - stub type
        @classmethod
        def from_string(cls, value: str) -> "Pubkey":
            return cls(value)

    pubkey_mod.Pubkey = Pubkey
    solders_mod.pubkey = pubkey_mod

    keypair_mod = types.ModuleType("solders.keypair")
    keypair_mod.__spec__ = importlib.machinery.ModuleSpec("solders.keypair", None)

    class Keypair:  # pragma: no cover - stub type
        @classmethod
        def from_base58_string(cls, value: str) -> "Keypair":
            return cls()

        def pubkey(self) -> Pubkey:
            return Pubkey("0" * 32)

    keypair_mod.Keypair = Keypair
    solders_mod.keypair = keypair_mod

    transaction_mod = types.ModuleType("solders.transaction")
    transaction_mod.__spec__ = importlib.machinery.ModuleSpec("solders.transaction", None)

    class VersionedTransaction:  # pragma: no cover - stub type
        pass

    transaction_mod.VersionedTransaction = VersionedTransaction
    solders_mod.transaction = transaction_mod

    sys.modules.setdefault("solders", solders_mod)
    sys.modules.setdefault("solders.pubkey", pubkey_mod)
    sys.modules.setdefault("solders.keypair", keypair_mod)
    sys.modules.setdefault("solders.transaction", transaction_mod)

if importlib.util.find_spec("cryptography") is None:
    cryptography_mod = types.ModuleType("cryptography")
    cryptography_mod.__spec__ = importlib.machinery.ModuleSpec("cryptography", None)
    cryptography_mod.__path__ = []  # pragma: no cover - package stub

    fernet_mod = types.ModuleType("cryptography.fernet")
    fernet_mod.__spec__ = importlib.machinery.ModuleSpec("cryptography.fernet", None)

    class Fernet:  # pragma: no cover - stub type
        def __init__(self, key: bytes) -> None:
            self.key = key

        def encrypt(self, data: bytes) -> bytes:
            return data

        def decrypt(self, token: bytes) -> bytes:
            return token

    class InvalidToken(Exception):  # pragma: no cover - stub type
        pass

    fernet_mod.Fernet = Fernet
    fernet_mod.InvalidToken = InvalidToken
    cryptography_mod.fernet = fernet_mod

    sys.modules.setdefault("cryptography", cryptography_mod)
    sys.modules.setdefault("cryptography.fernet", fernet_mod)

if importlib.util.find_spec("rich") is None:
    rich_mod = types.ModuleType("rich")
    rich_mod.__spec__ = importlib.machinery.ModuleSpec("rich", None)
    rich_mod.__path__ = []  # pragma: no cover - package stub

    console_mod = types.ModuleType("rich.console")
    console_mod.__spec__ = importlib.machinery.ModuleSpec("rich.console", None)

    class Console:  # pragma: no cover - stub type
        def print(self, *args: Any, **kwargs: Any) -> None:
            pass

    console_mod.Console = Console
    rich_mod.console = console_mod

    sys.modules.setdefault("rich", rich_mod)
    sys.modules.setdefault("rich.console", console_mod)


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


_CHAOS_METADATA_ENV = "CHAOS_REMEDIATION_DIR"
_DEFAULT_CHAOS_DIR = Path("artifacts/chaos")


def _normalise_node_id(node_id: str) -> str:
    safe = node_id.replace("::", "__")
    safe = safe.replace("/", "__").replace("\\", "__")
    return safe


def _coerce_steps(remediation: str | Sequence[str]) -> list[str]:
    if isinstance(remediation, str):
        return [remediation.strip()] if remediation.strip() else []
    steps: list[str] = []
    for item in remediation:
        if item is None:
            continue
        text = str(item).strip()
        if text:
            steps.append(text)
    return steps


def _unique_list(items: Sequence[str] | None) -> list[str]:
    if not items:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


@pytest.fixture
def chaos_remediator(request):
    """Record remediation guidance for chaos/health test scenarios."""

    records: list[dict[str, Any]] = []

    def record(
        *,
        component: str,
        failure: str,
        remediation: str | Sequence[str],
        detection: str | None = None,
        impact: str | None = None,
        verification: str | None = None,
        severity: str | None = None,
        notes: str | None = None,
        tags: Sequence[str] | None = None,
        links: Sequence[str] | None = None,
        owner: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if not component:
            raise ValueError("component is required")
        if not failure:
            raise ValueError("failure is required")

        steps = _coerce_steps(remediation)
        if not steps:
            raise ValueError("remediation must include at least one step")

        entry: dict[str, Any] = {
            "component": component,
            "failure": failure,
            "remediation": steps,
        }
        if detection:
            entry["detection"] = detection
        if impact:
            entry["impact"] = impact
        if verification:
            entry["verification"] = verification
        if severity:
            entry["severity"] = severity
        if notes:
            entry["notes"] = notes
        if owner:
            entry["owner"] = owner
        tags_list = _unique_list(tags)
        if tags_list:
            entry["tags"] = tags_list
        links_list = _unique_list(links)
        if links_list:
            entry["links"] = links_list
        if metadata:
            entry["metadata"] = metadata

        records.append(entry)

    yield record

    if not records:
        return

    base_dir = Path(os.getenv(_CHAOS_METADATA_ENV, _DEFAULT_CHAOS_DIR))
    base_dir.mkdir(parents=True, exist_ok=True)

    node_id = _normalise_node_id(request.node.nodeid)
    artifact_path = base_dir / f"{node_id}.json"

    module = getattr(request.node, "module", None)
    module_name = getattr(module, "__name__", None) if module else None
    try:
        markers = sorted({marker.name for marker in request.node.iter_markers()})
    except Exception:  # pragma: no cover - defensive
        markers = []

    payload: dict[str, Any] = {
        "test": request.node.nodeid,
        "module": module_name,
        "markers": markers,
        "records": records,
    }

    artifact_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


@pytest.fixture
def shared_prices() -> list[float]:
    """Deterministic price series used by both demo paths.

    The paper trading CLI and the investor demo share this fixture so that the
    underlying trading engine receives identical inputs in tests.
    """
    return [100.0, 90.0, 95.0, 105.0]
