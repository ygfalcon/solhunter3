from __future__ import annotations

import asyncio
import sys
import types

import pytest


def _noop(*_args, **_kwargs):
    return None


def _install_sqlalchemy_stub() -> None:
    if "sqlalchemy" in sys.modules:
        return

    class _SQLAlchemyModule(types.ModuleType):
        def __getattr__(self, name: str):  # type: ignore[override]
            if name == "event":
                return types.SimpleNamespace(listens_for=lambda *a, **k: (lambda func: func))
            return _noop

    stub = _SQLAlchemyModule("sqlalchemy")
    stub.__path__ = []  # type: ignore[attr-defined]
    event_module = types.ModuleType("sqlalchemy.event")
    event_module.listens_for = lambda *a, **k: (lambda func: func)
    sys.modules["sqlalchemy.event"] = event_module
    sys.modules["sqlalchemy"] = stub

    orm_stub = types.ModuleType("sqlalchemy.orm")
    orm_stub.declarative_base = lambda *a, **k: type("Base", (), {})
    orm_stub.sessionmaker = lambda *a, **k: lambda **_kw: None
    sys.modules["sqlalchemy.orm"] = orm_stub

    ext_stub = types.ModuleType("sqlalchemy.ext")
    sys.modules["sqlalchemy.ext"] = ext_stub
    async_stub = types.ModuleType("sqlalchemy.ext.asyncio")
    async_stub.AsyncSession = object
    async_stub.async_sessionmaker = lambda *a, **k: None
    async_stub.create_async_engine = lambda *a, **k: None
    sys.modules["sqlalchemy.ext.asyncio"] = async_stub


def _install_base58_stub() -> None:
    if "base58" in sys.modules:
        return
    module = types.ModuleType("base58")
    module.b58decode = lambda *a, **k: b""
    module.b58encode = lambda *a, **k: b""
    sys.modules["base58"] = module


def _install_jsonschema_stub() -> None:
    if "jsonschema" in sys.modules:
        return

    module = types.ModuleType("jsonschema")

    class _ValidationError(Exception):
        def __init__(self, message: str = "", *, instance=None, schema=None) -> None:
            super().__init__(message)
            self.message = message
            self.instance = instance
            self.schema = schema

    class _Validator:
        def __init__(self, schema):
            self.schema = schema

        def validate(self, _payload):
            return True

        @staticmethod
        def check_schema(_schema):
            return True

    module.ValidationError = _ValidationError
    module.Draft7Validator = _Validator
    module.Draft202012Validator = _Validator
    module.validate = _noop

    exceptions = types.ModuleType("jsonschema.exceptions")
    exceptions.ValidationError = _ValidationError
    module.exceptions = exceptions

    sys.modules["jsonschema"] = module
    sys.modules["jsonschema.exceptions"] = exceptions


_install_sqlalchemy_stub()
_install_base58_stub()
_install_jsonschema_stub()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


from solhunter_zero import event_bus
from solhunter_zero.runtime.runtime_wiring import RuntimeEventCollectors
from solhunter_zero.runtime.schema_adapters import read_golden, read_ohlcv
from solhunter_zero.runtime.trading_runtime import TradingRuntime
from solhunter_zero.schemas import validate_message


def _base_ohlcv_payload() -> dict[str, object]:
    return {
        "mint": "MINT",
        "open": "1.00",
        "high": "1.10",
        "low": "0.90",
        "close": "1.05",
        "volume": "250.0",
        "volume_usd": "250.0",
        "buyers": 4,
        "sellers": 3,
        "trades": 12,
        "zret": "0.4",
        "zvol": "0.2",
        "schema_version": "2.0",
        "asof_close": 1_704_067_500.0,
    }


def _nested_golden_payload() -> dict[str, object]:
    now = 1_704_067_500.0
    return {
        "mint": "MINT",
        "hash": "abc123",
        "content_hash": "abc123",
        "schema_version": "2.0",
        "asof": now,
        "px_mid_usd": 1.05,
        "liq_depth_1pct_usd": 2500.0,
        "px": {"mid_usd": 1.05, "spread_bps": 22.0, "ts": now},
        "liq": {"depth_pct": {"1": 2500.0}, "asof": now, "bands": []},
        "meta": {"source": "unit-test"},
        "metrics": {"latency_ms": 120.0},
        "ohlcv5m": {
            "mint": "MINT",
            "o": 1.0,
            "h": 1.1,
            "l": 0.9,
            "c": 1.05,
            "close": 1.05,
            "vol_usd": 250.0,
            "volume": 250.0,
            "volume_usd": 250.0,
            "vol_base": 25.0,
            "volume_base": 25.0,
            "trades": 12,
            "buyers": 4,
            "sellers": 3,
            "zret": 0.4,
            "zvol": 0.2,
            "asof_close": now,
            "schema_version": "2.0",
            "content_hash": "abc123-ohlcv",
        },
    }


def test_schema_adapter_shape_agnostic() -> None:
    legacy_msg = _base_ohlcv_payload()
    legacy_msg.pop("schema_version")
    legacy_msg.pop("volume_usd")
    v1_normalised = read_ohlcv(dict(legacy_msg), reader="test")

    modern_msg = {
        "mint": "MINT",
        "o": 1.0,
        "h": 1.1,
        "l": 0.9,
        "c": 1.05,
        "vol_usd": 250.0,
        "vol_base": 25.0,
        "trades": 12,
        "buyers": 4,
        "zret": 0.4,
        "zvol": 0.2,
        "schema_version": "2.0",
        "content_hash": "hash-ohlcv",
        "asof_close": "2024-01-01T00:05:00Z",
    }
    v2_normalised = read_ohlcv(dict(modern_msg), reader="test")

    for key in ("open", "high", "low", "close", "volume_usd", "buyers", "trades"):
        assert v1_normalised[key] == pytest.approx(v2_normalised[key])

    legacy_golden = {
        "mint": "MINT",
        "hash": "abc123",
        "px": 1.05,
        "liq": 2500.0,
        "ohlcv5m": legacy_msg,
        "asof": "2024-01-01T00:05:00Z",
    }
    nested_golden = _nested_golden_payload()
    v1_golden = read_golden(dict(legacy_golden), reader="test")
    v2_golden = read_golden(dict(nested_golden), reader="test")

    assert v1_golden["mid_usd"] == pytest.approx(v2_golden["mid_usd"])
    assert v1_golden["depth_1pct_usd"] == pytest.approx(v2_golden["depth_1pct_usd"])
    assert v1_golden["ohlcv"]["close"] == pytest.approx(v2_golden["ohlcv"]["close"])


def test_read_ohlcv_maps_v2_fields() -> None:
    payload = {
        "mint": "MINT",
        "o": 1.0,
        "h": 1.1,
        "l": 0.9,
        "c": 1.05,
        "vol_usd": 250.0,
        "trades": 12,
        "buyers": 4,
        "schema_version": "2.0",
        "content_hash": "hash",
        "asof_close": "2024-01-01T00:05:00Z",
    }
    normalised = read_ohlcv(payload, reader="test")
    assert normalised["close"] == pytest.approx(1.05)
    assert normalised["volume_usd"] == pytest.approx(250.0)
    assert normalised["volume_base"] is None


@pytest.mark.anyio("asyncio")
async def test_golden_v2_payload_propagates(monkeypatch):
    event_bus.reset()
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()
    runtime._subscribe_to_events()
    collectors = RuntimeEventCollectors()
    collectors.start()

    try:
        payload = _nested_golden_payload()
        validated = validate_message("x:mint.golden", payload)
        assert validated["px_mid_usd"] == pytest.approx(1.05)
        event_bus.publish("x:mint.golden", payload)
        await asyncio.sleep(0)

        assert runtime._golden_snapshots, "runtime should retain raw snapshots"
        panel = runtime._collect_golden_panel()
        assert panel["snapshots"], "runtime panel should have data"
        entry = panel["snapshots"][0]
        assert entry["px"] == pytest.approx(1.05)
        assert entry["liq"] == pytest.approx(2500.0)

        snapshot = collectors.golden_snapshot()
        assert snapshot["snapshots"], "wiring collector should have data"
        wiring_entry = snapshot["snapshots"][0]
        assert wiring_entry["px"] == pytest.approx(1.05)
        assert wiring_entry["liq"] == pytest.approx(2500.0)

    finally:
        for topic, handler in runtime._subscriptions:
            event_bus.unsubscribe(topic, handler)
        runtime._subscriptions.clear()
        collectors.stop()
