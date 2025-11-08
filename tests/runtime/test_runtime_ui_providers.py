import asyncio
import importlib
import importlib.machinery
import math
import sys
import types
from typing import Any, Dict, List

if "pydantic" not in sys.modules:
    pydantic_stub = types.ModuleType("pydantic")

    class ValidationError(Exception):
        pass

    class BaseModel:
        def __init__(self, **data: Any) -> None:
            for key, value in data.items():
                setattr(self, key, value)

        def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
            return dict(self.__dict__)

    class AnyUrl(str):
        pass

    def _decorator(*dargs: Any, **dkwargs: Any):
        if dargs and callable(dargs[0]) and len(dargs) == 1:
            return dargs[0]

        def _wrap(func: Any) -> Any:
            return func

        return _wrap

    class ValidationInfo:  # pragma: no cover - stub
        pass

    pydantic_stub.ValidationError = ValidationError
    pydantic_stub.BaseModel = BaseModel
    pydantic_stub.AnyUrl = AnyUrl
    pydantic_stub.field_validator = _decorator
    pydantic_stub.model_validator = _decorator
    pydantic_stub.validator = _decorator
    pydantic_stub.root_validator = _decorator
    pydantic_stub.ValidationInfo = ValidationInfo
    sys.modules["pydantic"] = pydantic_stub

solana_mod = sys.modules.get("solana")
if solana_mod is None:
    solana_mod = types.ModuleType("solana")
    solana_spec = importlib.machinery.ModuleSpec("solana", None)
    solana_spec.submodule_search_locations = []
    solana_mod.__spec__ = solana_spec
    solana_mod.__path__ = []  # type: ignore[attr-defined]
    sys.modules["solana"] = solana_mod

rpc_mod = sys.modules.get("solana.rpc")
if rpc_mod is None:
    rpc_mod = types.ModuleType("solana.rpc")
    rpc_spec = importlib.machinery.ModuleSpec("solana.rpc", None)
    rpc_spec.submodule_search_locations = []
    rpc_mod.__spec__ = rpc_spec
    rpc_mod.__path__ = []  # type: ignore[attr-defined]
    rpc_mod.__package__ = "solana"
    sys.modules["solana.rpc"] = rpc_mod

core_mod = sys.modules.get("solana.rpc.core")
if core_mod is None:
    core_mod = types.ModuleType("solana.rpc.core")
    core_spec = importlib.machinery.ModuleSpec("solana.rpc.core", None)
    core_spec.submodule_search_locations = []
    core_mod.__spec__ = core_spec
    core_mod.__path__ = []  # type: ignore[attr-defined]
    core_mod.__package__ = "solana.rpc"
    sys.modules["solana.rpc.core"] = core_mod

if not hasattr(rpc_mod, "core"):
    rpc_mod.core = core_mod

if not hasattr(solana_mod, "rpc"):
    solana_mod.rpc = rpc_mod

if not hasattr(core_mod, "RPCException"):
    class RPCException(Exception):
        pass

    core_mod.RPCException = RPCException

if "solhunter_zero.agent_manager" not in sys.modules:
    agent_manager_mod = types.ModuleType("solhunter_zero.agent_manager")

    class AgentManager:
        def __init__(self) -> None:
            self.weights: Dict[str, Any] = {}
            self.agents: List[Any] = []
            self._rl_window_sec = 0.4

        @classmethod
        def from_config(cls, _cfg: Dict[str, Any] | None) -> "AgentManager":
            return cls()

        @classmethod
        def from_default(cls) -> "AgentManager":
            return cls()

        def set_rl_disabled(self, *_args: Any, **_kwargs: Any) -> None:
            return None

        def set_rl_weights(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    agent_manager_mod.AgentManager = AgentManager
    sys.modules["solhunter_zero.agent_manager"] = agent_manager_mod

if "solhunter_zero.onchain_metrics" not in sys.modules:
    onchain_mod = types.ModuleType("solhunter_zero.onchain_metrics")

    async def fetch_dex_metrics_async(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        return {}

    async def _helius_price_overview(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        return {}

    async def _birdeye_price_overview(*_args: Any, **_kwargs: Any) -> Dict[str, Any]:
        return {}

    onchain_mod.fetch_dex_metrics_async = fetch_dex_metrics_async
    onchain_mod._helius_price_overview = _helius_price_overview
    onchain_mod._birdeye_price_overview = _birdeye_price_overview
    sys.modules["solhunter_zero.onchain_metrics"] = onchain_mod

import pytest

import solhunter_zero.ui as ui
from solhunter_zero.portfolio import Portfolio, Position
from solhunter_zero.runtime.trading_runtime import TradingRuntime


def _build_runtime(monkeypatch: pytest.MonkeyPatch) -> TradingRuntime:
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()
    portfolio = Portfolio(path=None)
    portfolio.balances = {
        "MINT_V": Position(
            token="MINT_V",
            amount=1.5,
            entry_price=2.0,
            last_mark=2.4,
            realized_pnl=1.25,
            unrealized_pnl=0.6,
            breakeven_price=1.8,
            breakeven_bps=25.0,
            lifecycle="open",
            attribution={"entries": ["seed"], "exits": []},
        ),
        "MINT_L": Position(
            token="MINT_L",
            amount=-0.75,
            entry_price=1.8,
            last_mark=1.5,
            realized_pnl=-0.2,
            unrealized_pnl=0.225,
            breakeven_price=1.6,
            breakeven_bps=12.0,
            lifecycle="open",
            attribution={"entries": ["hedge"], "exits": []},
        ),
    }
    portfolio.risk_metrics = {
        "covariance": 0.12,
        "cov_matrix": _DummyArray([[1.0, 0.2], [0.2, 1.5]]),
        "portfolio_cvar": 0.05,
    }
    runtime.portfolio = portfolio

    runtime._virtual_fills.clear()
    runtime._live_fills.clear()
    runtime._virtual_fills.appendleft(
        {
            "mint": "MINT_V",
            "side": "buy",
            "qty_base": "1.5",
            "price_usd": 2.0,
            "fees_usd": "0.02",
            "route": "simulator",
            "order_id": "virt-001",
            "ts": 1700000000.0,
            "_received": 1700000000.5,
            "meta": {"strategy": "shadow"},
        }
    )
    runtime._live_fills.appendleft(
        {
            "mint": "MINT_L",
            "side": "sell",
            "qty_base": -0.5,
            "price_usd": "1.7",
            "fees_usd": 0.01,
            "venue": "dex",
            "order_id": "live-001",
            "timestamp": 1700000050.0,
            "_received": 1700000050.4,
        }
    )
    asyncio.run(runtime._start_ui())
    return runtime


def _sorted_by_timestamp(entries: List[Dict[str, Any]]) -> bool:
    last = math.inf
    for entry in entries:
        ts = entry.get("timestamp") or entry.get("received_at") or 0.0
        if ts > last:
            return False
        last = ts
    return True


def test_execution_and_portfolio_endpoints(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = _build_runtime(monkeypatch)
    app = ui.create_app(state=runtime.ui_state)
    client = app.test_client()

    fills_resp = client.get("/api/execution/fills?limit=5")
    assert fills_resp.status_code == 200
    fills = fills_resp.get_json()
    assert isinstance(fills, list) and fills
    assert _sorted_by_timestamp(fills)
    sources = {fill.get("source") for fill in fills}
    assert {"virtual", "live"}.issubset(sources)
    for fill in fills:
        assert "mint" in fill
        assert "qty_base" in fill and isinstance(fill["qty_base"], (int, float))

    positions_resp = client.get("/api/portfolio/positions")
    assert positions_resp.status_code == 200
    positions = positions_resp.get_json()
    assert {pos["mint"] for pos in positions} == {"MINT_V", "MINT_L"}
    long = next(pos for pos in positions if pos["mint"] == "MINT_V")
    assert long["side"] == "long"
    assert pytest.approx(long["notional_usd"], rel=1e-6) == 1.5 * 2.4

    pnl_resp = client.get("/api/portfolio/pnl?window=1h")
    assert pnl_resp.status_code == 200
    pnl = pnl_resp.get_json()
    assert pnl["window"] == "1h"
    assert pnl["positions"] == 2
    expected_realized = 1.25 - 0.2
    expected_unrealized = 0.6 + 0.225
    assert pytest.approx(pnl["realized_usd"], rel=1e-6) == expected_realized
    assert pytest.approx(pnl["unrealized_usd"], rel=1e-6) == expected_unrealized
    assert pytest.approx(pnl["total_usd"], rel=1e-6) == expected_realized + expected_unrealized

    risk_resp = client.get("/api/risk/state")
    assert risk_resp.status_code == 200
    risk = risk_resp.get_json()
    assert pytest.approx(risk["covariance"], rel=1e-6) == 0.12
    assert risk["cov_matrix"] == [[1.0, 0.2], [0.2, 1.5]]
    assert risk["portfolio_cvar"] == 0.05

    ui._set_active_ui_state(None)
class _DummyArray:
    def __init__(self, data: List[List[float]]):
        self._data = data

    def tolist(self) -> List[List[float]]:
        return self._data

