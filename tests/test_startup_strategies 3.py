import os
import sys
import types
import asyncio

os.environ.setdefault("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d")
os.environ.setdefault("DEX_BASE_URL", "https://quote-api.jup.ag")
os.environ.setdefault("AGENTS", "[\"dummy\"]")

# Provide a minimal stub for pydantic if it's missing
try:  # pragma: no cover - optional dependency
    import pydantic  # type: ignore
except Exception:  # pragma: no cover - pydantic optional
    dummy_pydantic = types.SimpleNamespace(
        BaseModel=type(
            "DummyBaseModel",
            (),
            {"__init__": lambda self, **data: self.__dict__.update(data), "dict": lambda self, *a, **k: self.__dict__},
        ),
        AnyUrl=str,
        ValidationError=Exception,
        root_validator=lambda *a, **k: (lambda f: f),
        validator=lambda *a, **k: (lambda f: f),
        field_validator=lambda *a, **k: (lambda f: f),
        model_validator=lambda *a, **k: (lambda f: f),
    )
    sys.modules["pydantic"] = dummy_pydantic

import pytest
from solhunter_zero import main as main_module
from solhunter_zero.strategy_manager import StrategyManager


def test_multiple_strategies_evaluated_and_merged(monkeypatch):
    calls: list[str] = []

    # create dummy strategy modules with distinctive names and outputs
    mod_a = types.ModuleType("dummy_strategy_alpha")
    async def eval_a(token, portfolio):
        calls.append("alpha")
        return [{"token": token, "side": "buy", "amount": 1, "price": 0}]
    mod_a.evaluate = eval_a

    mod_b = types.ModuleType("dummy_strategy_beta")
    async def eval_b(token, portfolio):
        calls.append("beta")
        return [{"token": token, "side": "buy", "amount": 2, "price": 0}]
    mod_b.evaluate = eval_b

    sys.modules["dummy_strategy_alpha"] = mod_a
    sys.modules["dummy_strategy_beta"] = mod_b

    mgr = StrategyManager(["dummy_strategy_alpha", "dummy_strategy_beta"])

    async def fake_discover_tokens(self, **kwargs):
        return ["tok"]
    monkeypatch.setattr(main_module.DiscoveryAgent, "discover_tokens", fake_discover_tokens)

    orders: list[dict] = []
    async def fake_place_order(token, side, amount, price, testnet=False, dry_run=False, keypair=None, connectivity_test=False):
        orders.append({"token": token, "side": side, "amount": amount, "price": price})
        return {"order_id": "1"}
    monkeypatch.setattr(main_module, "place_order_async", fake_place_order)

    mem = main_module.Memory("sqlite:///:memory:")
    pf = main_module.Portfolio(path=None)

    asyncio.run(main_module._run_iteration(mem, pf, dry_run=True, offline=True, strategy_manager=mgr))

    assert set(calls) == {"alpha", "beta"}
    assert orders == [{"token": "tok", "side": "buy", "amount": 3.0, "price": 0.0}]
