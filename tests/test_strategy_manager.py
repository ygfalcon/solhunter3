import sys
import types
import asyncio
import logging
import pytest
from solhunter_zero.strategy_manager import StrategyManager


class DummyPortfolio:
    pass


def test_strategy_manager_invokes_modules(monkeypatch):
    calls = []

    async def eval1(token, portfolio):
        calls.append("s1")
        return [{"token": token, "side": "buy", "amount": 1, "price": 0}]

    def eval2(token, portfolio):
        calls.append("s2")
        return [{"token": token, "side": "sell", "amount": 1, "price": 0}]

    mod1 = types.SimpleNamespace(evaluate=eval1)
    mod2 = types.SimpleNamespace(evaluate=eval2)
    monkeypatch.setitem(sys.modules, "mod1", mod1)
    monkeypatch.setitem(sys.modules, "mod2", mod2)

    mgr = StrategyManager(["mod1", "mod2"])
    actions = asyncio.run(mgr.evaluate("tok", DummyPortfolio()))

    assert "s1" in calls and "s2" in calls
    assert {"token": "tok", "side": "buy", "amount": 1, "price": 0} in actions
    assert {"token": "tok", "side": "sell", "amount": 1, "price": 0} in actions


def test_strategy_manager_merges_actions(monkeypatch):
    async def eval1(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 1.0}]

    async def eval2(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 3.0}]

    mod1 = types.SimpleNamespace(evaluate=eval1)
    mod2 = types.SimpleNamespace(evaluate=eval2)
    monkeypatch.setitem(sys.modules, "mod1", mod1)
    monkeypatch.setitem(sys.modules, "mod2", mod2)

    mgr = StrategyManager(["mod1", "mod2"])
    actions = asyncio.run(mgr.evaluate("tok", DummyPortfolio()))

    assert actions == [{"token": "tok", "side": "buy", "amount": 2.0, "price": 2.0}]


def test_strategy_manager_weighted_merge(monkeypatch):
    async def eval1(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 1.0}]

    async def eval2(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 3.0}]

    mod1 = types.SimpleNamespace(evaluate=eval1)
    mod2 = types.SimpleNamespace(evaluate=eval2)
    monkeypatch.setitem(sys.modules, "mod1", mod1)
    monkeypatch.setitem(sys.modules, "mod2", mod2)

    mgr = StrategyManager(["mod1", "mod2"])
    actions = asyncio.run(
        mgr.evaluate(
            "tok",
            DummyPortfolio(),
            weights={"mod1": 1.0, "mod2": 2.0},
        )
    )

    assert actions == [
        {"token": "tok", "side": "buy", "amount": 3.0, "price": pytest.approx(7 / 3)}
    ]


def test_strategy_manager_init_weights(monkeypatch):
    async def eval1(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 1.0}]

    async def eval2(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 3.0}]

    mod1 = types.SimpleNamespace(evaluate=eval1)
    mod2 = types.SimpleNamespace(evaluate=eval2)
    monkeypatch.setitem(sys.modules, "mod1", mod1)
    monkeypatch.setitem(sys.modules, "mod2", mod2)

    mgr = StrategyManager(["mod1", "mod2"], weights={"mod1": 1.0, "mod2": 2.0})
    actions = asyncio.run(mgr.evaluate("tok", DummyPortfolio()))

    assert actions == [
        {"token": "tok", "side": "buy", "amount": 3.0, "price": pytest.approx(7 / 3)}
    ]


def test_strategy_manager_timeout(monkeypatch):
    async def slow_eval(token, portfolio):
        await asyncio.sleep(0.1)
        return [{"token": token, "side": "buy", "amount": 1, "price": 1.0}]

    async def fast_eval(token, portfolio):
        return [{"token": token, "side": "buy", "amount": 1, "price": 1.0}]

    mod1 = types.SimpleNamespace(evaluate=slow_eval)
    mod2 = types.SimpleNamespace(evaluate=fast_eval)
    monkeypatch.setitem(sys.modules, "modslow", mod1)
    monkeypatch.setitem(sys.modules, "modfast", mod2)

    mgr = StrategyManager(["modslow", "modfast"])
    actions = asyncio.run(
        mgr.evaluate(
            "tok",
            DummyPortfolio(),
            timeouts={"modslow": 0.05},
        )
    )

    assert actions == [
        {"token": "tok", "side": "buy", "amount": 1.0, "price": 1.0}
    ]


def test_strategy_manager_warns_on_missing_evaluate(monkeypatch, caplog):
    mod = types.SimpleNamespace()  # missing evaluate
    monkeypatch.setitem(sys.modules, "mod_no_eval", mod)
    with caplog.at_level(logging.WARNING):
        StrategyManager(["mod_no_eval"])
    assert any(
        "mod_no_eval" in record.getMessage()
        and "no evaluate" in record.getMessage()
        for record in caplog.records
    )

