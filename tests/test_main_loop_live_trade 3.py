import asyncio
import pytest

from solhunter_zero import wallet, routeffi, depth_client
from solhunter_zero.agents.discovery import DiscoveryAgent


@pytest.mark.asyncio
async def test_main_loop_executes_trade(monkeypatch):
    flags = {"wallet": False, "route": False, "depth": False}
    trades: list[dict] = []

    def fake_load_keypair(path: str):
        flags["wallet"] = True
        return object()

    async def fake_best_route(*args, **kwargs):
        flags["route"] = True
        return {"path": ["A", "B"], "amount": kwargs.get("amount", 0)}

    async def fake_snapshot(token: str):
        flags["depth"] = True
        return {}, 0.0

    async def fake_discover(self, **_):
        return ["FAKE"]

    class DummyStrategyManager:
        async def evaluate(self, token: str, portfolio):
            await routeffi.best_route({}, 1.0)
            await depth_client.snapshot(token)
            return [{"token": token, "side": "buy", "amount": 1.0, "price": 1.0}]

        def list_missing(self):
            return []

    class DummyMemory:
        async def log_trade(self, **kw):
            trades.append(kw)

    monkeypatch.setattr(wallet, "load_keypair", fake_load_keypair)
    monkeypatch.setattr(routeffi, "best_route", fake_best_route)
    monkeypatch.setattr(depth_client, "snapshot", fake_snapshot)
    monkeypatch.setattr(DiscoveryAgent, "discover_tokens", fake_discover)

    strategy = DummyStrategyManager()
    memory = DummyMemory()

    async def trading_sequence() -> None:
        wallet.load_keypair("dummy")
        disc = DiscoveryAgent()
        tokens = await disc.discover_tokens()
        portfolio = object()
        async with asyncio.TaskGroup() as tg:
            for token in tokens:
                async def eval_and_log(tok: str):
                    proposals = await strategy.evaluate(tok, portfolio)
                    for trade in proposals:
                        await memory.log_trade(**trade)
                tg.create_task(eval_and_log(token))

    await trading_sequence()

    assert flags["wallet"], "wallet was not loaded"
    assert flags["route"], "route lookup did not occur"
    assert flags["depth"], "depth query did not occur"
    assert trades, "no trade was logged"
