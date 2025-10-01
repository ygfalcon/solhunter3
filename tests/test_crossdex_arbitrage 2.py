import asyncio

from solhunter_zero.agents.crossdex_arbitrage import CrossDEXArbitrage
from solhunter_zero import arbitrage as arb
from solhunter_zero.portfolio import Portfolio


async def dex1(token):
    return 1.0


async def dex2(token):
    return 1.2


async def dex3(token):
    return 1.4


def test_crossdex_arbitrage_best_path(monkeypatch):
    pf = Portfolio(path=None)

    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_arbitrage.DEX_FEES",
        {"dex1": 0.0, "dex2": 0.0, "dex3": 0.0},
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_arbitrage.DEX_GAS",
        {"dex1": 0.0, "dex2": 0.0, "dex3": 0.0},
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_arbitrage.DEX_LATENCY",
        {"dex1": 0.0, "dex2": 0.0, "dex3": 0.0},
    )

    called = {}

    def fake_best_route(prices, amt, *, fees=None, gas=None, latency=None, max_hops=4):
        called["used"] = True
        return arb._best_route_py(
            prices,
            amt,
            fees=fees,
            gas=gas,
            latency=latency,
            max_hops=max_hops,
        )

    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_arbitrage._routeffi.best_route",
        fake_best_route,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_arbitrage._routeffi.parallel_enabled",
        lambda: False,
    )

    agent = CrossDEXArbitrage(
        amount=1.0,
        feeds={"dex1": dex1, "dex2": dex2, "dex3": dex3},
        max_hops=3,
    )

    actions = asyncio.run(agent.propose_trade("tok", pf))
    agent.close()

    expected_path, _ = arb._best_route_py(
        {"dex1": 1.0, "dex2": 1.2, "dex3": 1.4},
        1.0,
        fees={"dex1": 0.0, "dex2": 0.0, "dex3": 0.0},
        gas={"dex1": 0.0, "dex2": 0.0, "dex3": 0.0},
        latency={"dex1": 0.0, "dex2": 0.0, "dex3": 0.0},
        max_hops=3,
    )

    assert called.get("used", False)
    assert actions
    assert actions[0]["venue"] == expected_path[0]
    assert actions[-1]["venue"] == expected_path[-1]
    assert len(actions) == 2 * (len(expected_path) - 1)
