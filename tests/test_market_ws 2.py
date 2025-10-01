import asyncio
import solhunter_zero.market_ws as mws
from solhunter_zero.simulation import SimulationResult


def test_listen_and_trade_triggers_buy(monkeypatch):
    async def fake_events(url):
        yield {"token": "tok", "volume": 200}

    monkeypatch.setattr(mws, "subscribe_events", fake_events)
    async def fake_prices(tokens):
        return {t: 1.0 for t in tokens}

    monkeypatch.setattr(mws, "fetch_token_prices_async", fake_prices)

    async def fake_log_trade(*a, **k):
        pass

    monkeypatch.setattr(mws.Memory, "log_trade", fake_log_trade)
    monkeypatch.setattr(mws.Portfolio, "update", lambda *a, **k: None)

    monkeypatch.setattr(
        mws,
        "run_simulations",
        lambda token, count=100, recent_volume=None, recent_slippage=None: [
            SimulationResult(1.0, 0.5, volume=recent_volume or 0, liquidity=0)
        ],
    )
    monkeypatch.setattr(mws, "should_buy", lambda sims: True)
    called = {}

    async def fake_place_order(token, side, amount, price, testnet=False, dry_run=False, keypair=None):
        called["token"] = token
        return {"ok": True}

    monkeypatch.setattr(mws, "place_order_async", fake_place_order)

    pf = mws.Portfolio(path=None)
    mem = mws.Memory("sqlite:///:memory:")

    asyncio.run(
        mws.listen_and_trade(
            "ws://test",
            mem,
            pf,
            volume_threshold=100,
            max_events=1,
            dry_run=True,
        )
    )

    assert called.get("token") == "tok"


