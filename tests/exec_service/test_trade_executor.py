import asyncio
import importlib

import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_trade_executor_forwards_flags(monkeypatch):
    service = importlib.import_module("solhunter_zero.exec_service.service")

    calls = []

    async def fake_place_order(token, side, amount, price, *, testnet=False, dry_run=False, keypair=None):
        calls.append(
            {
                "token": token,
                "side": side,
                "amount": amount,
                "price": price,
                "testnet": testnet,
                "dry_run": dry_run,
                "keypair": keypair,
            }
        )
        return {"status": "ok"}

    monkeypatch.setattr(service, "place_order_async", fake_place_order)
    monkeypatch.setattr(service.event_bus, "publish", lambda *_, **__: None)
    monkeypatch.setenv("RAMP_TRADES_COUNT", "1")

    class DummyMemory:
        def start_writer(self) -> None:
            pass

        async def log_trade(self, **_kwargs):
            return None

    class DummyPortfolio:
        price_history: dict[str, list[float]]
        balances: dict[str, object]

        def __init__(self) -> None:
            self.price_history = {}
            self.balances = {}

        async def update_async(self, *_args, **_kwargs):
            return None

        def total_value(self, _prices):
            return 100.0

    executor = service.TradeExecutor(
        DummyMemory(),
        DummyPortfolio(),
        testnet=True,
        dry_run=True,
    )

    payload = {"side": "buy", "token": "SOL", "size": 1.0, "price": 2.5}

    executor._on_decision(payload)

    await asyncio.sleep(0)

    assert calls, "place_order_async should be invoked"
    call = calls[0]
    assert call["testnet"] is True
    assert call["dry_run"] is True
    assert call["token"] == "SOL"
    assert call["side"] == "buy"
    assert call["amount"] == pytest.approx(1.0)
    assert call["price"] == pytest.approx(2.5)
