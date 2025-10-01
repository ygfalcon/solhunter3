import asyncio
import importlib

from tests import stubs


def test_trade_logging_with_sqlalchemy(tmp_path, monkeypatch):
    """Investor demo logs trades using SQLAlchemy-backed Memory."""
    stubs.stub_sqlalchemy()

    import solhunter_zero.memory as memory
    importlib.reload(memory)

    class TrackingMemory(memory.Memory):
        last_instance: "TrackingMemory | None" = None

        def __init__(self, url: str):
            super().__init__(url)
            TrackingMemory.last_instance = self

    import solhunter_zero.investor_demo as investor_demo
    importlib.reload(investor_demo)
    monkeypatch.setattr(investor_demo, "Memory", TrackingMemory, raising=False)

    investor_demo.main(["--reports", str(tmp_path)])

    mem = TrackingMemory.last_instance
    assert mem is not None
    trades = asyncio.run(mem.list_trades())
    assert trades and len(trades) == 1
    trade = trades[0]
    assert trade.token and trade.direction and trade.price
