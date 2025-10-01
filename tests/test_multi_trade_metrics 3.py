import pytest
import types
import sys

pb_stub = types.ModuleType("event_pb2")
for _name in [
    "ActionExecuted",
    "WeightsUpdated",
    "RLWeights",
    "RLCheckpoint",
    "PortfolioUpdated",
    "DepthUpdate",
    "DepthServiceStatus",
    "Heartbeat",
    "TradeLogged",
    "RLMetrics",
    "SystemMetrics",
    "PriceUpdate",
    "ConfigUpdated",
    "PendingSwap",
    "RemoteSystemMetrics",
    "RiskMetrics",
    "RiskUpdated",
    "SystemMetricsCombined",
    "TokenDiscovered",
    "MemorySyncRequest",
    "MemorySyncResponse",
    "Event",
]:
    setattr(pb_stub, _name, type(_name, (), {}))
sys.modules.setdefault("solhunter_zero.event_pb2", pb_stub)

from solhunter_zero.memory import Memory


import asyncio


@pytest.mark.asyncio
async def test_multi_trade_roi(tmp_path):
    db = tmp_path / "mem.db"
    mem = Memory(f"sqlite:///{db}")

    # Agent a1 profitable trades
    await mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1")
    await mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a1")
    await mem.log_trade(token="tok", direction="buy", amount=1, price=2, reason="a1")
    await mem.log_trade(token="tok", direction="sell", amount=1, price=3, reason="a1")

    # Agent a2 losing trades
    await mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a2")
    await mem.log_trade(token="tok", direction="sell", amount=1, price=0.5, reason="a2")
    await mem.log_trade(token="tok", direction="buy", amount=1, price=2, reason="a2")
    await mem.log_trade(token="tok", direction="sell", amount=1, price=1.5, reason="a2")

    trades = await mem.list_trades()
    summary = {}
    for t in trades:
        info = summary.setdefault(t.reason, {"buy": 0.0, "sell": 0.0})
        info[t.direction] += float(t.amount) * float(t.price)

    roi_a1 = (summary["a1"]["sell"] - summary["a1"]["buy"]) / summary["a1"]["buy"]
    roi_a2 = (summary["a2"]["sell"] - summary["a2"]["buy"]) / summary["a2"]["buy"]

    assert roi_a1 == pytest.approx((5 - 3) / 3)
    assert roi_a2 == pytest.approx((2 - 3) / 3)
    spent = sum(t.amount * t.price for t in trades if t.direction == "buy")
    revenue = sum(t.amount * t.price for t in trades if t.direction == "sell")
    total_roi = (revenue - spent) / spent
    assert total_roi == pytest.approx((7 - 6) / 6)
