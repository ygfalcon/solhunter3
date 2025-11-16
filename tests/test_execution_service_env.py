import asyncio
import sys
import types

import pytest

sys.modules.setdefault("yaml", types.SimpleNamespace(safe_load=lambda fh: {}))

from solhunter_zero.pipeline.execution_service import ExecutionService
from solhunter_zero.pipeline.types import ActionBundle


class DummyPortfolio:
    def __init__(self, position_value_usd: dict | None = None, open_orders: int = 0):
        self.position_value_usd = position_value_usd or {}
        self.open_orders = open_orders
        self.position_qty = {}


class DummyAgentManager:
    def __init__(self, portfolio: DummyPortfolio | None = None):
        self.portfolio = portfolio


def test_position_cap_uses_current_env(monkeypatch):
    async def _run():
        bundle = ActionBundle(
            token="TEST",
            actions=[],
            created_at=0.0,
            metadata={"side": "buy", "qty": 1, "notional_usd": 60.0},
        )
        portfolio = DummyPortfolio(position_value_usd={"TEST": 50.0})

        monkeypatch.setenv("MAX_POSITION_USD_PER_TOKEN", "100")
        svc_first = ExecutionService(asyncio.Queue(), DummyAgentManager(portfolio))
        ok_first, errors_first, _ = await svc_first._pretrade_checks(bundle)

        assert not ok_first
        assert any("position cap would be exceeded" in err for err in errors_first)

        monkeypatch.setenv("MAX_POSITION_USD_PER_TOKEN", "150")
        svc_second = ExecutionService(asyncio.Queue(), DummyAgentManager(portfolio))
        ok_second, errors_second, _ = await svc_second._pretrade_checks(bundle)

        assert ok_second
        assert errors_second == []

    asyncio.run(_run())
