from __future__ import annotations

import pytest

from solhunter_zero import discovery_state
from solhunter_zero import loop as loop_mod


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_trading_loop_uses_updated_discovery_method(monkeypatch):
    monkeypatch.delenv("DISCOVERY_METHOD", raising=False)
    discovery_state.clear_override()

    captured: list[str | None] = []

    async def fake_run_iteration(*args, discovery_method=None, **kwargs):
        captured.append(discovery_method)
        if len(captured) == 1:
            discovery_state.set_override("mempool")
        else:
            discovery_state.clear_override()
        return {}

    async def fake_sleep(_delay):
        return None

    monkeypatch.setattr(loop_mod, "run_iteration", fake_run_iteration)
    monkeypatch.setattr(loop_mod.asyncio, "sleep", fake_sleep)

    await loop_mod.trading_loop(
        {},
        {},
        object(),
        object(),
        object(),
        iterations=2,
        discovery_method="helius",
    )

    assert captured[:2] == ["helius", "mempool"]
