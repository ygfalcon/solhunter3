import asyncio
import os
import sys
import types
from types import SimpleNamespace
from typing import Iterable

import pytest

if "solhunter_zero.agent_manager" not in sys.modules:
    agent_manager_stub = types.ModuleType("solhunter_zero.agent_manager")

    class AgentManager:  # pragma: no cover - minimal stub
        pass

    agent_manager_stub.AgentManager = AgentManager
    sys.modules["solhunter_zero.agent_manager"] = agent_manager_stub
else:  # pragma: no cover - reuse existing stub
    agent_manager_stub = sys.modules["solhunter_zero.agent_manager"]

if "solhunter_zero.portfolio" not in sys.modules:
    portfolio_stub = types.ModuleType("solhunter_zero.portfolio")

    class Portfolio:  # pragma: no cover - minimal stub
        def __init__(self, path: str | None = None) -> None:
            self.path = path
            self.balances: dict[str, SimpleNamespace] = {}
            self.price_history: dict[str, list[float]] = {}

        def record_prices(
            self, prices: dict[str, float], *, window: int | None = None
        ) -> None:
            self.price_history.setdefault("prices", []).append(dict(prices))

        def get_position(self, token: str) -> None:
            return None

        def total_value(self, prices: dict[str, float]) -> float:
            return 0.0

    portfolio_stub.Portfolio = Portfolio
    sys.modules["solhunter_zero.portfolio"] = portfolio_stub
else:  # pragma: no cover - reuse existing stub
    portfolio_stub = sys.modules["solhunter_zero.portfolio"]

if "solhunter_zero.token_scanner" not in sys.modules:
    token_scanner_stub = types.ModuleType("solhunter_zero.token_scanner")
    token_scanner_stub.TRENDING_METADATA = {}
    sys.modules["solhunter_zero.token_scanner"] = token_scanner_stub
else:  # pragma: no cover - reuse existing stub
    token_scanner_stub = sys.modules["solhunter_zero.token_scanner"]
    if not hasattr(token_scanner_stub, "TRENDING_METADATA"):
        token_scanner_stub.TRENDING_METADATA = {}

from solhunter_zero.event_bus import EventBus
from solhunter_zero.golden_pipeline.service import GoldenPipelineService
from solhunter_zero.golden_pipeline.types import DepthSnapshot, OHLCVBar, TokenSnapshot
from solhunter_zero.portfolio import Portfolio


class RecordingAgentManager:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def evaluate_with_swarm(self, mint: str, portfolio: object) -> SimpleNamespace:
        self.calls.append(mint)
        return SimpleNamespace(actions=[])


MINT = "MintAphex1111111111111111111111111111111"


def _clear_timeout_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("GOLDEN_AGENT_TIMEOUT_SEC", "GOLDEN_AGENT_TIMEOUT", "AGENT_TIMEOUT"):
        monkeypatch.delenv(name, raising=False)


def test_resolve_agent_timeout_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_timeout_env(monkeypatch)
    timeout = GoldenPipelineService.resolve_agent_timeout({}, env={})
    assert timeout == pytest.approx(GoldenPipelineService.DEFAULT_AGENT_TIMEOUT_SEC)


def test_resolve_agent_timeout_from_config(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_timeout_env(monkeypatch)
    cfg = {"golden": {"agent_timeout_sec": 2.25}}
    timeout = GoldenPipelineService.resolve_agent_timeout(cfg, env={})
    assert timeout == pytest.approx(2.25)


def test_resolve_agent_timeout_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_timeout_env(monkeypatch)
    monkeypatch.setenv("GOLDEN_AGENT_TIMEOUT", "2.75")
    timeout = GoldenPipelineService.resolve_agent_timeout({}, env=dict(os.environ))
    assert timeout == pytest.approx(2.75)


def test_resolve_agent_timeout_disable(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_timeout_env(monkeypatch)
    monkeypatch.setenv("GOLDEN_AGENT_TIMEOUT_SEC", "0")
    timeout = GoldenPipelineService.resolve_agent_timeout({}, env=dict(os.environ))
    assert timeout is None

def test_agents_receive_bootstrapped_snapshot() -> None:
    async def _run() -> None:
        token_scanner_stub.TRENDING_METADATA.clear()
        token_scanner_stub.TRENDING_METADATA[MINT] = {
            "symbol": "BOOT",
            "decimals": 6,
        }

        fetch_calls: list[tuple[str, ...]] = []

        async def fake_enrichment(mints: Iterable[str]) -> dict[str, TokenSnapshot]:
            batch = tuple(str(m) for m in mints)
            fetch_calls.append(batch)
            return {
                mint: TokenSnapshot(
                    mint=mint,
                    symbol="BOOT",
                    name="Bootstrap Token",
                    decimals=6,
                    token_program="Tokenkeg",
                    venues=("bootstrap",),
                    flags={"source": "bootstrap"},
                    asof=1_234_567.0,
                )
                for mint in batch
            }

        manager = RecordingAgentManager()
        portfolio = Portfolio(path=None)
        bus = EventBus()

        service = GoldenPipelineService(
            agent_manager=manager,
            portfolio=portfolio,
            enrichment_fetcher=fake_enrichment,
            event_bus=bus,
        )

        try:
            await service.start()
            assert fetch_calls == [(MINT,)]

            bar = OHLCVBar(
                mint=MINT,
                open=1.0,
                high=1.1,
                low=0.95,
                close=1.05,
                vol_usd=25_000.0,
                trades=12,
                buyers=6,
                flow_usd=8_000.0,
                zret=3.2,
                zvol=3.5,
                asof_close=1_234_572.0,
            )
            await service.pipeline.inject_bar(bar)

            depth = DepthSnapshot(
                mint=MINT,
                venue="bootstrap",
                mid_usd=1.05,
                spread_bps=24.0,
                depth_pct={"1": 20_000.0, "2": 35_000.0},
                asof=1_234_573.0,
            )
            await service.pipeline.submit_depth(depth)

            assert manager.calls == [MINT]
        finally:
            await service.stop()
            token_scanner_stub.TRENDING_METADATA.clear()

    asyncio.run(_run())
