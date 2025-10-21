import asyncio
import json
import sys
import types
from types import SimpleNamespace
from typing import Any, Iterable

import pytest

if "solhunter_zero.agent_manager" not in sys.modules:
    agent_manager_stub = types.ModuleType("solhunter_zero.agent_manager")

    class AgentManager:  # pragma: no cover - minimal stub for tests
        pass

    agent_manager_stub.AgentManager = AgentManager
    sys.modules["solhunter_zero.agent_manager"] = agent_manager_stub

if "solhunter_zero.portfolio" not in sys.modules:
    portfolio_stub = types.ModuleType("solhunter_zero.portfolio")

    class Portfolio:  # pragma: no cover - minimal stub for tests
        def __init__(self, path: str | None = None) -> None:
            self.path = path

        def record_prices(
            self, prices: dict[str, float], *, window: int | None = None
        ) -> None:
            self._last_prices = dict(prices)

    portfolio_stub.Portfolio = Portfolio
    sys.modules["solhunter_zero.portfolio"] = portfolio_stub

if "solhunter_zero.token_scanner" not in sys.modules:
    token_scanner_stub = types.ModuleType("solhunter_zero.token_scanner")
    token_scanner_stub.TRENDING_METADATA = {}
    sys.modules["solhunter_zero.token_scanner"] = token_scanner_stub

from solhunter_zero.event_bus import BUS as RUNTIME_BUS, reset as reset_event_bus
from solhunter_zero.golden_pipeline.contracts import STREAMS, discovery_buffer_key
from solhunter_zero.golden_pipeline.service import GoldenPipelineService
from solhunter_zero.golden_pipeline.types import TokenSnapshot


MINT_A = "MintAphex1111111111111111111111111111111"
MINT_B = "MintBeton11111111111111111111111111111111"


class DummyPortfolio:
    def record_prices(self, prices: dict[str, float], *, window: int | None = None) -> None:
        self._last_prices = dict(prices)


class DummyAgentManager:
    async def evaluate_with_swarm(self, mint: str, portfolio: DummyPortfolio) -> SimpleNamespace:
        return SimpleNamespace(actions=[])


async def _fake_enrichment_fetcher(mints: Iterable[str]) -> dict[str, TokenSnapshot]:
    now = 1_700_000_000.0
    snapshots: dict[str, TokenSnapshot] = {}
    for mint in mints:
        snapshots[str(mint)] = TokenSnapshot(
            mint=str(mint),
            symbol="TEST",
            name="Test Token",
            decimals=6,
            token_program="Tokenkeg",
            venues=("sim",),
            flags={},
            asof=now,
        )
    return snapshots


def test_service_restores_cursor_and_replays_buffered_candidates(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _exercise() -> None:
        reset_event_bus()
        manager = DummyAgentManager()
        portfolio = DummyPortfolio()
        service = GoldenPipelineService(
            agent_manager=manager,
            portfolio=portfolio,
            enrichment_fetcher=_fake_enrichment_fetcher,
            event_bus=RUNTIME_BUS,
        )

        stage = service.pipeline._discovery_stage  # type: ignore[attr-defined]
        await stage.persist_cursor("cursor-test-123")
        kv = service.pipeline._kv  # type: ignore[attr-defined]
        buffered_payload = [
            {"mint": MINT_A, "cursor": "cursor-test-123", "source": "das"},
            {"mint": MINT_B},
        ]
        await kv.set(discovery_buffer_key(), json.dumps(buffered_payload))

        discovery_events: list[dict[str, Any]] = []
        ready = asyncio.Event()

        def _capture(payload: Any) -> None:
            discovery_events.append(payload)
            if len(discovery_events) >= len(buffered_payload):
                ready.set()

        unsub = RUNTIME_BUS.subscribe(STREAMS.discovery_candidates, _capture)

        call_order: list[str] = []
        original_subscribe = service._event_bus.subscribe

        def _tracking_subscribe(topic: str, handler):
            result = original_subscribe(topic, handler)
            call_order.append(f"subscribe:{topic}")
            return result

        monkeypatch.setattr(service._event_bus, "subscribe", _tracking_subscribe)

        original_load_cursor = stage.load_cursor

        async def _tracking_load_cursor() -> str | None:
            call_order.append("load_cursor")
            return await original_load_cursor()

        monkeypatch.setattr(stage, "load_cursor", _tracking_load_cursor)

        try:
            await service.start()
            await asyncio.wait_for(ready.wait(), timeout=1.0)
            await asyncio.sleep(0.05)

            assert len(discovery_events) == len(buffered_payload)
            first, second = discovery_events
            assert first["mint"] == MINT_A
            assert first.get("cursor") == "cursor-test-123"
            assert second["mint"] == MINT_B
            assert second.get("cursor") in (None, "")

            RUNTIME_BUS.publish(
                "token_discovered",
                {"tokens": [MINT_A, {"mint": MINT_B}]},
            )
            await asyncio.sleep(0.1)
            assert len(discovery_events) == len(buffered_payload)

            assert "load_cursor" in call_order
            load_index = call_order.index("load_cursor")
            for topic in ("token_discovered", "price_update", "depth_update"):
                target = f"subscribe:{topic}"
                topic_index = next(
                    idx for idx, entry in enumerate(call_order) if entry == target
                )
                assert load_index < topic_index
        finally:
            unsub()
            await service.stop()

    asyncio.run(_exercise())
