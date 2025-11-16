import asyncio
import sys
import time
import types

import pytest

if "yaml" not in sys.modules:
    yaml_stub = types.ModuleType("yaml")
    yaml_stub.safe_load = lambda *args, **kwargs: {}
    yaml_stub.dump = lambda *args, **kwargs: ""
    sys.modules["yaml"] = yaml_stub

from solhunter_zero.pipeline.execution_service import ExecutionService
from solhunter_zero.pipeline.types import ActionBundle


def test_execute_bundle_respects_configured_venues(monkeypatch):
    agent_manager = types.SimpleNamespace(metadata=None, memory_agent=None)
    service = ExecutionService(
        asyncio.Queue(),
        agent_manager,
        config={"execution_venues": ["phoenix", "orca"]},
    )

    order: list[str] = []

    async def _pretrade(_bundle):
        return True, [], {}

    async def _retry(func, *, deadline: float):
        del deadline
        result = await func()
        return 1, result

    async def _try(bundle, actions, *, venue, **kwargs):
        order.append(venue)
        success = venue == "orca"
        return {
            "success": success,
            "results": [{"venue": venue}],
            "errors": [] if success else ["fail"],
        }

    monkeypatch.setattr(service, "_pretrade_checks", _pretrade)
    monkeypatch.setattr(service, "_retry_transient", _retry)
    monkeypatch.setattr(service, "_try_venue", _try)

    bundle = ActionBundle(
        token="TEST",
        actions=[{"action": "swap"}],
        created_at=time.time(),
        metadata={},
    )

    receipt = asyncio.run(service._execute_bundle(bundle, lane="L1"))

    assert order == ["phoenix", "orca"]
    assert receipt.metadata.get("venue") == "orca"
    assert receipt.success


def test_execute_bundle_prefers_agent_metadata_chain(monkeypatch):
    agent_manager = types.SimpleNamespace(
        metadata={"execution_venues": ["jupiter", "raydium"]},
        memory_agent=None,
    )
    service = ExecutionService(asyncio.Queue(), agent_manager)

    order: list[str] = []

    async def _pretrade(_bundle):
        return True, [], {}

    async def _retry(func, *, deadline: float):
        del deadline
        result = await func()
        return 1, result

    async def _try(bundle, actions, *, venue, **kwargs):
        order.append(venue)
        success = venue == "raydium"
        return {
            "success": success,
            "results": [{"venue": venue}],
            "errors": [] if success else ["fail"],
        }

    monkeypatch.setattr(service, "_pretrade_checks", _pretrade)
    monkeypatch.setattr(service, "_retry_transient", _retry)
    monkeypatch.setattr(service, "_try_venue", _try)

    bundle = ActionBundle(
        token="TEST",
        actions=[{"action": "swap"}],
        created_at=time.time(),
        metadata={},
    )

    receipt = asyncio.run(service._execute_bundle(bundle, lane="L2"))

    assert order[:2] == ["jupiter", "raydium"]
    assert receipt.metadata.get("venue") == "raydium"
    assert receipt.success
