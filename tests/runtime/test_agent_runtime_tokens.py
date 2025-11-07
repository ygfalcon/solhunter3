import asyncio
import importlib.util
import sys
from pathlib import Path
from typing import Any, Coroutine
from unittest.mock import AsyncMock, Mock

import pytest

from solhunter_zero.schemas import TokenDiscovered

MODULE_PATH = (
    Path(__file__).resolve().parents[2]
    / "solhunter_zero"
    / "agents"
    / "runtime.py"
)

spec = importlib.util.spec_from_file_location(
    "solhunter_zero.agents.agent_runtime_module", MODULE_PATH
)
assert spec and spec.loader is not None
agent_runtime = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = agent_runtime
spec.loader.exec_module(agent_runtime)
AgentRuntime = agent_runtime.AgentRuntime


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_agent_runtime_on_tokens_dataclass(monkeypatch):
    manager = Mock()
    portfolio = Mock()
    runtime = AgentRuntime(manager, portfolio)
    runtime._running = True

    scheduled_tokens: list[str] = []

    async def record_token(token: str) -> None:
        scheduled_tokens.append(token)

    runtime._evaluate_and_publish = AsyncMock(side_effect=record_token)

    created_tasks: list[asyncio.Task] = []
    original_create_task = asyncio.create_task

    def capture_create_task(coro: Coroutine[Any, Any, Any]) -> asyncio.Task:
        task = original_create_task(coro)
        created_tasks.append(task)
        return task

    monkeypatch.setattr(agent_runtime.asyncio, "create_task", capture_create_task)

    payload = TokenDiscovered(tokens=["SOL", "BTC"], changed_tokens=["ETH"])
    runtime._on_tokens(payload)

    if created_tasks:
        await asyncio.gather(*created_tasks)

    assert scheduled_tokens == ["SOL", "BTC", "ETH"]
