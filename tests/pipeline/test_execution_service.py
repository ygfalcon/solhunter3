import asyncio

import pytest

from solhunter_zero.pipeline.execution_service import ExecutionService


class _StubExecutor:
    async def execute(self, action):
        return action


class _StubAgentManager:
    def __init__(self) -> None:
        self.executor = _StubExecutor()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_execution_service_respects_lane_worker_count():
    queue: asyncio.Queue = asyncio.Queue()
    agent_manager = _StubAgentManager()
    service = ExecutionService(queue, agent_manager, lane_workers=1)

    await service.start()
    try:
        for _ in range(20):
            if len(service._worker_tasks) == service.lane_workers:
                break
            await asyncio.sleep(0)

        assert service.lane_workers == 1
        assert len(service._worker_tasks) == 1
    finally:
        await service.stop()
