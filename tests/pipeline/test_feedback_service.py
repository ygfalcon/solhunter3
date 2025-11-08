import asyncio

import pytest

from solhunter_zero.pipeline.feedback_service import FeedbackService
from solhunter_zero.pipeline.types import EvaluationResult


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_feedback_service_honors_single_worker(monkeypatch):
    monkeypatch.delenv("FEEDBACK_WORKERS", raising=False)

    processed: list[str] = []
    processed_event = asyncio.Event()

    def on_no_action(result: EvaluationResult) -> None:
        processed.append(result.token)
        processed_event.set()

    service = FeedbackService(on_no_action=on_no_action, workers=1)

    await service.start()

    while len(service._worker_tasks) < 1:
        await asyncio.sleep(0)

    assert service._worker_limit == 1
    assert len(service._worker_tasks) == 1

    await service.put(EvaluationResult(token="token-1", actions=[], latency=0.0))

    await asyncio.wait_for(processed_event.wait(), timeout=1.0)
    assert processed == ["token-1"]

    await service.stop()
