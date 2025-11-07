import asyncio

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _DummyPipeline:
    def __init__(self, samples):
        self.samples = samples

    async def snapshot_telemetry(self):
        return list(self.samples)


@pytest.mark.anyio("asyncio")
async def test_pipeline_telemetry_resets_after_restart(monkeypatch):
    runtime = trading_runtime.TradingRuntime()

    original_sleep = asyncio.sleep

    async def fast_sleep(_: float) -> None:
        await original_sleep(0)

    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fast_sleep)

    first_pipeline = _DummyPipeline([{"timestamp": 1.0, "value": "first"}])
    runtime.pipeline = first_pipeline

    poller = asyncio.create_task(runtime._pipeline_telemetry_poller())

    try:
        for _ in range(5):
            await original_sleep(0)
            entries = [
                entry["payload"]["value"]
                for entry in runtime._snapshot_ui_logs()
                if entry.get("topic") == "pipeline"
            ]
            if entries:
                break
        assert entries == ["first"]

        second_pipeline = _DummyPipeline([{"timestamp": 2.0, "value": "second"}])
        runtime.pipeline = second_pipeline

        for _ in range(5):
            await original_sleep(0)
            entries = [
                entry["payload"]["value"]
                for entry in runtime._snapshot_ui_logs()
                if entry.get("topic") == "pipeline"
            ]
            if entries == ["first", "second"]:
                break
        assert entries == ["first", "second"]
    finally:
        runtime.stop_event.set()
        await poller
