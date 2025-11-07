from types import SimpleNamespace

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FailingPipeline:
    def __init__(self) -> None:
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        raise RuntimeError("pipeline start failed")

    async def stop(self) -> None:
        self.stop_calls += 1


@pytest.mark.anyio("asyncio")
async def test_runtime_rollback_on_pipeline_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = trading_runtime.TradingRuntime()
    runtime._use_new_pipeline = True

    failing_pipeline = _FailingPipeline()

    async def _prepare(self) -> None:
        self.cfg = {}
        self.runtime_cfg = {}

    async def _start_event_bus_stub(self) -> None:
        return None

    async def _start_ui_stub(self) -> None:
        self.ui_server = SimpleNamespace(stop=lambda: None)

    async def _start_agents_stub(self) -> None:
        self.pipeline = failing_pipeline
        self.agent_manager = object()
        self.memory = object()
        self.portfolio = object()

    def _ensure_metrics_stub(self) -> None:
        return None

    monkeypatch.setattr(trading_runtime.TradingRuntime, "_prepare_configuration", _prepare)
    monkeypatch.setattr(
        trading_runtime.TradingRuntime,
        "_ensure_metrics_aggregator_started",
        _ensure_metrics_stub,
    )
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_event_bus", _start_event_bus_stub)
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_ui", _start_ui_stub)
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_agents", _start_agents_stub)
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_rl_status_watcher", lambda self: None)

    with pytest.raises(RuntimeError, match="pipeline start failed"):
        await runtime.start()

    assert failing_pipeline.start_calls == 1
    assert failing_pipeline.stop_calls == 1
    assert runtime.pipeline is None
    assert runtime.status.trading_loop is False
    assert runtime.stop_event.is_set()


@pytest.mark.anyio("asyncio")
async def test_runtime_does_not_retain_empty_pipeline_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = trading_runtime.TradingRuntime()

    monkeypatch.setattr(trading_runtime, "publish", lambda *_, **__: None)

    for idx in range(50):
        result = SimpleNamespace(
            token=f"token-{idx}",
            actions=[],
            latency=0.01,
            errors=[],
            metadata={},
            cached=False,
        )
        await runtime._pipeline_on_evaluation(result)

    assert runtime._pending_tokens == {}


@pytest.mark.anyio("asyncio")
async def test_runtime_drops_existing_entry_when_actions_disappear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = trading_runtime.TradingRuntime()

    monkeypatch.setattr(trading_runtime, "publish", lambda *_, **__: None)

    token = "token-42"
    with_actions = SimpleNamespace(
        token=token,
        actions=[
            {
                "token": token,
                "side": "buy",
                "amount": 1,
                "price": 1.0,
                "agent": "demo-agent",
            }
        ],
        latency=0.01,
        errors=[],
        metadata={"score": 1.0},
        cached=False,
    )

    await runtime._pipeline_on_evaluation(with_actions)
    assert token in runtime._pending_tokens

    without_actions = SimpleNamespace(
        token=token,
        actions=[],
        latency=0.02,
        errors=["no actions"],
        metadata={},
        cached=False,
    )

    await runtime._pipeline_on_evaluation(without_actions)

    assert token not in runtime._pending_tokens
