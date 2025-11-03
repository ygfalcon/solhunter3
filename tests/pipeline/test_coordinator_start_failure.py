import pytest

from solhunter_zero.pipeline import coordinator


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _StubService:
    def __init__(self, name: str, *, fail: bool = False, registry: dict[str, "_StubService"]):
        self.name = name
        self.fail = fail
        self.started = False
        self.stopped = False
        self.stop_calls = 0
        registry[name] = self

    async def start(self) -> None:
        self.started = True
        if self.fail:
            raise RuntimeError(f"{self.name} start failure")

    async def stop(self) -> None:
        self.stopped = True
        self.stop_calls += 1

    async def put(self, *_args, **_kwargs) -> None:
        return None


def _service_factory(name: str, *, fail: bool = False, registry: dict[str, _StubService]):
    def _factory(*_args, **_kwargs) -> _StubService:
        return _StubService(name, fail=fail, registry=registry)

    return _factory


@pytest.mark.anyio("asyncio")
async def test_pipeline_start_rolls_back_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    created: dict[str, _StubService] = {}

    monkeypatch.setattr(
        coordinator,
        "DiscoveryService",
        _service_factory("discovery", registry=created),
    )
    monkeypatch.setattr(
        coordinator,
        "ScoringService",
        _service_factory("scoring", registry=created),
    )
    monkeypatch.setattr(
        coordinator,
        "EvaluationService",
        _service_factory("evaluation", fail=True, registry=created),
    )
    monkeypatch.setattr(
        coordinator,
        "ExecutionService",
        _service_factory("execution", registry=created),
    )
    monkeypatch.setattr(
        coordinator,
        "FeedbackService",
        _service_factory("feedback", registry=created),
    )
    monkeypatch.setattr(
        coordinator,
        "PortfolioManagementService",
        _service_factory("portfolio", registry=created),
    )

    pipeline = coordinator.PipelineCoordinator(agent_manager=object(), portfolio=object())

    with pytest.raises(RuntimeError, match="evaluation start failure"):
        await pipeline.start()

    discovery = created["discovery"]
    scoring = created["scoring"]
    evaluation = created["evaluation"]
    execution = created["execution"]
    feedback = created["feedback"]
    portfolio = created["portfolio"]

    assert discovery.started
    assert scoring.started
    assert evaluation.started

    assert discovery.stop_calls == 1
    assert scoring.stop_calls == 1
    assert evaluation.stop_calls == 1

    assert not execution.started
    assert feedback.stop_calls == 0
    assert portfolio.stop_calls == 0

