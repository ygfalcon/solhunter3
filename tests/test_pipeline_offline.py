from types import SimpleNamespace
from typing import Any

import pytest


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio
async def test_pipeline_discovery_uses_token_file(tmp_path, monkeypatch):
    from solhunter_zero.pipeline.coordinator import PipelineCoordinator
    from solhunter_zero.agents import discovery as discovery_mod

    # Fail fast if any live discovery helper is invoked.
    async def _fail(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover - defensive
        raise AssertionError("live discovery invoked")

    monkeypatch.setattr(discovery_mod, "scan_tokens_async", _fail)
    monkeypatch.setattr(discovery_mod, "merge_sources", _fail)
    monkeypatch.setattr(discovery_mod, "scan_tokens_onchain", _fail)

    token_file = tmp_path / "tokens.txt"
    expected_tokens = ["TokA111111111111111111111111111111111111", "TokB222222222222222222222222222222222222"]
    token_file.write_text("\n".join(expected_tokens))

    class DummyExecutor:
        async def execute(self, action: dict) -> dict:  # pragma: no cover - not exercised
            return {"action": action}

    class DummyAgentManager:
        def __init__(self) -> None:
            self.executor = DummyExecutor()
            self.memory_agent = None

        async def evaluate_with_swarm(self, token: str, portfolio: object) -> SimpleNamespace:
            return SimpleNamespace(actions=[])

    class DummyPortfolio:
        def record_prices(self, prices: dict | None = None) -> None:  # pragma: no cover - not used
            return None

        def update_risk_metrics(self) -> None:  # pragma: no cover - not used
            return None

    coordinator = PipelineCoordinator(
        DummyAgentManager(),
        DummyPortfolio(),
        discovery_interval=0.1,
        discovery_cache_ttl=0.1,
        offline=True,
        token_file=str(token_file),
    )

    tokens = await coordinator._discovery_service._fetch()
    assert tokens == expected_tokens
    assert coordinator._discovery_service.offline is True
    assert coordinator._discovery_service.token_file == str(token_file)


@pytest.mark.anyio
async def test_trading_runtime_passes_offline_and_token_file(monkeypatch, tmp_path):
    from solhunter_zero.runtime import trading_runtime as runtime_mod

    monkeypatch.setenv("NEW_PIPELINE", "1")
    monkeypatch.setenv("SOLHUNTER_OFFLINE", "1")

    token_file = tmp_path / "tokens.txt"
    token_file.write_text("TokC333333333333333333333333333333333333")

    class DummyMemory:
        def __init__(self, url: str) -> None:
            self.url = url
            self.started = False

        def start_writer(self) -> None:
            self.started = True

    class DummyPortfolio:
        def __init__(self, path: str) -> None:
            self.path = path

        def record_prices(self, prices: dict | None = None) -> None:  # pragma: no cover - not used
            return None

        def update_risk_metrics(self) -> None:  # pragma: no cover - not used
            return None

    class DummyAgentManager:
        executor = SimpleNamespace(execute=lambda *args, **kwargs: None)
        memory_agent = None

    dummy_manager = DummyAgentManager()

    async def fake_init_rl_training(*args: Any, **kwargs: Any) -> None:
        return None

    class CapturingPipeline:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.kwargs = kwargs

        async def start(self) -> None:  # pragma: no cover - not used in test
            return None

        async def stop(self) -> None:  # pragma: no cover - not used in test
            return None

    monkeypatch.setattr(runtime_mod, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_mod, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_mod.AgentManager, "from_config", classmethod(lambda cls, cfg: dummy_manager))
    monkeypatch.setattr(runtime_mod.AgentManager, "from_default", classmethod(lambda cls: dummy_manager))
    monkeypatch.setattr(runtime_mod, "PipelineCoordinator", CapturingPipeline)
    monkeypatch.setattr(runtime_mod, "_init_rl_training", fake_init_rl_training)

    runtime = runtime_mod.TradingRuntime()
    runtime.cfg = {
        "memory_path": f"sqlite:///{tmp_path/'memory.db'}",
        "portfolio_path": str(tmp_path / "portfolio.json"),
        "token_file": str(token_file),
        "rl_auto_train": False,
    }

    await runtime._start_agents()

    assert isinstance(runtime.pipeline, CapturingPipeline)
    assert runtime.pipeline.kwargs["offline"] is True
    assert runtime.pipeline.kwargs["token_file"] == str(token_file)


@pytest.mark.anyio
async def test_trading_runtime_dry_run_keeps_live_discovery(monkeypatch, tmp_path):
    from solhunter_zero.runtime import trading_runtime as runtime_mod

    monkeypatch.setenv("NEW_PIPELINE", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.delenv("SOLHUNTER_OFFLINE", raising=False)
    monkeypatch.delenv("LIVE_DISCOVERY", raising=False)

    class DummyMemory:
        def __init__(self, url: str) -> None:
            self.url = url
            self.started = False

        def start_writer(self) -> None:
            self.started = True

    class DummyPortfolio:
        def __init__(self, path: str) -> None:
            self.path = path

        def record_prices(self, prices: dict | None = None) -> None:  # pragma: no cover - not used
            return None

        def update_risk_metrics(self) -> None:  # pragma: no cover - not used
            return None

    class DummyExecutor:
        def __init__(self) -> None:
            self.dry_run = False

        async def execute(self, action: dict) -> dict:  # pragma: no cover - defensive
            return {"action": action}

    class DummyAgentManager:
        def __init__(self) -> None:
            self.executor = DummyExecutor()
            self.memory_agent = None

        async def evaluate_with_swarm(self, token: str, portfolio: object) -> SimpleNamespace:
            return SimpleNamespace(actions=[])

    dummy_manager = DummyAgentManager()

    async def fake_init_rl_training(*args: Any, **kwargs: Any) -> None:
        return None

    class CapturingPipeline:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.kwargs = kwargs

        async def start(self) -> None:  # pragma: no cover - not used in test
            return None

        async def stop(self) -> None:  # pragma: no cover - not used in test
            return None

    monkeypatch.setattr(runtime_mod, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_mod, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_mod.AgentManager, "from_config", classmethod(lambda cls, cfg: dummy_manager))
    monkeypatch.setattr(runtime_mod.AgentManager, "from_default", classmethod(lambda cls: dummy_manager))
    monkeypatch.setattr(runtime_mod, "PipelineCoordinator", CapturingPipeline)
    monkeypatch.setattr(runtime_mod, "_init_rl_training", fake_init_rl_training)

    runtime = runtime_mod.TradingRuntime()
    runtime.cfg = {
        "memory_path": f"sqlite:///{tmp_path/'memory.db'}",
        "portfolio_path": str(tmp_path / "portfolio.json"),
        "rl_auto_train": False,
    }

    await runtime._start_agents()

    assert isinstance(runtime.pipeline, CapturingPipeline)
    assert runtime.pipeline.kwargs["offline"] is False
    assert runtime.agent_manager.executor.dry_run is True
