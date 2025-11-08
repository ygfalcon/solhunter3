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
    from solhunter_zero import discovery_state

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

    tokens, _details = await coordinator._discovery_service._fetch()
    assert tokens == expected_tokens
    assert coordinator._discovery_service.offline is True
    assert coordinator._discovery_service.token_file == str(token_file)


@pytest.mark.anyio
async def test_pipeline_refresh_updates_discovery_settings(monkeypatch, tmp_path):
    from solhunter_zero.pipeline.coordinator import PipelineCoordinator
    from solhunter_zero.agents import discovery as discovery_mod
    from solhunter_zero import discovery_state

    token_file = tmp_path / "dynamic_tokens.txt"
    offline_tokens = ["TokOffline1", "TokOffline2", "TokOffline3"]
    token_file.write_text("\n".join(offline_tokens))
    live_tokens = ["TokLive1", "TokLive2", "TokLive3"]

    call_count = {"value": 0}
    captured: list[tuple[bool, str | None]] = []

    async def fake_discover(
        self,
        *,
        offline: bool,
        token_file: str | None,
        method: str | None = None,
        **_: Any,
    ) -> list[str]:
        call_count["value"] += 1
        captured.append((offline, token_file))
        self.last_details = {}
        if call_count["value"] == 1:
            return list(live_tokens)
        if call_count["value"] == 2:
            assert offline is True
            assert token_file == str(token_file_path)
            return token_file_path.read_text().splitlines()
        assert offline is True
        return []

    token_file_path = token_file
    monkeypatch.setattr(discovery_mod.DiscoveryAgent, "discover_tokens", fake_discover)
    monkeypatch.setattr(discovery_state, "current_method", lambda **_: "helius")

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
        discovery_cache_ttl=0.0,
        discovery_empty_cache_ttl=0.5,
    )

    tokens, _details = await coordinator._discovery_service._fetch()
    assert tokens == live_tokens
    assert captured[-1] == (False, None)

    coordinator.offline = True
    coordinator.token_file = str(token_file_path)
    coordinator.discovery_limit = 2
    coordinator.discovery_backoff_factor = 4.0
    coordinator.discovery_max_backoff = 0.3

    coordinator.refresh_discovery()

    tokens, _details = await coordinator._discovery_service._fetch()
    assert tokens == offline_tokens[:2]
    assert captured[-1] == (True, str(token_file_path))
    assert coordinator._discovery_service.offline is True
    assert coordinator._discovery_service.token_file == str(token_file_path)
    assert coordinator._discovery_service.limit == 2
    assert coordinator._discovery_service.backoff_factor == pytest.approx(4.0)
    assert coordinator._discovery_service.max_backoff == pytest.approx(0.3)
    assert coordinator.discovery_backoff_factor == pytest.approx(4.0)
    assert coordinator.discovery_max_backoff == pytest.approx(0.3)
    assert coordinator.discovery_limit == 2

    coordinator._discovery_service._cooldown_until = 0.0
    tokens, _details = await coordinator._discovery_service._fetch()
    assert tokens == []
    assert captured[-1] == (True, str(token_file_path))
    assert coordinator._discovery_service._current_backoff == pytest.approx(0.3)
    assert coordinator._discovery_service._consecutive_empty == 1
    assert call_count["value"] == 3


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


@pytest.mark.anyio
async def test_pipeline_coordinator_sets_testnet_flag(monkeypatch):
    from solhunter_zero.pipeline.coordinator import PipelineCoordinator

    class DummyExecutor:
        def __init__(self) -> None:
            self.testnet = False

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

    manager = DummyAgentManager()
    coordinator = PipelineCoordinator(
        manager,
        DummyPortfolio(),
        discovery_interval=0.1,
        discovery_cache_ttl=0.1,
        testnet=True,
    )

    assert manager.executor.testnet is True
    assert coordinator.testnet is True
    assert coordinator._execution_service.testnet is True


@pytest.mark.anyio
@pytest.mark.parametrize("source", ["env", "config"])
async def test_trading_runtime_applies_testnet_flag(monkeypatch, tmp_path, source):
    from solhunter_zero.runtime import trading_runtime as runtime_mod

    monkeypatch.setenv("NEW_PIPELINE", "1")
    monkeypatch.delenv("SOLHUNTER_OFFLINE", raising=False)
    monkeypatch.delenv("DRY_RUN", raising=False)

    if source == "env":
        monkeypatch.setenv("TESTNET", "1")
    else:
        monkeypatch.delenv("TESTNET", raising=False)

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
            self.testnet = False

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
            self.executor_testnet = getattr(args[0], "executor", SimpleNamespace()).testnet

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
    if source == "config":
        runtime.cfg["testnet"] = True

    await runtime._start_agents()

    assert isinstance(runtime.pipeline, CapturingPipeline)
    assert runtime.agent_manager.executor.testnet is True
    assert runtime.pipeline.kwargs["testnet"] is True
    assert runtime.pipeline.executor_testnet is True
