import asyncio
import types

import pytest

from solhunter_zero.config import load_config
from solhunter_zero.pipeline import PipelineCoordinator


class _StubExecutor:
    async def execute(self, action):  # pragma: no cover - interface stub
        _ = action
        return {"status": "ok"}


class _StubAgentManager:
    def __init__(self) -> None:
        self.executor = _StubExecutor()
        self.memory_agent = None

    async def evaluate_with_swarm(self, token, portfolio):  # pragma: no cover - interface stub
        _ = portfolio
        return types.SimpleNamespace(token=token, actions=[])


class _StubPortfolio:
    def __init__(self) -> None:
        self.price_history: dict[str, list[float]] = {}
        self.risk_metrics: dict[str, float] = {}

    def record_prices(self, prices):  # pragma: no cover - interface stub
        for token, price in prices.items():
            self.price_history.setdefault(token, []).append(price)

    def update_risk_metrics(self):  # pragma: no cover - interface stub
        return None


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio
async def test_pipeline_respects_configured_discovery_limit(tmp_path, monkeypatch):
    monkeypatch.delenv("DISCOVERY_LIMIT", raising=False)

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        (
            "solana_rpc_url = \"https://rpc.example.invalid\"\n"
            "dex_base_url = \"https://quote-api.jup.ag\"\n"
            "agents = [\"example\"]\n"
            "agent_weights = { example = 1.0 }\n"
            "discovery_limit = 3\n"
        )
    )
    config = load_config(str(cfg_path))

    agent_manager = _StubAgentManager()
    portfolio = _StubPortfolio()

    coordinator = PipelineCoordinator(
        agent_manager,
        portfolio,
        config=config,
    )

    service = coordinator._discovery_service
    assert service.limit == 3
    assert service._agent.limit == 3

    generated = [f"Token{i}" for i in range(5)]

    async def _fake_discover_tokens(*, offline=False, token_file=None):
        _ = offline, token_file
        await asyncio.sleep(0)
        return list(generated)

    service._agent.discover_tokens = _fake_discover_tokens  # type: ignore[attr-defined]

    tokens = await service._fetch()
    assert tokens == generated[:3]


def test_pipeline_honors_eval_workers_env(monkeypatch):
    monkeypatch.setenv("EVALUATION_WORKERS", "1")

    agent_manager = _StubAgentManager()
    portfolio = _StubPortfolio()

    coordinator = PipelineCoordinator(
        agent_manager,
        portfolio,
        config={},
    )

    service = coordinator._evaluation_service
    assert service._worker_limit == 1
