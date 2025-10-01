import importlib
import asyncio
import pytest

from solhunter_zero.agents.crossdex_rebalancer import CrossDEXRebalancer, PortfolioOptimizer
from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero.portfolio import Portfolio
import solhunter_zero.agents.crossdex_rebalancer as reb


class DummyExec(ExecutionAgent):
    def __init__(self):
        super().__init__(rate_limit=0)
        self.actions = []

    async def execute(self, action):
        self.actions.append(action)
        return action


@pytest.fixture
def ensure_ffi(monkeypatch):
    from pathlib import Path
    import subprocess

    lib_path = Path(__file__).resolve().parents[1] / "route_ffi/target/release/libroute_ffi.so"
    if not lib_path.exists():
        subprocess.run(
            [
                "cargo",
                "build",
                "--manifest-path",
                str(Path(__file__).resolve().parents[1] / "route_ffi/Cargo.toml"),
                "--release",
            ],
            check=True,
        )
    monkeypatch.setenv("ROUTE_FFI_LIB", str(lib_path))
    importlib.reload(reb._routeffi)
    importlib.reload(reb)
    yield


def test_rebalancer_uses_ffi(monkeypatch, ensure_ffi):
    pf = Portfolio(path=None)

    async def fake_opt(self, token, pf, *, depth=None, imbalance=None):
        return [{"token": token, "side": "buy", "amount": 10.0, "price": 1.0}]

    monkeypatch.setattr(PortfolioOptimizer, "propose_trade", fake_opt)

    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_rebalancer.snapshot",
        lambda t: ({"dexA": {"bids": 50, "asks": 100}, "dexB": {"bids": 50, "asks": 20}}, 0.0),
    )

    async def fake_latency(urls):
        return {"dexA": 0.5, "dexB": 0.01}

    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_rebalancer.measure_dex_latency_async",
        fake_latency,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.crossdex_rebalancer.DEX_FEES", {"dexA": 0.0, "dexB": 0.0}
    )

    called = {}
    real = reb._routeffi.best_route_parallel

    def wrapper(*a, **k):
        called["used"] = True
        return real(*a, **k)

    monkeypatch.setattr(reb._routeffi, "best_route_parallel", wrapper)
    monkeypatch.setattr(reb._routeffi, "parallel_enabled", lambda: True)

    exec_agent = DummyExec()
    agent = CrossDEXRebalancer(executor=exec_agent, rebalance_interval=0)

    asyncio.run(agent.propose_trade("tok", pf))

    assert called.get("used", False)
