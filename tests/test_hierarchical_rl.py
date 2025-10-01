import asyncio
import importlib.util
import sys
import types

import pytest

if importlib.util.find_spec("psutil") is None:
    sys.modules.setdefault("psutil", types.ModuleType("psutil"))
if importlib.util.find_spec("numpy") is None:
    np_mod = types.ModuleType("numpy")
    np_mod.std = lambda *a, **k: 0.0
    np_mod.mean = lambda *a, **k: 0.0
    sys.modules.setdefault("numpy", np_mod)
if importlib.util.find_spec("torch") is None:
    torch_mod = types.ModuleType("torch")
    torch_nn = types.ModuleType("torch.nn")
    torch_nn.Module = object
    torch_mod.Tensor = object
    torch_mod.device = type("device", (), {})
    torch_mod.cuda = types.SimpleNamespace(is_available=lambda: False)
    torch_mod.backends = types.SimpleNamespace(mps=types.SimpleNamespace(is_available=lambda: False))
    torch_mod.nn = torch_nn
    sys.modules.setdefault("torch", torch_mod)
    sys.modules.setdefault("torch.nn", torch_nn)
if importlib.util.find_spec("solana") is None:
    sol_mod = types.ModuleType("solana")
    sol_mod.__spec__ = importlib.machinery.ModuleSpec("solana", None)
    sys.modules.setdefault("solana", sol_mod)
    sys.modules["solana.rpc"] = types.ModuleType("rpc")
    sys.modules["solana.rpc.api"] = types.SimpleNamespace(Client=object)
    sys.modules["solana.rpc.async_api"] = types.SimpleNamespace(AsyncClient=object)
    sys.modules["solana.rpc.websocket_api"] = types.SimpleNamespace(connect=lambda *a, **k: None)
    sys.modules["solana.rpc.websocket_api"].RpcTransactionLogsFilterMentions = object
if importlib.util.find_spec("solders") is None:
    s_mod = types.ModuleType("solders")
    s_mod.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    sys.modules.setdefault("solders", s_mod)
    sys.modules["solders.keypair"] = types.SimpleNamespace(Keypair=type("Keypair", (), {}))
    sys.modules["solders.pubkey"] = types.SimpleNamespace(Pubkey=object)
    sys.modules["solders.hash"] = types.SimpleNamespace(Hash=object)
    sys.modules["solders.message"] = types.SimpleNamespace(MessageV0=object)
    sys.modules["solders.transaction"] = types.SimpleNamespace(VersionedTransaction=object)
if importlib.util.find_spec("watchfiles") is None:
    watch_mod = types.ModuleType("watchfiles")
    watch_mod.awatch = lambda *a, **k: iter(())
    sys.modules.setdefault("watchfiles", watch_mod)
if importlib.util.find_spec("sklearn") is None:
    sk_mod = types.ModuleType("sklearn")
    cluster_mod = types.SimpleNamespace(KMeans=object, DBSCAN=object)
    sk_mod.cluster = cluster_mod
    sys.modules.setdefault("sklearn", sk_mod)
    sys.modules["sklearn.cluster"] = cluster_mod
dummy_trans = types.ModuleType("transformers")
dummy_trans.pipeline = lambda *a, **k: lambda x: []
if importlib.util.find_spec("transformers") is None:
    sys.modules.setdefault("transformers", dummy_trans)
sys.modules["transformers"].pipeline = dummy_trans.pipeline
if importlib.util.find_spec("sentence_transformers") is None:
    sys.modules.setdefault("sentence_transformers", types.ModuleType("sentence_transformers"))
import types

# Stub out heavy rl_training dependency
dummy_rl_mod = types.ModuleType("solhunter_zero.rl_training")
class _DummyRL:
    def __init__(self, *a, **k):
        self.controller = types.SimpleNamespace(population=[])
    def train_controller(self, names):
        if not self.controller.population:
            return {n: 1.0 for n in names}
        cfg = self.controller.population[0]
        w = cfg.get("weights", {})
        return {n: float(w.get(n, 1.0)) for n in names}
dummy_rl_mod.MultiAgentRL = _DummyRL
sys.modules.setdefault("solhunter_zero.rl_training", dummy_rl_mod)
from solhunter_zero.swarm_coordinator import SwarmCoordinator


class HierarchicalRLAgent:
    name = "hierarchical_rl"
    def __init__(self, rl):
        self.rl = rl
        self.weights = {}
    def train(self, agent_names):
        self.weights = self.rl.train_controller(agent_names)
        return self.weights
    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return []
class DummyMemory:
    def __init__(self):
        self.trades = []

    def list_trades(self, limit: int | None = None):
        return self.trades


class DummyMemoryAgent:
    def __init__(self, mem: DummyMemory | None = None):
        self.memory = mem or DummyMemory()


class DummyAgent:
    def __init__(self, name):
        self.name = name

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return []


def test_hierarchical_rl_agent_trains(tmp_path):
    rl = _DummyRL()
    rl.controller.population = [
        {"weights": {"a1": 0.5, "a2": 1.5}, "risk": {"risk_multiplier": 1.0}},
        {"weights": {"a1": 1.0, "a2": 1.0}, "risk": {"risk_multiplier": 1.0}},
    ]
    agent = HierarchicalRLAgent(rl)
    w = agent.train(["a1", "a2"])
    assert w["a1"] == 0.5
    assert w["a2"] == 1.5


@pytest.mark.asyncio
async def test_swarm_coordinator_filters_hierarchical_agent(tmp_path):
    mem_agent = DummyMemoryAgent()
    from types import SimpleNamespace
    mem_agent.memory.trades.extend(
        [
            SimpleNamespace(token="tok", direction="buy", amount=1, price=2, reason="a1"),
            SimpleNamespace(token="tok", direction="sell", amount=1, price=1, reason="a1"),
            SimpleNamespace(token="tok", direction="buy", amount=1, price=1, reason="a2"),
            SimpleNamespace(token="tok", direction="sell", amount=1, price=2, reason="a2"),
        ]
    )
    rl = _DummyRL()
    rl.controller.population = [
        {"weights": {"a1": 0.5, "a2": 1.5}, "risk": {"risk_multiplier": 1.0}},
        {"weights": {"a1": 1.0, "a2": 1.0}, "risk": {"risk_multiplier": 1.0}},
    ]
    hier = HierarchicalRLAgent(rl)
    agents = [DummyAgent("a1"), DummyAgent("a2"), hier]
    coord = SwarmCoordinator(mem_agent, {"a1": 1.0, "a2": 1.0})
    w = await coord.compute_weights(agents)
    assert w["a1"] < w["a2"]


def test_train_policy_updates_weights():
    from types import SimpleNamespace
    from solhunter_zero.hierarchical_rl import HighLevelPolicyNetwork, train_policy

    trades = [
        SimpleNamespace(reason="a1", direction="buy", amount=1, price=1),
        SimpleNamespace(reason="a1", direction="sell", amount=1, price=2),
        SimpleNamespace(reason="a2", direction="buy", amount=1, price=2),
        SimpleNamespace(reason="a2", direction="sell", amount=1, price=1),
    ]
    model = HighLevelPolicyNetwork(2)
    w = train_policy(model, trades, ["a1", "a2"], lr=1.0, epochs=1)
    assert w["a1"] > w["a2"]

from solhunter_zero.hierarchical_rl import SupervisorAgent
from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.portfolio import Portfolio


def test_supervisor_policy_load(tmp_path):
    path = tmp_path / "sup.json"
    path.write_text('{"a1": 0.0, "a2": 2.0}')
    sup = SupervisorAgent(checkpoint=str(path))
    weights = sup.predict_weights(["a1", "a2"], token="tok", portfolio=None)
    assert weights["a1"] == 0.0
    assert weights["a2"] == 2.0


def test_agent_manager_supervisor_integration(tmp_path):
    policy = tmp_path / "sup.json"
    policy.write_text('{"a1": 0.0, "a2": 1.0}')

    calls = []

    class Dummy(DummyAgent):
        async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
            calls.append(self.name)
            return []

    agents = [Dummy("a1"), Dummy("a2")]
    cfg = AgentManagerConfig(
        weights={"a1": 1.0, "a2": 1.0},
        use_supervisor=True,
        supervisor_checkpoint=str(policy),
    )
    mgr = AgentManager(agents, config=cfg)
    pf = Portfolio(path=None)
    asyncio.run(mgr.evaluate("tok", pf))
    assert calls == ["a2"]


def test_agent_manager_hierarchical_policy(tmp_path):
    class DummyDaemon:
        def __init__(self):
            self.hier_weights = {"a1": 0.0, "a2": 2.0}

    calls = []

    class Dummy(DummyAgent):
        async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
            calls.append(self.name)
            return [{"agent": self.name, "token": token, "side": "buy", "amount": 1.0, "price": 1.0}]

    agents = [Dummy("a1"), Dummy("a2")]
    cfg = AgentManagerConfig(
        weights={"a1": 1.0, "a2": 1.0},
        rl_daemon=DummyDaemon(),
    )
    mgr = AgentManager(agents, config=cfg)
    pf = Portfolio(path=None)
    actions = asyncio.run(mgr.evaluate("tok", pf))
    assert actions and actions[0]["amount"] == 2.0


def test_from_config_creates_hierarchical_agent(monkeypatch, tmp_path):
    import solhunter_zero.agent_manager as am

    class Dummy(am.BaseAgent):
        name = "x"

        async def propose_trade(self, *a, **k):
            return []

    monkeypatch.setattr(am, "load_agent", lambda name: Dummy())
    dummy_rl = object()
    monkeypatch.setattr(am, "MultiAgentRL", lambda **k: dummy_rl)

    cfg = {
        "agents": ["x"],
        "hierarchical_rl": True,
        "hierarchical_model_path": str(tmp_path / "hp.json"),
    }
    mgr = am.AgentManager.from_config(cfg)
    assert mgr is not None
    assert mgr.hierarchical_rl is dummy_rl
    assert any(isinstance(a, am.HierarchicalRLAgent) for a in mgr.agents)
