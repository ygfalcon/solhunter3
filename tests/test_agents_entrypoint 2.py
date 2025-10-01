import sys
import types
import importlib.metadata
import importlib.util

# Stub heavy optional dependencies before importing the package
import importlib.machinery

for mod in [
    "transformers",
    "sentence_transformers",
    "faiss",
    "torch",
    "torch.nn",
    "torch.optim",
    "aiofiles",
]:
    m = types.ModuleType(mod)
    m.__spec__ = importlib.machinery.ModuleSpec(mod, None)
    sys.modules.setdefault(mod, m)

from solhunter_zero import agents as agent_mod
from solhunter_zero.agents import BUILT_IN_AGENTS, BaseAgent, load_agent


class EPAgent(BaseAgent):
    name = "ep_dummy"

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return []

module = types.ModuleType("epmod")
module.EPAgent = EPAgent
sys.modules["epmod"] = module

entry = importlib.metadata.EntryPoint(name="ep_dummy", value="epmod:EPAgent", group="solhunter_zero.agents")

def fake_entry_points(group=None):
    if group == "solhunter_zero.agents":
        return [entry]
    return []


def test_entrypoint_agent_loaded(monkeypatch):
    monkeypatch.setattr(importlib.metadata, "entry_points", fake_entry_points)

    def dummy_loader():
        BUILT_IN_AGENTS.clear()
        for ep in importlib.metadata.entry_points(group="solhunter_zero.agents"):
            BUILT_IN_AGENTS[ep.load().name] = ep.load()

    monkeypatch.setattr(agent_mod, "_ensure_agents_loaded", dummy_loader)

    agent = load_agent("ep_dummy")
    assert isinstance(agent, EPAgent)


