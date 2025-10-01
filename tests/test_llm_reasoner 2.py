import asyncio
import types
import sys
import importlib.util

import pytest
import importlib.machinery

# Stub heavy optional dependencies
if importlib.util.find_spec("transformers") is None:
    trans_mod = types.ModuleType("transformers")
    trans_mod.__spec__ = importlib.machinery.ModuleSpec("transformers", None)
    sys.modules.setdefault("transformers", trans_mod)
sys.modules["transformers"].pipeline = lambda *a, **k: lambda x: []
if importlib.util.find_spec("sentence_transformers") is None:
    st_mod = types.ModuleType("sentence_transformers")
    st_mod.__spec__ = importlib.machinery.ModuleSpec("sentence_transformers", None)
    sys.modules.setdefault("sentence_transformers", st_mod)
if importlib.util.find_spec("faiss") is None:
    faiss_mod = types.ModuleType("faiss")
    faiss_mod.__spec__ = importlib.machinery.ModuleSpec("faiss", None)
    sys.modules.setdefault("faiss", faiss_mod)
if importlib.util.find_spec("torch") is None:
    tmod = types.ModuleType("torch")
    tmod.__spec__ = importlib.machinery.ModuleSpec("torch", None)
    tmod.__path__ = []
    tmod.Tensor = object
    tmod.cuda = types.SimpleNamespace(is_available=lambda: False)
    tmod.backends = types.SimpleNamespace(mps=types.SimpleNamespace(is_available=lambda: False))
    class _Device:
        def __init__(self, typ="cpu"):
            self.type = str(typ)

    tmod.device = _Device
    tmod.load = lambda *a, **k: {}
    tmod.save = lambda *a, **k: None
    tmod.tensor = lambda *a, **k: object()
    tmod.zeros = lambda *a, **k: object()
    tmod.long = int
    sys.modules.setdefault("torch", tmod)
    nn_mod = types.ModuleType("torch.nn")
    nn_mod.__spec__ = importlib.machinery.ModuleSpec("torch.nn", None)
    nn_mod.__path__ = []
    nn_mod.Module = type("Module", (), {})
    utils_module = types.ModuleType("torch.nn.utils")
    utils_module.__spec__ = importlib.machinery.ModuleSpec("torch.nn.utils", None)
    utils_module.rnn = types.SimpleNamespace(pad_sequence=lambda *a, **k: None)
    sys.modules.setdefault("torch.nn.utils", utils_module)
    sys.modules.setdefault("torch.nn.utils.rnn", utils_module.rnn)
    nn_mod.utils = utils_module
    sys.modules.setdefault("torch.nn", nn_mod)
    opt_mod = types.ModuleType("torch.optim")
    opt_mod.__spec__ = importlib.machinery.ModuleSpec("torch.optim", None)
    opt_mod.__path__ = []
    sys.modules.setdefault("torch.optim", opt_mod)
    utils_mod = types.ModuleType("torch.utils")
    utils_mod.__spec__ = importlib.machinery.ModuleSpec("torch.utils", None)
    utils_mod.__path__ = []
    sys.modules.setdefault("torch.utils", utils_mod)
    tud = types.ModuleType("torch.utils.data")
    tud.__spec__ = importlib.machinery.ModuleSpec("torch.utils.data", None)
    tud.Dataset = object
    tud.DataLoader = object
    sys.modules.setdefault("torch.utils.data", tud)
if importlib.util.find_spec("sklearn") is None:
    sk_mod = types.ModuleType("sklearn")
    sk_mod.linear_model = types.SimpleNamespace(LinearRegression=object)
    sk_mod.ensemble = types.SimpleNamespace(
        GradientBoostingRegressor=object, RandomForestRegressor=object
    )
    sk_mod.cluster = types.SimpleNamespace(KMeans=object, DBSCAN=object)
    sys.modules.setdefault("sklearn", sk_mod)
    sys.modules.setdefault("sklearn.linear_model", sk_mod.linear_model)
    sys.modules.setdefault("sklearn.ensemble", sk_mod.ensemble)
    sys.modules.setdefault("sklearn.cluster", sk_mod.cluster)
if importlib.util.find_spec("pytorch_lightning") is None:
    pl = types.ModuleType("pytorch_lightning")
    pl.__spec__ = importlib.machinery.ModuleSpec("pytorch_lightning", None)
    pl.callbacks = types.SimpleNamespace(Callback=object)
    pl.LightningModule = type("LightningModule", (), {})
    pl.LightningDataModule = type("LightningDataModule", (), {})
    pl.Trainer = type("Trainer", (), {"fit": lambda *a, **k: None})
    sys.modules.setdefault("pytorch_lightning", pl)

from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.agents.llm_reasoner import LLMReasoner
from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


class DummyModel:
    def generate(self, ids, max_length=None, num_beams=2, do_sample=False):
        return ids


class DummyTokenizer:
    def __call__(self, text, return_tensors=None, truncation=True, max_length=None):
        return {"input_ids": [[0]]}

    def decode(self, ids, skip_special_tokens=True):
        return "good news"


def test_bias_output(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.llm_reasoner.AutoTokenizer.from_pretrained",
        lambda m: DummyTokenizer(),
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.llm_reasoner.AutoModelForCausalLM.from_pretrained",
        lambda m: DummyModel(),
    )
    async def fake_headlines(*a, **k):
        return ["headline"]

    monkeypatch.setattr(
        "solhunter_zero.agents.llm_reasoner.fetch_headlines_async", fake_headlines
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.llm_reasoner.compute_sentiment", lambda t: 0.8
    )

    agent = LLMReasoner(feeds=["f"])
    mgr = AgentManager([agent], executor=ExecutionAgent(rate_limit=0))
    pf = DummyPortfolio()
    actions = asyncio.run(mgr.evaluate("TOK", pf))
    assert actions and actions[0]["bias"] == pytest.approx(0.8)
