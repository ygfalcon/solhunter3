import json
import asyncio
import logging
from pathlib import Path

import pytest

pytest.importorskip("torch.nn.utils.rnn")
pytest.importorskip("transformers")

from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.agents.attention_swarm import AttentionSwarm
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero import event_bus

event_bus.websockets = None
event_bus._encode_event = lambda *a, **k: b""


def test_update_weights_persists(tmp_path):
    path = tmp_path / "w.json"
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    cfg = AgentManagerConfig(memory_agent=mem_agent, weights={"a": 1.0}, weights_path=str(path))
    mgr = AgentManager([], config=cfg)

    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a")
    mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a")

    mgr.update_weights()

    assert path.exists()
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    assert data.get("a", 0) > 1.0


def test_rl_weights_event_updates_manager(tmp_path):
    path = tmp_path / "w.json"
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    cfg = AgentManagerConfig(memory_agent=mem_agent, weights={}, weights_path=str(path))
    mgr = AgentManager([], config=cfg)

    from solhunter_zero.event_bus import publish
    from solhunter_zero.schemas import RLWeights

    publish("rl_weights", RLWeights(weights={"b": 2.0}, risk={"risk_multiplier": 1.1}))
    asyncio.run(asyncio.sleep(0))

    assert mgr.weights.get("b") == 2.0
    assert path.exists()


def test_malformed_rl_policy_logs_warning(tmp_path, monkeypatch, caplog):
    bad = tmp_path / "rl_policy.json"
    bad.write_text("{bad json")
    monkeypatch.setenv("RL_POLICY_PATH", str(bad))
    cfg = AgentManagerConfig()
    mgr = AgentManager([], config=cfg)
    with caplog.at_level(logging.WARNING, logger="solhunter_zero.agent_manager"):
        conf = mgr._get_rl_policy_confidence()
    assert conf == {}
    assert any("rl policy" in r.message.lower() for r in caplog.records)


def test_rotate_weight_configs_selects_best(tmp_path):
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1")
    mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a1")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a2")
    mem.log_trade(token="tok", direction="sell", amount=1, price=0.5, reason="a2")
    mem_agent = MemoryAgent(mem)

    w1 = tmp_path / "w1.json"
    w1.write_text('{"a1": 1.0, "a2": 1.0}')
    w2 = tmp_path / "w2.json"
    w2.write_text('{"a1": 2.0, "a2": 0.5}')

    cfg = AgentManagerConfig(
        memory_agent=mem_agent,
        weights_path=str(tmp_path / "active.json"),
        weight_config_paths=[str(w1), str(w2)],
    )
    mgr = AgentManager([], config=cfg)

    mgr.rotate_weight_configs()
    assert mgr.weights["a1"] == 2.0
    assert mgr.weights["a2"] == 0.5


def test_attention_swarm_device_env(monkeypatch, tmp_path):
    from solhunter_zero import agent_manager as am
    called = {}

    def fake_load(path, *, device="cpu"):
        called["device"] = device
        return AttentionSwarm(1, device=device)

    monkeypatch.setattr(am, "load_model", fake_load)
    monkeypatch.setenv("ATTENTION_SWARM_DEVICE", "cuda")
    monkeypatch.setattr(am.torch.cuda, "is_available", lambda: True)
    model_path = tmp_path / "m.pt"
    model_path.write_text("x")
    cfg = AgentManagerConfig(use_attention_swarm=True, attention_model_path=str(model_path))
    AgentManager([], config=cfg)
    assert called["device"] == "cuda"


def test_close_persists_mutation_state(tmp_path):
    from solhunter_zero.agents.conviction import ConvictionAgent

    state_path = tmp_path / "state.json"
    db1 = tmp_path / "mem1.db"
    idx1 = tmp_path / "idx1"
    mem = AdvancedMemory(url=f"sqlite:///{db1}", index_path=str(idx1))
    mem_agent = MemoryAgent(mem)
    base = ConvictionAgent(threshold=0.1)
    cfg = AgentManagerConfig(memory_agent=mem_agent, mutation_path=str(state_path))
    mgr = AgentManager([base, mem_agent], config=cfg)

    spawned = mgr.spawn_mutations(1)
    assert spawned
    mut_name = spawned[0].name

    mgr.close()

    db2 = tmp_path / "mem2.db"
    idx2 = tmp_path / "idx2"
    mem2 = AdvancedMemory(url=f"sqlite:///{db2}", index_path=str(idx2))
    mem_agent2 = MemoryAgent(mem2)
    cfg2 = AgentManagerConfig(memory_agent=mem_agent2, mutation_path=str(state_path))
    mgr2 = AgentManager([base, mem_agent2], config=cfg2)
    assert any(a.name == mut_name for a in mgr2.agents)


def test_from_config_loads_keypair(monkeypatch):
    import solhunter_zero.agent_manager as am

    class Dummy(am.BaseAgent):
        name = "dummy"

        async def propose_trade(self, *args, **kwargs):
            return []

    monkeypatch.setattr(am, "load_agent", lambda name: Dummy())
    sentinel = object()
    captured: dict[str, str] = {}

    def fake_load(path: str):
        captured["path"] = path
        return sentinel

    monkeypatch.setattr(am.wallet, "load_keypair", fake_load)

    cfg = {"agents": ["dummy"], "solana_keypair": "kp.json"}
    mgr = am.AgentManager.from_config(cfg)

    assert mgr is not None
    assert mgr.executor.keypair is sentinel
    assert mgr.keypair is sentinel
    assert Path(captured["path"]).name == "kp.json"
    assert mgr.keypair_path == captured["path"]

