import json
import pytest
pytest.importorskip("torch.nn.utils.rnn")
from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.agents.conviction import ConvictionAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.advanced_memory import AdvancedMemory


def test_mutations_spawn_and_prune(tmp_path):
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    base = ConvictionAgent(threshold=0.1)
    path = tmp_path / 'state.json'
    cfg = AgentManagerConfig(memory_agent=mem_agent, mutation_path=str(path))
    mgr = AgentManager([base, mem_agent], config=cfg)

    mgr.spawn_mutations(1)
    active_names = [e["name"] for e in mgr.mutation_state.get("active", [])]
    mutated = [a for a in mgr.agents if a.name != base.name and a.name in active_names]
    assert mutated
    m = mutated[0]

    mem.log_trade(token='tok', direction='buy', amount=1, price=1, reason=m.name)
    mem.log_trade(token='tok', direction='sell', amount=1, price=0.5, reason=m.name)

    mgr.prune_underperforming(0.0)
    assert m.name not in [a.name for a in mgr.agents]

    mgr.save_mutation_state()
    data = json.loads(path.read_text())
    names = [e.get("name") for e in data.get('active', [])]
    assert m.name not in names


def test_mutation_state_persists(tmp_path):
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    base = ConvictionAgent(threshold=0.1)
    path = tmp_path / 'state.json'

    cfg = AgentManagerConfig(memory_agent=mem_agent, mutation_path=str(path))
    mgr = AgentManager([base, mem_agent], config=cfg)
    spawned = mgr.spawn_mutations(1)
    assert spawned
    mut_name = spawned[0].name
    mgr.save_mutation_state()

    del mgr

    cfg2 = AgentManagerConfig(memory_agent=mem_agent, mutation_path=str(path))
    mgr2 = AgentManager([base, mem_agent], config=cfg2)
    assert any(a.name == mut_name for a in mgr2.agents)
