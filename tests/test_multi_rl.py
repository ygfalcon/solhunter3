import pytest
pytest.importorskip("torch.nn.utils.rnn")
from solhunter_zero.multi_rl import PopulationRL
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig


def test_population_rl_evolves_and_persists(tmp_path):
    mem = Memory('sqlite:///:memory:')
    mem.log_trade(token='tok', direction='buy', amount=1, price=1, reason='a1')
    mem.log_trade(token='tok', direction='sell', amount=1, price=2, reason='a1')
    mem.log_trade(token='tok', direction='buy', amount=1, price=1, reason='a2')
    mem.log_trade(token='tok', direction='sell', amount=1, price=0.5, reason='a2')
    mem_agent = MemoryAgent(mem)

    path = tmp_path / 'weights.json'
    rl = PopulationRL(mem_agent, population_size=2, weights_path=str(path))
    rl.population = [
        {'weights': {'a1': 1.0, 'a2': 1.0}, 'risk': {'risk_multiplier': 1.0}},
        {'weights': {'a1': 0.5, 'a2': 1.5}, 'risk': {'risk_multiplier': 1.0}},
    ]
    best = rl.evolve()
    assert best['weights']['a1'] >= best['weights']['a2']
    assert path.exists()

    rl2 = PopulationRL(mem_agent, population_size=2, weights_path=str(path))
    assert rl2.population


def test_agent_manager_population_rl_updates_weights(tmp_path):
    mem = Memory('sqlite:///:memory:')
    mem.log_trade(token='tok', direction='buy', amount=1, price=1, reason='a1')
    mem.log_trade(token='tok', direction='sell', amount=1, price=2, reason='a1')
    mem.log_trade(token='tok', direction='buy', amount=1, price=1, reason='a2')
    mem.log_trade(token='tok', direction='sell', amount=1, price=0.5, reason='a2')
    mem_agent = MemoryAgent(mem)

    rl_path = tmp_path / 'rl_weights.json'
    rl = PopulationRL(mem_agent, population_size=2, weights_path=str(rl_path))
    rl.population = [
        {'weights': {'a1': 1.0, 'a2': 1.0}, 'risk': {'risk_multiplier': 1.0}},
        {'weights': {'a1': 0.5, 'a2': 1.5}, 'risk': {'risk_multiplier': 1.0}},
    ]

    mgr_path = tmp_path / 'mgr_weights.json'
    cfg = AgentManagerConfig(memory_agent=mem_agent, weights_path=str(mgr_path), population_rl=rl)
    mgr = AgentManager([], config=cfg)
    mgr.evolve(spawn_count=0)

    assert mgr.weights['a1'] >= mgr.weights['a2']
    assert mgr_path.exists()
