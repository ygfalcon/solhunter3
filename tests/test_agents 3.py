import asyncio
import types

import pytest
pytest.importorskip("torch.nn.utils.rnn")
pytest.importorskip("transformers")
import numpy as np, faiss
if getattr(np, "_STUB", False) or getattr(faiss, "_STUB", False):
    pytest.skip("requires real numpy/faiss", allow_module_level=True)

from solhunter_zero.agents.simulation import SimulationAgent
from solhunter_zero.agents.conviction import ConvictionAgent
from solhunter_zero.agents.arbitrage import ArbitrageAgent
from solhunter_zero.agents.exit import ExitAgent
from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.agents.meta_conviction import MetaConvictionAgent
from solhunter_zero.agents.portfolio_agent import PortfolioAgent
from solhunter_zero.agents.swarm import AgentSwarm
from solhunter_zero.agents.dqn import DQNAgent
from solhunter_zero.memory import Memory
from solhunter_zero.advanced_memory import AdvancedMemory

from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.portfolio import Portfolio, Position
from solhunter_zero.simulation import SimulationResult


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_simulation_agent_buy(monkeypatch):
    agent = SimulationAgent(count=1)

    monkeypatch.setattr(
        'solhunter_zero.agents.simulation.run_simulations',
        lambda t, count=1, **_: [types.SimpleNamespace(expected_roi=0.5)],
    )
    monkeypatch.setattr(
        'solhunter_zero.agents.simulation.should_buy', lambda sims: True
    )
    monkeypatch.setattr(
        'solhunter_zero.agents.simulation.should_sell', lambda sims: False
    )
    async def fake_prices(tokens):
        return {next(iter(tokens)): 1.0}

    monkeypatch.setattr(
        'solhunter_zero.agents.simulation.fetch_token_prices_async',
        fake_prices,
    )

    actions = asyncio.run(agent.propose_trade('tok', DummyPortfolio()))
    assert actions and actions[0]['side'] == 'buy'


def test_conviction_agent_threshold(monkeypatch):
    agent = ConvictionAgent(threshold=0.1, count=1)
    monkeypatch.setattr(
        'solhunter_zero.agents.conviction.run_simulations',
        lambda t, count=1, **_: [types.SimpleNamespace(expected_roi=0.2)],
    )

    actions = asyncio.run(agent.propose_trade('tok', DummyPortfolio()))
    assert actions and actions[0]['side'] == 'buy'


def test_conviction_agent_uses_prediction(monkeypatch):
    agent = ConvictionAgent(threshold=0.05, count=1)

    monkeypatch.setattr(
        'solhunter_zero.agents.conviction.run_simulations',
        lambda t, count=1, **_: [SimulationResult(success_prob=1.0, expected_roi=0.06)],
    )
    monkeypatch.setattr(
        'solhunter_zero.agents.conviction.predict_price_movement',
        lambda t, *, sentiment=None, model_path=None: 0.04,
    )

    actions = asyncio.run(agent.propose_trade('tok', DummyPortfolio()))
    assert actions == []


async def fake_feed_low(token):
    return 1.0


async def fake_feed_high(token):
    return 1.2


def test_arbitrage_agent(monkeypatch):
    agent = ArbitrageAgent(threshold=0.1, amount=5, feeds=[fake_feed_low, fake_feed_high])
    actions = asyncio.run(agent.propose_trade('tok', DummyPortfolio()))
    assert {"side": "buy"} in [{"side": a['side']} for a in actions]
    assert {"side": "sell"} in [{"side": a['side']} for a in actions]
    venues = {a['venue'] for a in actions}
    assert len(venues) == 2


def test_dqn_arbitrage_memory_integration(tmp_path):
    async def _feed_low(token: str) -> float:
        return 0.6

    async def _feed_high(token: str) -> float:
        return 0.8

    mem = Memory("sqlite:///:memory:")
    mem_agent = MemoryAgent(mem)
    dqn = DQNAgent(memory_agent=mem_agent, epsilon=0.0, model_path=tmp_path / "dqn.pt")
    pf = DummyPortfolio()

    first = asyncio.run(dqn.propose_trade("tok", pf))
    assert first and first[0]["side"] == "buy"
    asyncio.run(mem_agent.log({"token": "tok", "side": "buy", "amount": 1.0, "price": 1.0, "agent": "dqn"}))
    pf.balances["tok"] = Position("tok", 1.0, 1.0, 1.0)

    arb_agent = ArbitrageAgent(threshold=0.0, amount=1.0, feeds=[_feed_low, _feed_high])
    arb_actions = asyncio.run(arb_agent.propose_trade("tok", pf))
    assert {a["side"] for a in arb_actions} == {"buy", "sell"}
    for action in arb_actions:
        asyncio.run(mem_agent.log(action))

    second = asyncio.run(dqn.propose_trade("tok", pf))
    assert second


def test_exit_agent_trailing(monkeypatch):
    pf = DummyPortfolio()
    pf.balances['tok'] = Position('tok', 10, 2.0, 3.0)

    async def price_high(tokens):
        return {'tok': 2.5}

    monkeypatch.setattr(
        'solhunter_zero.agents.exit.fetch_token_prices_async',
        price_high,
    )

    agent = ExitAgent(trailing=0.2)
    actions = asyncio.run(agent.propose_trade('tok', pf))
    assert actions == []

    async def price_low(tokens):
        return {'tok': 2.0}

    monkeypatch.setattr(
        'solhunter_zero.agents.exit.fetch_token_prices_async',
        price_low,
    )
    actions = asyncio.run(agent.propose_trade('tok', pf))
    assert actions and actions[0]['side'] == 'sell'


def test_exit_agent_stop_loss(monkeypatch):
    pf = DummyPortfolio()
    pf.balances['tok'] = Position('tok', 10, 10.0, 10.0)

    async def price_low(tokens):
        return {'tok': 8.0}

    monkeypatch.setattr(
        'solhunter_zero.agents.exit.fetch_token_prices_async',
        price_low,
    )

    agent = ExitAgent(stop_loss=0.2)
    actions = asyncio.run(agent.propose_trade('tok', pf))
    assert actions and actions[0]['side'] == 'sell'


def test_exit_agent_take_profit(monkeypatch):
    pf = DummyPortfolio()
    pf.balances['tok'] = Position('tok', 5, 10.0, 10.0)

    async def price_high(tokens):
        return {'tok': 12.0}

    monkeypatch.setattr(
        'solhunter_zero.agents.exit.fetch_token_prices_async',
        price_high,
    )

    agent = ExitAgent(take_profit=0.2)
    actions = asyncio.run(agent.propose_trade('tok', pf))
    assert actions and actions[0]['side'] == 'sell'


def test_execution_agent(monkeypatch):
    captured = {}

    async def fake_place(token, side, amount, price, **_):
        captured['side'] = side
        return {'ok': True}

    monkeypatch.setattr('solhunter_zero.agents.execution.place_order_async', fake_place)
    agent = ExecutionAgent(rate_limit=0)
    res = asyncio.run(agent.execute({'token': 'tok', 'side': 'buy', 'amount': 1.0, 'price': 1.0}))
    assert captured['side'] == 'buy'
    assert res == {'ok': True}


def test_agent_manager_execute(monkeypatch):
    async def buy_agent(token, portfolio, *, depth=None, imbalance=None):
        return [{'token': token, 'side': 'buy', 'amount': 1.0, 'price': 1.0}]

    class DummyAgent:
        name = 'dummy'

        async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
            return [{'token': token, 'side': 'sell', 'amount': 1.0, 'price': 1.5}]

    captured = []

    async def fake_place(token, side, amount, price, **_):
        captured.append((side, amount))
        return {'ok': True}

    monkeypatch.setattr('solhunter_zero.agents.execution.place_order_async', fake_place)
    exec_agent = ExecutionAgent(rate_limit=0)
    mgr = AgentManager(
        [types.SimpleNamespace(propose_trade=buy_agent, name='b'), DummyAgent()],
        executor=exec_agent,
    )

    pf = DummyPortfolio()
    asyncio.run(mgr.execute('tok', pf))
    # Buy and sell of equal size cancel out -> no orders executed
    assert captured == []


def test_execution_agent_event_enqueue(monkeypatch):
    async def fake_tx(self, *a, **k):
        return "TX"

    monkeypatch.setattr(
        "solhunter_zero.agents.execution.ExecutionAgent._create_signed_tx",
        fake_tx,
    )

    class DummyExec:
        def __init__(self):
            self.txs = []

        async def enqueue(self, tx):
            self.txs.append(tx)

    exec_agent = ExecutionAgent(rate_limit=0, depth_service=True, keypair=None)
    dummy = DummyExec()
    exec_agent.add_executor("tok", dummy)
    asyncio.run(
        exec_agent.execute({"token": "tok", "side": "buy", "amount": 1.0, "price": 1.0})
    )
    assert dummy.txs == ["TX"]


def test_agent_manager_event_executor(monkeypatch):
    async def fake_tx(self, *a, **k):
        return "TX"

    monkeypatch.setattr(
        "solhunter_zero.agents.execution.ExecutionAgent._create_signed_tx",
        fake_tx,
    )

    class DummyExec:
        def __init__(self, token, **_k):
            self.token = token
            self.txs = []

        async def enqueue(self, tx):
            self.txs.append(tx)

        async def run(self):
            pass

    monkeypatch.setattr("solhunter_zero.agent_manager.EventExecutor", DummyExec)

    async def buy(token, pf, *, depth=None, imbalance=None):
        return [{"token": token, "side": "buy", "amount": 1.0, "price": 1.0}]

    cfg = AgentManagerConfig(depth_service=True)
    mgr = AgentManager(
        [types.SimpleNamespace(propose_trade=buy, name="b")],
        executor=ExecutionAgent(rate_limit=0, depth_service=True, keypair=None),
        config=cfg,
    )

    pf = DummyPortfolio()
    asyncio.run(mgr.execute("tok", pf))
    assert mgr._event_executors["tok"].txs == ["TX"]


def test_agent_swarm_weighted(monkeypatch):
    async def buy_one(token, pf, *, depth=None, imbalance=None):
        return [{'token': token, 'side': 'buy', 'amount': 1.0, 'price': 1.0}]

    async def buy_two(token, pf, *, depth=None, imbalance=None):
        return [{'token': token, 'side': 'buy', 'amount': 1.0, 'price': 2.0}]

    swarm = AgentSwarm([
        types.SimpleNamespace(propose_trade=buy_one, name='a'),
        types.SimpleNamespace(propose_trade=buy_two, name='b'),
    ])
    actions = asyncio.run(swarm.propose('tok', DummyPortfolio(), weights={'a': 1.0, 'b': 2.0}))
    assert actions == [{
        'token': 'tok',
        'side': 'buy',
        'amount': 3.0,
        'price': pytest.approx(5 / 3),
    }]


def test_agent_swarm_conflict_cancel():
    async def buy(token, pf, *, depth=None, imbalance=None):
        return [{'token': token, 'side': 'buy', 'amount': 1.0, 'price': 1.0}]

    async def sell(token, pf, *, depth=None, imbalance=None):
        return [{'token': token, 'side': 'sell', 'amount': 1.0, 'price': 1.5}]

    swarm = AgentSwarm([
        types.SimpleNamespace(propose_trade=buy, name='b'),
        types.SimpleNamespace(propose_trade=sell, name='s'),
    ])
    actions = asyncio.run(swarm.propose('tok', DummyPortfolio()))
    assert actions == []


def test_memory_agent(monkeypatch):
    mem_agent = MemoryAgent()
    asyncio.run(mem_agent.log({'token': 'tok', 'side': 'buy', 'amount': 1.0, 'price': 2.0}))
    trades = asyncio.run(mem_agent.memory.list_trades())
    assert trades and trades[0].token == 'tok'


def test_agent_manager_update_weights():
    mem = Memory('sqlite:///:memory:')
    mem_agent = MemoryAgent(mem)
    cfg = AgentManagerConfig(memory_agent=mem_agent, weights={'a1': 1.0, 'a2': 1.0})
    mgr = AgentManager([], config=cfg)

    mem.log_trade(token='tok', direction='buy', amount=1, price=1, reason='a1')
    mem.log_trade(token='tok', direction='sell', amount=1, price=2, reason='a1')
    mem.log_trade(token='tok', direction='buy', amount=1, price=2, reason='a2')
    mem.log_trade(token='tok', direction='sell', amount=1, price=1, reason='a2')

    mgr.update_weights()

    assert mgr.weights['a1'] > 1.0
    assert mgr.weights['a2'] < 1.0


def test_agent_manager_update_weights_success_rate(tmp_path):
    db = tmp_path / "mem.db"
    idx = tmp_path / "idx.faiss"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    cfg = AgentManagerConfig(memory_agent=mem_agent, weights={"a1": 1.0, "a2": 1.0})
    mgr = AgentManager([], config=cfg)

    mem.log_simulation("a1", expected_roi=1.0, success_prob=0.8)
    mem.log_simulation("a2", expected_roi=1.0, success_prob=0.2)

    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a1")
    mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a1")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1, reason="a2")
    mem.log_trade(token="tok", direction="sell", amount=1, price=2, reason="a2")

    mgr.update_weights()

    assert mgr.weights["a1"] > mgr.weights["a2"]



def test_agent_manager_weights_persistence_json(tmp_path):
    path = tmp_path / "w.json"
    cfg = AgentManagerConfig(weights={"a": 2.0}, weights_path=str(path))
    mgr = AgentManager([], config=cfg)
    mgr.save_weights()

    mgr2 = AgentManager([], config=AgentManagerConfig(weights_path=str(path)))
    assert mgr2.weights == {"a": 2.0}


def test_agent_manager_weights_persistence_toml(tmp_path):
    path = tmp_path / "w.toml"
    cfg = AgentManagerConfig(weights={"a": 1.5}, weights_path=str(path))
    mgr = AgentManager([], config=cfg)
    mgr.save_weights()

    mgr2 = AgentManager([], config=AgentManagerConfig(weights_path=str(path)))
    assert mgr2.weights == {"a": 1.5}



def test_meta_conviction_majority_buy(monkeypatch):
    calls = []

    async def buy(self, token, pf, *, depth=None, imbalance=None):
        calls.append("b")
        return [{"token": token, "side": "buy", "amount": 1.0, "price": 0.0}]

    async def sell(self, token, pf, *, depth=None, imbalance=None):
        calls.append("s")
        return [{"token": token, "side": "sell", "amount": 1.0, "price": 0.0}]

    monkeypatch.setattr(SimulationAgent, "propose_trade", buy)
    monkeypatch.setattr(ConvictionAgent, "propose_trade", buy)
    monkeypatch.setattr(
        "solhunter_zero.agents.ramanujan_agent.RamanujanAgent.propose_trade",
        sell,
    )

    agent = MetaConvictionAgent()
    actions = asyncio.run(agent.propose_trade("tok", DummyPortfolio()))

    assert calls.count("b") == 2 and calls.count("s") == 1
    assert actions and actions[0]["side"] == "buy"


def test_meta_conviction_majority_sell(monkeypatch):
    async def buy(self, token, pf, *, depth=None, imbalance=None):
        return [{"token": token, "side": "buy", "amount": 1.0, "price": 0.0}]

    async def sell(self, token, pf, *, depth=None, imbalance=None):
        return [{"token": token, "side": "sell", "amount": 1.0, "price": 0.0}]

    monkeypatch.setattr(SimulationAgent, "propose_trade", sell)
    monkeypatch.setattr(ConvictionAgent, "propose_trade", sell)
    monkeypatch.setattr(
        "solhunter_zero.agents.ramanujan_agent.RamanujanAgent.propose_trade",
        buy,
    )

    pf = DummyPortfolio()
    pf.balances["tok"] = Position("tok", 2, 1.0, 1.0)
    agent = MetaConvictionAgent()
    actions = asyncio.run(agent.propose_trade("tok", pf))

    assert actions and actions[0]["side"] == "sell"


def test_portfolio_agent_sell():
    pf = DummyPortfolio()
    pf.balances["tok"] = Position("tok", 10, 1.0, 1.0)
    agent = PortfolioAgent(max_allocation=0.5)
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "sell"
    assert actions[0]["amount"] < 10


def test_portfolio_agent_buy():
    pf = DummyPortfolio()
    agent = PortfolioAgent(max_allocation=0.5)
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"


