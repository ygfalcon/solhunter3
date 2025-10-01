import pytest

from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero import event_bus


def test_execution_agent_cpu_adjust():
    agent = ExecutionAgent(rate_limit=0.1, concurrency=4, min_rate=0.05, max_rate=0.2)
    agent._smoothing = 1.0
    # high CPU usage should reduce concurrency and increase rate limit
    event_bus.publish("system_metrics_combined", {"cpu": 90.0})
    assert agent.rate_limit > 0.15
    assert agent._sem._value == 1
    # low CPU restores limits
    event_bus.publish("system_metrics_combined", {"cpu": 0.0})
    assert agent.rate_limit == pytest.approx(0.05)
    assert agent._sem._value == 4
