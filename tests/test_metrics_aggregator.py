import solhunter_zero.metrics_aggregator as ma
from solhunter_zero import event_bus


def test_metrics_aggregator_combines_remote():
    ma._HISTORY.clear()
    ma._def_subs = []
    ma.start()

    results = []
    unsub = event_bus.subscribe('system_metrics_combined', lambda p: results.append(p))

    event_bus.publish('system_metrics', {'cpu': 50.0, 'memory': 60.0})
    event_bus.publish('remote_system_metrics', {'cpu': 100.0, 'memory': 120.0})

    unsub()

    assert results
    last = results[-1]
    assert abs(last['cpu'] - 75.0) < 1e-6
    assert abs(last['memory'] - 90.0) < 1e-6
