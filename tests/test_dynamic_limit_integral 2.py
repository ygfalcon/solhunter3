import types
from solhunter_zero import dynamic_limit


def test_integral_reaches_target_faster(monkeypatch):
    monkeypatch.setattr(
        dynamic_limit.psutil,
        "virtual_memory",
        lambda: types.SimpleNamespace(percent=0.0),
    )
    dynamic_limit._CPU_EMA = 0.0
    dynamic_limit._ERR_INT = 0.0
    cur = 4
    steps_no_int = 0
    for _ in range(10):
        tgt = dynamic_limit._target_concurrency(90.0, 4, 40.0, 80.0)
        cur = dynamic_limit._step_limit(cur, tgt, 4, smoothing=0.5, ki=0.0)
        steps_no_int += 1
        if cur == tgt:
            break
    dynamic_limit._ERR_INT = 0.0
    cur = 4
    steps_int = 0
    for _ in range(10):
        tgt = dynamic_limit._target_concurrency(90.0, 4, 40.0, 80.0)
        cur = dynamic_limit._step_limit(cur, tgt, 4, smoothing=0.5, ki=0.5)
        steps_int += 1
        if cur == tgt:
            break
    assert steps_int < steps_no_int
