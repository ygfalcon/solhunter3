import types
from solhunter_zero import dynamic_limit


def test_dynamic_limit_fast_response(monkeypatch):
    monkeypatch.setattr(dynamic_limit.psutil, "virtual_memory", lambda: types.SimpleNamespace(percent=0.0))

    def run(smooth: float) -> int:
        dynamic_limit._CPU_EMA = 0.0
        cur = 4
        for _ in range(2):
            tgt = dynamic_limit._target_concurrency(90.0, 4, 40.0, 80.0, smoothing=smooth)
            cur = dynamic_limit._step_limit(cur, tgt, 4, smoothing=smooth)
        return cur

    slow = run(0.15)
    fast = run(0.75)
    assert fast < slow
    assert fast <= 2
