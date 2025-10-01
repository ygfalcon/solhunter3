import os
import time
import datetime as dt
from types import SimpleNamespace

import solhunter_zero.rl_training as rl_training


def _gen_data(n=200):
    now = dt.datetime.utcnow()
    trades = [
        SimpleNamespace(
            token=f"tok{i%5}",
            price=1.0,
            amount=1.0,
            side="buy" if i % 2 == 0 else "sell",
            timestamp=now,
        )
        for i in range(n)
    ]
    snaps = [
        SimpleNamespace(
            token=f"tok{i%5}",
            price=1.0,
            depth=1.0,
            total_depth=1.0,
            slippage=0.0,
            volume=1.0,
            imbalance=0.0,
            tx_rate=0.0,
            whale_share=0.0,
            spread=0.0,
            sentiment=0.0,
            timestamp=now,
        )
        for i in range(n // 2)
    ]
    return trades, snaps


def test_parallel_training_speed(tmp_path):
    trades, snaps = _gen_data()
    m1 = tmp_path / "m1.pt"
    start = time.perf_counter()
    rl_training.fit(trades, snaps, model_path=m1, device="cpu")
    t_single = time.perf_counter() - start

    os.environ["RL_WORKERS"] = "2"
    m2 = tmp_path / "m2.pt"
    start = time.perf_counter()
    rl_training.fit(trades, snaps, model_path=m2, device="cpu")
    t_multi = time.perf_counter() - start
    os.environ.pop("RL_WORKERS", None)

    assert t_multi <= t_single * 1.2
