import datetime
import time
from types import SimpleNamespace
import numpy as np

from solhunter_zero.rl_training import _TradeDataset
from solhunter_zero.regime import detect_regime


def _generate_sample(n_trades: int = 100_000, n_snaps: int = 10_000):
    rng = np.random.default_rng(0)
    token_choices = [f"T{i}" for i in range(50)]
    trade_tokens = rng.choice(token_choices, size=n_trades)
    prices = rng.random(n_trades) * 10 + 1
    amounts = rng.random(n_trades) + 0.1
    sides = rng.choice(["buy", "sell"], size=n_trades)
    timestamps = np.arange(n_trades, dtype=float)
    trades = [
        SimpleNamespace(
            token=t,
            price=float(p),
            amount=float(a),
            side=s,
            timestamp=datetime.datetime.utcfromtimestamp(ts),
        )
        for t, p, a, s, ts in zip(trade_tokens, prices, amounts, sides, timestamps)
    ]

    snap_tokens = rng.choice(token_choices, size=n_snaps)
    snap_ts = np.sort(rng.uniform(0, n_trades, size=n_snaps))
    depth = rng.random(n_snaps)
    slip = rng.random(n_snaps) / 10
    imb = rng.random(n_snaps) - 0.5
    tx = rng.random(n_snaps)
    snaps = [
        SimpleNamespace(
            token=t,
            timestamp=datetime.datetime.utcfromtimestamp(ts),
            depth=float(d),
            slippage=float(s),
            imbalance=float(i),
            tx_rate=float(r),
        )
        for t, ts, d, s, i, r in zip(snap_tokens, snap_ts, depth, slip, imb, tx)
    ]
    return trades, snaps


def _regimes_python(tokens: np.ndarray, prices: np.ndarray, weight: float) -> np.ndarray:
    hist: dict[str, list[float]] = {}
    out = np.zeros(len(tokens), dtype=np.float32)
    for i, (tok, p) in enumerate(zip(tokens, prices)):
        seq = hist.setdefault(tok, [])
        regime = detect_regime(seq)
        if regime == "bull":
            out[i] = weight
        elif regime == "bear":
            out[i] = -weight
        seq.append(float(p))
    return out


def build_old(trades, snaps) -> float:
    start = time.time()
    trade_list = list(trades)
    tokens = np.array([t.token for t in trade_list])
    prices = np.array([float(t.price) for t in trade_list], dtype=np.float32)
    _ = _regimes_python(tokens, prices, 1.0)
    _TradeDataset(trades, snaps)
    return time.time() - start


def build_new(trades, snaps) -> float:
    start = time.time()
    _TradeDataset(trades, snaps)
    return time.time() - start


def main() -> None:
    trades, snaps = _generate_sample()
    t_old = build_old(trades, snaps)
    t_new = build_new(trades, snaps)
    print(f"baseline build time: {t_old:.3f}s")
    print(f"vectorized build time: {t_new:.3f}s")


if __name__ == "__main__":  # pragma: no cover
    main()
