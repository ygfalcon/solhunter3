import numpy as np
from solhunter_zero import models
import pytest


@pytest.mark.slow
def test_train_price_model_converges():
    prices = [1.0]
    for _ in range(20):
        prices.append(prices[-1] * 1.1)
    liquidity = np.linspace(100, 120, len(prices)).tolist()
    depth = np.linspace(1, 2, len(prices)).tolist()
    depth_total = np.linspace(1.5, 2.5, len(prices)).tolist()
    tx = np.linspace(10, 30, len(prices)).tolist()
    slippage = np.linspace(0.1, 0.2, len(prices)).tolist()
    volume = np.linspace(50, 100, len(prices)).tolist()
    mem = np.linspace(0.1, 1.0, len(prices)).tolist()
    whales = np.linspace(0.0, 0.5, len(prices)).tolist()
    spread = np.linspace(0.01, 0.02, len(prices)).tolist()

    model = models.train_price_model(
        prices,
        liquidity,
        depth,
        depth_total,
        tx,
        slippage=slippage,
        volume=volume,
        mempool_rates=mem,
        whale_shares=whales,
        spreads=spread,
        seq_len=5,
        epochs=50,
    )
    seq = np.column_stack([
        prices[-5:],
        liquidity[-5:],
        depth[-5:],
        depth_total[-5:],
        slippage[-5:],
        volume[-5:],
        tx[-5:],
        mem[-5:],
        whales[-5:],
        spread[-5:],
    ])
    pred = model.predict(seq)
    assert pred == pytest.approx(0.1, abs=0.02)
