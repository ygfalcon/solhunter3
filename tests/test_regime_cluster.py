import numpy as np
import pytest
pytest.importorskip("torch.nn.utils.rnn")

from solhunter_zero.regime import detect_regime


def test_cluster_regime_bull():
    prices = np.linspace(1, 2, 60)
    assert detect_regime(prices, window=10) == "bull"


def test_cluster_regime_bear():
    prices = np.linspace(2, 1, 60)
    assert detect_regime(prices, window=10) == "bear"
