import numpy as np
import pytest
pytest.importorskip("torch.nn.utils.rnn")
from solhunter_zero.models.regime_classifier import train_regime_classifier


def test_train_regime_classifier_predicts_bull():
    prices = np.linspace(1.0, 2.0, 60).tolist()
    model = train_regime_classifier(
        prices, seq_len=10, epochs=40, threshold=0.01
    )
    pred = model.predict(prices[-10:])
    assert pred == "bull"
