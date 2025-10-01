import numpy as np
import pytest
torch = pytest.importorskip("torch")

from solhunter_zero import models


def test_load_and_predict(tmp_path):
    model = models.PriceModel(input_dim=4, hidden_dim=4, num_layers=1)
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()
        model.fc.bias.fill_(0.05)
    path = tmp_path / "model.pt"
    models.save_model(model, path)

    loaded = models.load_model(path)
    seq = np.zeros((5, 4), dtype=np.float32)
    pred = loaded.predict(seq)
    assert pred == pytest.approx(0.05)
