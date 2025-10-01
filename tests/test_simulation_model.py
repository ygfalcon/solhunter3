import numpy as np
import pytest
torch = pytest.importorskip("torch")
from solhunter_zero.simulation import SimulationResult

from solhunter_zero import models, simulation


def setup_function(_):
    simulation.invalidate_simulation_models()


def test_run_simulations_uses_model(tmp_path, monkeypatch):
    model = models.PriceModel(input_dim=4, hidden_dim=4, num_layers=1)
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()
        model.fc.bias.fill_(0.1)
    model_path = tmp_path / "model.pt"
    models.save_model(model, model_path)

    hist = list(np.linspace(1.0, 1.2, 30))
    metrics = {
        "mean": 0.0,
        "volatility": 0.0,
        "volume": 10.0,
        "liquidity": 20.0,
        "slippage": 0.01,
        "depth": 1.0,
        "price_history": hist,
        "liquidity_history": hist,
        "depth_history": hist,
        "slippage_history": [0.01] * 30,
        "tx_count_history": [5] * 30,
    }

    monkeypatch.setattr(simulation, "fetch_token_metrics", lambda _t: metrics)
    async def metrics_async(_t):
        return metrics

    monkeypatch.setattr(simulation, "fetch_token_metrics_async", metrics_async)
    async def fake_fetch(_t):
        return {}

    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", fake_fetch
    )
    monkeypatch.setattr(simulation.np.random, "normal", lambda mean, vol, size: np.full(size, mean))
    monkeypatch.setenv("PRICE_MODEL_PATH", str(model_path))

    res = simulation.run_simulations("tok", count=1, days=2)[0]
    assert res.expected_roi > 0.2


def test_predict_price_movement_transformer(tmp_path, monkeypatch):
    model = models.TransformerModel(input_dim=10, hidden_dim=4, num_layers=1)
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()
        model.fc.bias.fill_(0.05)
    path = tmp_path / "tr.pt"
    models.save_model(model, path)

    hist = list(np.linspace(1.0, 1.3, 30))
    metrics = {
        "mean": 0.0,
        "volatility": 0.0,
        "volume": 10.0,
        "liquidity": 20.0,
        "slippage": 0.01,
        "depth": 1.0,
        "price_history": hist,
        "liquidity_history": hist,
        "depth_history": hist,
        "total_depth_history": hist,
        "slippage_history": [0.01] * 30,
        "volume_history": [10.0] * 30,
        "tx_count_history": [5] * 30,
        "mempool_rate_history": [0.0] * 30,
        "whale_share_history": [0.0] * 30,
        "spread_history": [0.0] * 30,
    }

    monkeypatch.setattr(simulation, "fetch_token_metrics", lambda _t: metrics)
    monkeypatch.setenv("PRICE_MODEL_PATH", str(path))
    val = simulation.predict_price_movement("tok")
    assert val == pytest.approx(0.05)


def test_predict_price_movement_returns_rate(monkeypatch):
    sims = [SimulationResult(success_prob=1.0, expected_roi=0.1, tx_rate=2.0)]
    monkeypatch.setattr(simulation, "run_simulations", lambda *a, **k: sims)
    roi, rate = simulation.predict_price_movement("tok", return_tx_rate=True)
    assert roi == pytest.approx(0.1)
    assert rate == pytest.approx(2.0)


def test_predict_token_activity_warning(monkeypatch, caplog):
    class BadModel:
        def predict(self, _feat):
            raise RuntimeError("boom")

    monkeypatch.setattr(simulation, "get_activity_model", lambda _p=None: BadModel())
    metrics = {
        "depth_change": 0.0,
        "tx_rate": 0.0,
        "whale_activity": 0.0,
        "avg_swap_size": 0.0,
    }

    with caplog.at_level("WARNING"):
        val = simulation.predict_token_activity("tok", metrics=metrics)
    assert val == 0.0
    assert any("predict_token_activity failed" in r.message for r in caplog.records)
