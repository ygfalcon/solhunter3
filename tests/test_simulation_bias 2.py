import numpy as np
import pytest
import importlib

simulation = importlib.import_module("solhunter_zero.simulation")


def setup_function(_):
    simulation.invalidate_simulation_models()


def test_bias_correction_applied(monkeypatch):
    # reset history
    simulation._TRADE_ROIS.clear()
    simulation._BIAS["mean"] = 0.0
    simulation._BIAS["volatility"] = 0.0

    simulation.log_trade_outcome(0.1)
    simulation.log_trade_outcome(0.3)

    bias = simulation.bias_correction()
    assert bias["mean"] == pytest.approx(0.2)
    assert bias["volatility"] == pytest.approx(0.1)

    def fake_metrics(token):
        return {
            "mean": 0.0,
            "volatility": 0.0,
            "volume": 50.0,
            "liquidity": 60.0,
            "slippage": 0.01,
        }

    monkeypatch.setattr(simulation, "fetch_token_metrics", fake_metrics)
    monkeypatch.setattr(
        simulation.onchain_metrics, "fetch_dex_metrics_async", lambda _t: {}
    )
    monkeypatch.setattr(simulation.np.random, "normal", lambda mean, vol, size: np.full(size, mean))

    res = simulation.run_simulations("tok", count=1, days=1)[0]
    assert res.expected_roi == pytest.approx(0.2)

