import pytest

from solhunter_zero.risk import (
    RiskManager,
    value_at_risk,
    conditional_value_at_risk_prices,
    recent_conditional_value_at_risk,
    covariance_matrix,
    portfolio_cvar,
    portfolio_variance,
    hedge_ratio,
    recent_value_at_risk,
)
from solhunter_zero.memory import Memory
from solhunter_zero.portfolio import calculate_order_size, Portfolio


@pytest.fixture(autouse=True)
def _no_fee(monkeypatch):
    monkeypatch.setattr("solhunter_zero.gas.get_current_fee", lambda testnet=False: 0.0)


def test_risk_manager_adjustments_reduce_size():
    rm = RiskManager(
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
        max_drawdown=1.0,
        volatility_factor=1.0,
    )
    base = rm.adjusted(0.0, 0.0)
    base_size = calculate_order_size(
        100.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=base.risk_tolerance,
        max_allocation=base.max_allocation,
        max_risk_per_token=base.max_risk_per_token,
    )
    adjusted = rm.adjusted(drawdown=0.5, volatility=1.0)
    adj_size = calculate_order_size(
        100.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=adjusted.risk_tolerance,
        max_allocation=adjusted.max_allocation,
        max_risk_per_token=adjusted.max_risk_per_token,
    )
    assert adj_size < base_size


def test_risk_multiplier_increases_size():
    rm = RiskManager(
        risk_tolerance=0.1,
        max_allocation=0.2,
        max_risk_per_token=0.2,
        risk_multiplier=2.0,
    )
    params = rm.adjusted(0.0, 0.0)
    size = calculate_order_size(
        100.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=params.risk_tolerance,
        max_allocation=params.max_allocation,
        max_risk_per_token=params.max_risk_per_token,
    )
    assert size > 0.0
    assert params.risk_tolerance > 0.1


def test_risk_manager_new_metrics():
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.2, max_risk_per_token=0.2)
    base = rm.adjusted(0.0, 0.0)
    high = rm.adjusted(0.0, 0.0, volume_spike=2.0)
    low = rm.adjusted(0.0, 0.0, depth_change=-1.0, whale_activity=1.0, tx_rate=0.5)
    burst = rm.adjusted(0.0, 0.0, tx_rate=2.0)
    assert high.risk_tolerance > base.risk_tolerance
    assert low.risk_tolerance < base.risk_tolerance
    assert burst.risk_tolerance > base.risk_tolerance


def test_low_portfolio_scales_risk():
    rm = RiskManager(
        risk_tolerance=0.1,
        max_allocation=0.2,
        max_risk_per_token=0.2,
        min_portfolio_value=20.0,
    )
    high = rm.adjusted(0.0, 0.0, portfolio_value=100.0)
    low = rm.adjusted(0.0, 0.0, portfolio_value=10.0)
    assert low.risk_tolerance < high.risk_tolerance
    size_low = calculate_order_size(
        10.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=low.risk_tolerance,
        max_allocation=low.max_allocation,
        max_risk_per_token=low.max_risk_per_token,
        min_portfolio_value=low.min_portfolio_value,
    )
    assert size_low < calculate_order_size(
        100.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=high.risk_tolerance,
        max_allocation=high.max_allocation,
        max_risk_per_token=high.max_risk_per_token,
        min_portfolio_value=high.min_portfolio_value,
    )


def test_extreme_metric_values():
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.2, max_risk_per_token=0.2)
    base = rm.adjusted(0.0, 0.0)

    high_rate = rm.adjusted(0.0, 0.0, tx_rate=5.0)
    assert high_rate.risk_tolerance > base.risk_tolerance

    deep = rm.adjusted(0.0, 0.0, depth_change=3.0)
    assert deep.risk_tolerance < base.risk_tolerance

    whales = rm.adjusted(0.0, 0.0, whale_activity=5.0)
    assert whales.risk_tolerance < base.risk_tolerance


def test_optional_metrics_adjustment():
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.2, max_risk_per_token=0.2)
    base = rm.adjusted(0.0, 0.0)
    good = rm.adjusted(0.0, 0.0, funding_rate=0.05, sentiment=0.5, token_age=30)
    bad = rm.adjusted(0.0, 0.0, funding_rate=-0.05, sentiment=-0.5, token_age=1)
    assert good.risk_tolerance > base.risk_tolerance
    assert bad.risk_tolerance < base.risk_tolerance


def test_tx_rate_prediction(monkeypatch):
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.2, max_risk_per_token=0.2)
    base = rm.adjusted(tx_rate=1.0)
    higher = rm.adjusted(tx_rate=1.0, tx_rate_pred=2.0)
    assert higher.risk_tolerance > base.risk_tolerance


def test_from_config_parses_new_fields():
    cfg = {
        "funding_rate_factor": "2.0",
        "sentiment_factor": "0.5",
        "token_age_factor": "10",
    }
    rm = RiskManager.from_config(cfg)
    assert rm.funding_rate_factor == 2.0
    assert rm.sentiment_factor == 0.5
    assert rm.token_age_factor == 10.0


def test_value_at_risk_calculation():
    prices = [100, 110, 100, 90, 95]
    var = value_at_risk(prices, 0.95)
    assert var == pytest.approx(0.1)


def test_var_logging():
    prices = [100, 110, 100, 90, 95]
    mem = Memory('sqlite:///:memory:')
    value_at_risk(prices, 0.95, memory=mem)
    logs = mem.list_vars()
    assert logs and logs[-1].value == pytest.approx(0.1)


def test_var_reduces_order_size():
    prices = [100, 110, 100, 90, 95]
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.5, max_risk_per_token=0.5)
    base = rm.adjusted(0.0, 0.0)
    base_size = calculate_order_size(
        100.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=base.risk_tolerance,
        max_allocation=base.max_allocation,
        max_risk_per_token=base.max_risk_per_token,
    )
    params = rm.adjusted(0.0, 0.0, prices=prices, var_threshold=0.05)
    var_size = calculate_order_size(
        100.0,
        1.0,
        0.0,
        0.0,
        risk_tolerance=params.risk_tolerance,
        max_allocation=params.max_allocation,
        max_risk_per_token=params.max_risk_per_token,
    )
    assert params.risk_tolerance < base.risk_tolerance
    assert var_size < base_size


def test_recent_var_function():
    prices = [100, 110, 100, 90, 95, 96, 97]
    var_full = recent_value_at_risk(prices, window=5, confidence=0.95)
    assert var_full > 0


def test_conditional_value_at_risk_prices():
    prices = [100, 110, 100, 90, 95]
    cvar = conditional_value_at_risk_prices(prices, 0.95)
    assert cvar == pytest.approx(0.1)


def test_recent_cvar_function():
    prices = [100, 110, 100, 90, 95, 96, 97]
    cvar_recent = recent_conditional_value_at_risk(prices, window=5, confidence=0.95)
    assert cvar_recent > 0


def test_asset_cvar_adjustment():
    prices = [100, 110, 100, 90, 95]
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.5, max_risk_per_token=0.5)
    base = rm.adjusted()
    adjusted = rm.adjusted(prices=prices, asset_cvar_threshold=0.05)
    assert adjusted.risk_tolerance < base.risk_tolerance


def test_order_size_scaled_by_var():
    size = calculate_order_size(
        100.0,
        1.0,
        var=0.1,
        var_threshold=0.05,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    base = calculate_order_size(
        100.0,
        1.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    assert size < base


def test_portfolio_covariance_and_variance():
    prices = {
        "a": [1, 2, 3],
        "b": [2, 1, 2],
    }
    cov = covariance_matrix(prices)
    assert cov.shape == (2, 2)
    var = portfolio_variance(cov, [0.5, 0.5])
    assert var > 0


def test_cross_token_cvar_adjustment():
    prices = {"a": [1, 1.1, 1.0], "b": [2, 1.8, 1.9]}
    weights = {"a": 0.5, "b": 0.5}
    cvar = portfolio_cvar(prices, weights, confidence=0.9)
    rm = RiskManager()
    high = rm.adjusted(portfolio_cvar=cvar, cvar_threshold=cvar / 2)
    assert high.risk_tolerance < rm.risk_tolerance


def test_leverage_and_correlation_scaling():
    rm = RiskManager()
    hedged = rm.adjusted(leverage=2.0, correlation=-0.5)
    assert hedged.risk_tolerance > rm.risk_tolerance


def test_hedge_ratio_and_portfolio_metrics():
    pf = Portfolio(path=None)
    pf.add("a", 1, 1.0)
    pf.add("b", 1, 2.0)
    seq = [
        {"a": 1.0, "b": 2.0},
        {"a": 2.0, "b": 4.0},
        {"a": 3.0, "b": 6.0},
    ]
    for p in seq:
        pf.record_prices(p)
    pf.update_risk_metrics()
    hr = hedge_ratio(pf.price_history["a"], pf.price_history["b"])
    assert hr == pytest.approx(1.0)
    rm = RiskManager()
    adj = rm.adjusted(portfolio_metrics=pf.risk_metrics)
    assert adj.risk_tolerance < rm.risk_tolerance

def test_var_forecast_adjustment(tmp_path, monkeypatch):
    from solhunter_zero.models.var_forecaster import VaRForecaster, save_var_model
    import pytest
    torch = pytest.importorskip("torch")
    model = VaRForecaster(seq_len=2, hidden_dim=2, num_layers=1)
    with torch.no_grad():
        for p in model.parameters():
            p.zero_()
        model.fc.bias.fill_(0.1)
    path = tmp_path / "var.pt"
    save_var_model(model, path)
    monkeypatch.setenv("VAR_MODEL_PATH", str(path))
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.2, max_risk_per_token=0.2)
    prices = [1.0, 1.0, 1.0]
    base = rm.adjusted()
    adj = rm.adjusted(prices=prices, var_threshold=0.05)
    assert adj.risk_tolerance < base.risk_tolerance

