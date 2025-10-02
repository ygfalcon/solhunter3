import pytest
from solhunter_zero.portfolio import (
    Portfolio,
    calculate_order_size,
    dynamic_order_size,
)
from solhunter_zero.risk import RiskManager


@pytest.fixture(autouse=True)
def _no_fee(monkeypatch):
    monkeypatch.setattr("solhunter_zero.gas.get_current_fee", lambda testnet=False: 0.0)


def test_portfolio_update_and_pnl(tmp_path):
    path = tmp_path / "p.json"
    p = Portfolio(path=str(path))
    p.add("tok", 1, 1.0)
    p.update("tok", 1, 3.0)
    assert p.balances["tok"].amount == 2
    assert p.balances["tok"].entry_price == pytest.approx(2.0)
    pnl = p.unrealized_pnl({"tok": 4.0})
    assert pnl == pytest.approx(4.0)
    p.update("tok", -2, 4.0)
    assert "tok" not in p.balances


def test_portfolio_persistence(tmp_path):
    path = tmp_path / "pf.json"
    p1 = Portfolio(path=str(path))
    p1.add("tok", 1, 1.5)

    p2 = Portfolio(path=str(path))
    assert "tok" in p2.balances
    assert p2.balances["tok"].amount == 1
    assert p2.balances["tok"].entry_price == pytest.approx(1.5)


def test_position_roi():
    p = Portfolio(path=None)
    p.add("tok", 2, 1.0)
    roi = p.position_roi("tok", 1.5)
    assert roi == pytest.approx(0.5)


def test_calculate_order_size_basic():
    size = calculate_order_size(100.0, 1.0, risk_tolerance=0.1, max_allocation=0.2)
    assert size == pytest.approx(10.0)


def test_calculate_order_size_caps():
    size = calculate_order_size(100.0, 5.0, risk_tolerance=0.1, max_allocation=0.2)
    assert size == pytest.approx(10.0)


def test_calculate_order_size_negative_roi():
    assert calculate_order_size(100.0, -0.5) == 0.0


def test_calculate_order_size_risk_controls():
    base = calculate_order_size(
        100.0,
        1.0,
        volatility=0.0,
        drawdown=0.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    high_vol = calculate_order_size(
        100.0,
        1.0,
        volatility=1.0,
        drawdown=0.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    high_dd = calculate_order_size(
        100.0,
        1.0,
        volatility=0.0,
        drawdown=0.5,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    capped = calculate_order_size(
        100.0,
        10.0,
        volatility=0.0,
        drawdown=0.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.1,
    )

    assert high_vol < base
    assert high_dd < base
    assert capped == pytest.approx(10.0)


def test_portfolio_drawdown():
    p = Portfolio(path=None)
    p.add("tok", 1, 1.0)
    p.update_drawdown({"tok": 1.0})
    assert p.current_drawdown({"tok": 1.0}) == pytest.approx(0.0)
    p.update_drawdown({"tok": 2.0})
    assert p.current_drawdown({"tok": 2.0}) == pytest.approx(0.0)
    p.update_drawdown({"tok": 1.0})
    assert p.current_drawdown({"tok": 1.0}) == pytest.approx(0.5)


def test_trailing_stop_refreshes_highs():
    p = Portfolio(path=None)
    p.add("tok", 1.0, 100.0)

    # Rally updates the tracked high water mark via recorded prices.
    p.record_prices({"tok": 120.0})
    p.record_prices({"tok": 150.0})

    assert p.balances["tok"].high_price == pytest.approx(150.0)
    assert p.max_value == pytest.approx(150.0)

    # Pullback should only trip the stop once price crosses the refreshed high.
    p.record_prices({"tok": 140.0})
    assert not p.trailing_stop_triggered("tok", 140.0, 0.1)
    assert p.trailing_stop_triggered("tok", 134.0, 0.1)


def test_calculate_order_size_with_gas():
    size = calculate_order_size(
        100.0,
        1.0,
        risk_tolerance=0.1,
        max_allocation=0.2,
        gas_cost=1.0,
    )
    assert size == pytest.approx(9.0)


def test_percent_allocated():
    p = Portfolio(path=None)
    p.add("tok", 2, 1.0)
    p.add("oth", 3, 2.0)
    alloc = p.percent_allocated("tok")
    assert alloc == pytest.approx(2 / (2 + 6))
    alloc_prices = p.percent_allocated("tok", {"tok": 2.0, "oth": 2.0})
    assert alloc_prices == pytest.approx(4 / (4 + 6))


def test_order_size_respects_allocation():
    size = calculate_order_size(
        100.0,
        1.0,
        risk_tolerance=0.5,
        max_allocation=0.2,
        max_risk_per_token=0.5,
        current_allocation=0.15,
    )
    # Only 5% allocation left
    assert size == pytest.approx(5.0)


def test_small_portfolio_min_value():
    size = calculate_order_size(
        9.0,
        1.0,
        risk_tolerance=0.1,
        max_allocation=0.2,
        gas_cost=0.1,
        min_portfolio_value=10.0,
    )
    assert size == pytest.approx(0.9)


def test_trade_blocked_when_below_fee():
    size = calculate_order_size(
        9.0,
        0.5,
        risk_tolerance=0.1,
        max_allocation=0.2,
        gas_cost=1.0,
        min_portfolio_value=10.0,
    )
    assert size == 0.0


def test_portfolio_updated_event(tmp_path):
    path = tmp_path / "pf.json"
    p = Portfolio(path=str(path))
    events = []

    from solhunter_zero.event_bus import subscribe
    from solhunter_zero.schemas import PortfolioUpdated

    unsub = subscribe("portfolio_updated", lambda e: events.append(e))

    p.update("tok", 1, 1.0)
    assert events
    assert isinstance(events[-1], PortfolioUpdated)
    assert events[-1].balances.get("tok") == 1
    unsub()


def test_risk_metrics_event(monkeypatch):
    p = Portfolio(path=None)
    p.add("tok", 1, 1.0)
    p.add("oth", 1, 1.0)
    p.record_prices({"tok": 1.0, "oth": 1.0})
    p.record_prices({"tok": 1.1, "oth": 1.2})

    events = []
    from solhunter_zero.event_bus import subscribe

    unsub = subscribe("risk_metrics", lambda e: events.append(e))

    p.update_risk_metrics()
    unsub()

    assert events
    ev = events[-1]
    import math

    if math.isnan(p.risk_metrics["covariance"]):
        assert math.isnan(ev["covariance"])
    else:
        assert ev["covariance"] == p.risk_metrics["covariance"]

    if math.isnan(p.risk_metrics["portfolio_cvar"]):
        assert math.isnan(ev["portfolio_cvar"])
    else:
        assert ev["portfolio_cvar"] == p.risk_metrics["portfolio_cvar"]

    if math.isnan(p.risk_metrics["portfolio_evar"]):
        assert math.isnan(ev["portfolio_evar"])
    else:
        assert ev["portfolio_evar"] == p.risk_metrics["portfolio_evar"]

    if math.isnan(p.risk_metrics["correlation"]):
        assert math.isnan(ev["correlation"])
    else:
        assert ev["correlation"] == p.risk_metrics["correlation"]
    for a_row, b_row in zip(ev["cov_matrix"], p.risk_metrics["cov_matrix"].tolist()):
        for a_val, b_val in zip(a_row, b_row):
            if math.isnan(b_val):
                assert math.isnan(a_val)
            else:
                assert a_val == b_val
    for a_row, b_row in zip(ev["corr_matrix"], p.risk_metrics["corr_matrix"].tolist()):
        for a_val, b_val in zip(a_row, b_row):
            if math.isnan(b_val):
                assert math.isnan(a_val)
            else:
                assert a_val == b_val


def test_dynamic_order_size_prediction_boost():
    base = dynamic_order_size(
        100.0,
        0.1,
        None,
        volatility=1.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    boosted = dynamic_order_size(
        100.0,
        0.1,
        0.2,
        volatility=1.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    assert boosted > base


def test_dynamic_order_size_negative_prediction():
    size = dynamic_order_size(
        100.0,
        0.1,
        -0.2,
        volatility=1.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    assert size == 0.0


def test_dynamic_order_size_drawdown_and_volatility():
    base = dynamic_order_size(
        100.0,
        0.1,
        0.1,
        volatility=0.5,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    high_vol = dynamic_order_size(
        100.0,
        0.1,
        0.1,
        volatility=1.5,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    high_dd = dynamic_order_size(
        100.0,
        0.1,
        0.1,
        volatility=0.5,
        drawdown=0.8,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    assert high_vol < base
    assert high_dd < base


def test_calculate_order_size_predicted_variance():
    base = calculate_order_size(
        100.0,
        1.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
    )
    var_scaled = calculate_order_size(
        100.0,
        1.0,
        risk_tolerance=0.1,
        max_allocation=0.5,
        max_risk_per_token=0.5,
        predicted_var=0.5,
    )
    assert var_scaled < base


def test_calculate_order_size_risk_manager_metrics():
    rm = RiskManager(risk_tolerance=0.1, max_allocation=0.5, max_risk_per_token=0.5)
    base = calculate_order_size(
        100.0,
        1.0,
        risk_tolerance=rm.risk_tolerance,
        max_allocation=rm.max_allocation,
        max_risk_per_token=rm.max_risk_per_token,
    )
    metrics = {"correlation": 0.9}
    dynamic = calculate_order_size(
        100.0,
        1.0,
        risk_manager=rm,
        risk_metrics=metrics,
        max_risk_per_token=rm.max_risk_per_token,
    )
    assert dynamic < base


