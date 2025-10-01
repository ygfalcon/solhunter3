from pathlib import Path
from solhunter_zero.backtester import backtest_configs, DEFAULT_STRATEGIES
from solhunter_zero import backtest_cli


def test_backtest_cli_with_dates_and_config(tmp_path):
    h_path = Path(__file__).parent / "data" / "prices.json"
    prices, liquidity = backtest_cli._load_history(
        str(h_path), "2023-01-02", "2023-01-04"
    )
    assert prices == [101.2, 101.05, 100.56]
    assert liquidity is None

    cfg_path = tmp_path / "cfg.toml"
    cfg_path.write_text("""[agent_weights]
buy_hold = 1.0
momentum = 1.0
""")

    res = backtest_configs(
        prices,
        [("cfg.toml", {"buy_hold": 1.0, "momentum": 1.0})],
        DEFAULT_STRATEGIES,
        liquidity=liquidity,
    )[0]
    assert res.roi != 0.0
    assert res.sharpe == res.sharpe


def test_backtest_cli_long_history():
    h_path = Path(__file__).parent / "data" / "prices_long.json"
    prices, liquidity = backtest_cli._load_history(str(h_path), None, None)
    assert len(prices) >= 180  # at least ~6 months of data
    res = backtest_configs(
        prices,
        [("cfg", {"buy_hold": 1.0, "momentum": 1.0})],
        DEFAULT_STRATEGIES,
        liquidity=liquidity,
    )[0]
    assert res.roi != 0.0
    assert res.sharpe != 0.0
