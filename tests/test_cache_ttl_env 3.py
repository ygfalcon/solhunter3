import importlib
import pytest

CASES = [
    ("solhunter_zero.simulation", "TOKEN_METRICS_CACHE_TTL", "TOKEN_METRICS_CACHE", 30.0),
    ("solhunter_zero.simulation", "SIM_MODEL_CACHE_TTL", "SIM_MODEL_CACHE", 300.0),
    ("solhunter_zero.scanner_common", "TREND_CACHE_TTL", "TREND_CACHE", 60.0),
    ("solhunter_zero.scanner_common", "LISTING_CACHE_TTL", "LISTING_CACHE", 60.0),
    ("solhunter_zero.arbitrage", "PRICE_CACHE_TTL", "PRICE_CACHE", 30.0),
    ("solhunter_zero.arbitrage", "EDGE_CACHE_TTL", "_EDGE_CACHE", 60.0),
    ("solhunter_zero.onchain_metrics", "DEX_METRICS_CACHE_TTL", "DEX_METRICS_CACHE", 30.0),
    ("solhunter_zero.onchain_metrics", "TOKEN_VOLUME_CACHE_TTL", "TOKEN_VOLUME_CACHE", 30.0),
    ("solhunter_zero.onchain_metrics", "TOP_VOLUME_TOKENS_CACHE_TTL", "TOP_VOLUME_TOKENS_CACHE", 60.0),
    ("solhunter_zero.prices", "PRICE_CACHE_TTL", "PRICE_CACHE", 30.0),
    ("solhunter_zero.depth_client", "DEPTH_CACHE_TTL", None, 0.5),
]

@pytest.mark.parametrize("module_name, attr, cache_attr, default", CASES)
def test_cache_ttl_env(monkeypatch, module_name, attr, cache_attr, default):
    mod = importlib.import_module(module_name)
    mod = importlib.reload(mod)
    assert getattr(mod, attr) == default
    if cache_attr:
        assert getattr(mod, cache_attr).ttl == default

    monkeypatch.setenv(attr, "5")
    mod = importlib.reload(mod)
    assert getattr(mod, attr) == 5.0
    if cache_attr:
        assert getattr(mod, cache_attr).ttl == 5.0
