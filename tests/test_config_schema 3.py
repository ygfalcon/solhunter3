import pytest
from solhunter_zero.config_schema import validate_config


def _cfg():
    return {
        "solana_rpc_url": "https://mainnet.helius-rpc.com/?api-key=demo-helius-key",
        "dex_base_url": "https://quote-api.jup.ag",
        "agents": ["a"],
        "agent_weights": {"a": 1.0},
    }


def test_validate_config_schema_ok():
    cfg = _cfg()
    result = validate_config(cfg)
    assert result["agent_weights"]["a"] == 1.0


def test_validate_config_schema_missing_weight():
    cfg = _cfg()
    cfg["agent_weights"] = {}
    with pytest.raises(ValueError):
        validate_config(cfg)
