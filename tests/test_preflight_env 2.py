import logging
import os
from pathlib import Path

import pytest

from solhunter_zero.env_config import configure_environment
from solhunter_zero.preflight_utils import check_required_env


@pytest.fixture
def restore_env():
    """Snapshot os.environ and restore after the test."""
    original = os.environ.copy()
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


def test_check_required_env_success(restore_env):
    os.environ["SOLANA_RPC_URL"] = "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
    os.environ["BIRDEYE_API_KEY"] = "real_key"
    ok, msg = check_required_env()
    assert ok is True
    assert msg == "Required environment variables set"


def test_check_required_env_missing(restore_env, caplog):
    os.environ.pop("SOLANA_RPC_URL", None)
    os.environ.pop("BIRDEYE_API_KEY", None)
    with caplog.at_level(
        logging.WARNING, logger="solhunter_zero.preflight_utils"
    ):
        ok, msg = check_required_env()
    assert ok is False
    assert "SOLANA_RPC_URL" in msg and "BIRDEYE_API_KEY" not in msg


def test_check_required_env_placeholder(restore_env, caplog):
    os.environ["SOLANA_RPC_URL"] = "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
    os.environ["BIRDEYE_API_KEY"] = "invalid_birdeye_key"
    with caplog.at_level(
        logging.WARNING, logger="solhunter_zero.preflight_utils"
    ):
        ok, msg = check_required_env()
    assert ok is True
    assert msg == "Required environment variables set"
    assert "BIRDEYE_API_KEY" in caplog.text


def test_check_required_env_bd_placeholder(restore_env, caplog):
    os.environ["SOLANA_RPC_URL"] = "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
    os.environ["BIRDEYE_API_KEY"] = "BD1234567890ABCDEFGHIJKL"
    with caplog.at_level(
        logging.WARNING, logger="solhunter_zero.preflight_utils"
    ):
        ok, msg = check_required_env()
    assert ok is True
    assert msg == "Required environment variables set"
    assert "BIRDEYE_API_KEY" in caplog.text


def test_configure_env_strips_placeholder(tmp_path: Path, restore_env, caplog):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=demo-helius-key\n"
        "BIRDEYE_API_KEY=be_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE\n"
    )
    configure_environment(tmp_path)
    with caplog.at_level(
        logging.WARNING, logger="solhunter_zero.preflight_utils"
    ):
        ok, msg = check_required_env()
    assert ok is True
    assert os.environ.get("BIRDEYE_API_KEY") == ""
    assert "be_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE" not in env_file.read_text()
    assert "BIRDEYE_API_KEY" in caplog.text


def test_configure_env_strips_bd_placeholder(
    tmp_path: Path, restore_env, caplog
):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=demo-helius-key\n"
        "BIRDEYE_API_KEY=BD1234567890ABCDEFGHIJKL\n"
    )
    configure_environment(tmp_path)
    with caplog.at_level(
        logging.WARNING, logger="solhunter_zero.preflight_utils"
    ):
        ok, msg = check_required_env()
    assert ok is True
    assert os.environ.get("BIRDEYE_API_KEY") == ""
    assert "BD1234567890ABCDEFGHIJKL" not in env_file.read_text()
    assert "BIRDEYE_API_KEY" in caplog.text


def test_configure_env_uses_config_value(tmp_path: Path, restore_env, caplog):
    env_file = tmp_path / ".env"
    env_file.write_text(
        "SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=demo-helius-key\n"
        "BIRDEYE_API_KEY=be_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE\n"
    )
    (tmp_path / "config.toml").write_text('birdeye_api_key="real_key"\n')
    configure_environment(tmp_path)
    with caplog.at_level(
        logging.WARNING, logger="solhunter_zero.preflight_utils"
    ):
        ok, msg = check_required_env()
    assert ok is True
    assert os.environ.get("BIRDEYE_API_KEY") == "real_key"
    assert "real_key" in env_file.read_text()
    assert "BIRDEYE_API_KEY" not in caplog.text


def test_configure_env_creates_sanitized_env(tmp_path: Path, restore_env):
    example = tmp_path / ".env.example"
    example.write_text(
        "SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=demo-helius-key\n"
        "BIRDEYE_API_KEY=be_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE\n"
    )
    configure_environment(tmp_path)
    env_file = tmp_path / ".env"
    assert env_file.exists()
    assert "be_FAKEFAKEFAKEFAKEFAKEFAKEFAKEFAKE" not in env_file.read_text()
    assert os.environ.get("BIRDEYE_API_KEY") == ""


def test_configure_env_serializes_json(tmp_path: Path, restore_env):
    env_file = tmp_path / ".env"
    env_file.write_text("")
    (tmp_path / "config.toml").write_text(
        "agents=['simulation']\nagent_weights={simulation=1.0}\n"
    )
    configure_environment(tmp_path)
    from solhunter_zero.jsonutil import loads

    assert loads(os.environ["AGENTS"]) == ["simulation"]
    assert loads(os.environ["AGENT_WEIGHTS"]) == {"simulation": 1.0}
    content = env_file.read_text().splitlines()
    env_map = dict(line.split("=", 1) for line in content if "=" in line)
    assert loads(env_map["AGENTS"]) == ["simulation"]
    assert loads(env_map["AGENT_WEIGHTS"]) == {"simulation": 1.0}
