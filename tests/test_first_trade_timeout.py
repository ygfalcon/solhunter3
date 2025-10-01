import asyncio
import logging
import os

import pytest

import solhunter_zero.loop as loop


@pytest.fixture
def config_env(tmp_path):
    """Create a temporary config file and isolate environment vars."""
    original = os.environ.copy()
    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text(
        "solana_rpc_url='https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d'\n"
        "dex_base_url='https://quote-api.jup.ag'\n"
        "agents=['dummy']\n"
        "agent_weights={dummy=1.0}\n"
    )
    os.environ["SOLANA_RPC_URL"] = "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"
    os.environ["DEX_BASE_URL"] = "https://quote-api.jup.ag"
    os.environ["AGENTS"] = '["dummy"]'
    os.environ["SOLHUNTER_CONFIG"] = str(cfg_path)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(original)


def test_first_trade_detected(config_env, caplog):
    caplog.set_level(logging.ERROR)
    loop._first_trade_recorded = False
    loop._first_trade_event = asyncio.Event()

    async def trigger():
        await asyncio.sleep(0.01)
        loop._first_trade_event.set()
        loop._first_trade_recorded = True

    async def runner():
        asyncio.create_task(trigger())
        await loop._check_first_trade(0.1, retry=False)

    asyncio.run(runner())
    assert "First trade not recorded" not in caplog.text


def test_first_trade_timeout(config_env, caplog):
    caplog.set_level(logging.ERROR)
    loop._first_trade_recorded = False
    loop._first_trade_event = asyncio.Event()

    async def runner():
        await loop._check_first_trade(0.05, retry=True)

    with pytest.raises(loop.FirstTradeTimeoutError):
        asyncio.run(runner())
    assert "First trade not recorded" in caplog.text
