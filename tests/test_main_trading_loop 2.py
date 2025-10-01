"""Integration-like test for the asynchronous trading loop.

This test launches the trading loop with all network and exchange layers
mocked using utilities from :mod:`tests.test_live_trading`. It waits up to
``timeout`` seconds for a trade to be recorded and verifies that either a trade
has been logged or the ``_first_trade_recorded`` flag has been set.

Expected runtime: <2s. The test relies on stubbed dependencies and runs in
dry-run mode to avoid external network calls.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from tests.test_live_trading import setup_live_trading_env


def test_main_loop_records_trade(monkeypatch):
    timeout = 2
    with monkeypatch.context() as mp:
        main_module, trades = setup_live_trading_env(mp)

        import solhunter_zero.loop as loop_mod

        async def fake_run_iteration(*args, **kwargs):
            state = args[2]
            state.last_tokens = []
            main_module._first_trade_recorded = True
            trades.append({"token": "SOL"})

        mp.setattr(loop_mod, "run_iteration", fake_run_iteration)

        cfg = {}
        runtime_cfg = main_module.Config.from_env(cfg)
        memory = main_module.Memory("sqlite:///:memory:")
        portfolio = main_module.Portfolio(path=None)
        state = main_module.TradingState()
        strategy_manager = main_module.StrategyManager()
        proc_ref = [None]
        keypair = SimpleNamespace()

        async def run() -> None:
            await main_module.trading_loop(
                cfg,
                runtime_cfg,
                memory,
                portfolio,
                state,
                loop_delay=0,
                min_delay=0,
                max_delay=0,
                cpu_low_threshold=0,
                cpu_high_threshold=0,
                depth_freq_low=0,
                depth_freq_high=0,
                depth_rate_limit=0,
                iterations=1,
                testnet=False,
                dry_run=True,
                offline=True,
                token_file=None,
                discovery_method="websocket",
                keypair=keypair,
                stop_loss=None,
                take_profit=None,
                trailing_stop=None,
                max_drawdown=1.0,
                volatility_factor=1.0,
                arbitrage_threshold=0.0,
                arbitrage_amount=0.0,
                strategy_manager=strategy_manager,
                agent_manager=None,
                market_ws_url=None,
                order_book_ws_url=None,
                arbitrage_tokens=None,
                rl_daemon=False,
                rl_interval=1.0,
                proc_ref=proc_ref,
            )

        asyncio.run(asyncio.wait_for(run(), timeout=timeout))

        assert trades or main_module._first_trade_recorded
