import asyncio
import time

import pytest

from solhunter_zero.exit_management import (
    ExitManager,
    PostEntryMonitor,
    StrategySignals,
)
from solhunter_zero.golden_pipeline.types import GoldenSnapshot
from solhunter_zero.golden_pipeline.types import TradeSuggestion
from solhunter_zero.golden_pipeline.voting import VotingStage


def test_post_entry_monitor_micro_chart_latency():
    samples = []
    sleeps = []

    async def fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    snapshot_values = [
        GoldenSnapshot(
            mint="SOL",
            asof=time.time(),
            meta={},
            px={"mid_usd": 10.0, "spread_bps": 30.0},
            liq={"depth_pct": {"1": 5000.0, "5": 12000.0}},
            ohlcv5m={},
            hash="h1",
        ),
        GoldenSnapshot(
            mint="SOL",
            asof=time.time(),
            meta={},
            px={"mid_usd": 10.2, "spread_bps": 32.0},
            liq={"depth_pct": {"1": 4800.0, "5": 11000.0}},
            ohlcv5m={},
            hash="h2",
        ),
        GoldenSnapshot(
            mint="SOL",
            asof=time.time(),
            meta={},
            px={"mid_usd": 10.25, "spread_bps": 28.0},
            liq={"depth_pct": {"1": 5100.0, "5": 11500.0}},
            ohlcv5m={},
            hash="h3",
        ),
    ]

    async def fetch(_: str):
        if snapshot_values:
            return snapshot_values.pop(0)
        return None

    monitor_ref: dict[str, PostEntryMonitor] = {}

    async def runner() -> None:
        monitor = PostEntryMonitor(
            fetch,
            sleep=fake_sleep,
            schedule=(0.01, 0.01, 0.01),
            hot_duration=0.5,
        )
        monitor_ref["monitor"] = monitor

        await monitor.watch_fill(
            "SOL",
            entry_price=10.0,
            on_update=lambda signal: samples.append(signal),
            max_samples=3,
        )

    asyncio.run(runner())
    monitor = monitor_ref["monitor"]

    assert len(samples) == 3
    assert all(signal.price >= 10.0 for signal in samples)
    assert monitor.hot_tokens() == []
    chart = monitor.micro_chart("SOL")
    assert len(chart) == 3
    assert all(ts <= time.time() for ts, _ in chart)
    assert any(delay > 0 for delay in sleeps)


def test_exit_manager_rug_must_exit():
    manager = ExitManager()
    signals = StrategySignals()
    suggestion = manager.evaluate(
        token="SOL",
        qty=10.0,
        entry_price=10.0,
        breakeven_bps=50.0,
        price=9.4,
        spread_bps=40.0,
        depth1pct_usd=6000.0,
        signals=signals,
        now=time.time(),
    )
    assert suggestion is not None
    assert suggestion.must
    assert suggestion.reason == "fast_adverse_move"


def test_exit_manager_spread_blowout_must_exit():
    manager = ExitManager()
    signals = StrategySignals()
    suggestion = manager.evaluate(
        token="SOL",
        qty=5.0,
        entry_price=10.0,
        breakeven_bps=40.0,
        price=9.95,
        spread_bps=200.0,
        depth1pct_usd=1000.0,
        signals=signals,
        now=time.time(),
    )
    assert suggestion is not None
    assert suggestion.must
    assert suggestion.reason == "spread_blowout"


def test_exit_manager_trailing_stop_profit():
    manager = ExitManager()
    signals = StrategySignals()
    now = time.time()
    # initial observation (no exit)
    assert (
        manager.evaluate(
            token="SOL",
            qty=10.0,
            entry_price=10.0,
            breakeven_bps=100.0,
            price=10.0,
            spread_bps=40.0,
            depth1pct_usd=6000.0,
            signals=signals,
            now=now,
        )
        is None
    )
    # rally to arm trailing
    assert (
        manager.evaluate(
            token="SOL",
            qty=10.0,
            entry_price=10.0,
            breakeven_bps=100.0,
            price=10.35,
            spread_bps=40.0,
            depth1pct_usd=6000.0,
            signals=signals,
            now=now + 10,
        )
        is None
    )
    # drop to trigger trailing stop
    suggestion = manager.evaluate(
        token="SOL",
        qty=10.0,
        entry_price=10.0,
        breakeven_bps=100.0,
        price=10.2,
        spread_bps=40.0,
        depth1pct_usd=6000.0,
        signals=signals,
        now=now + 20,
    )
    assert suggestion is not None
    assert not suggestion.must
    assert suggestion.reason == "trailing_stop"


def test_exit_manager_gate_miss_logged():
    manager = ExitManager()
    signals = StrategySignals(momentum_flip=True)
    suggestion = manager.evaluate(
        token="SOL",
        qty=4.0,
        entry_price=10.0,
        breakeven_bps=50.0,
        price=10.4,
        spread_bps=150.0,
        depth1pct_usd=6000.0,
        signals=signals,
        now=time.time(),
    )
    assert suggestion is None
    missed = manager.missed()
    assert len(missed) == 1
    assert missed[0]["reason"] == "momentum_flip"


def test_exit_manager_time_stop_triggers():
    manager = ExitManager()
    signals = StrategySignals()
    now = time.time()
    manager.evaluate(
        token="SOL",
        qty=5.0,
        entry_price=10.0,
        breakeven_bps=80.0,
        price=10.3,
        spread_bps=40.0,
        depth1pct_usd=6000.0,
        signals=signals,
        now=now,
    )
    suggestion = manager.evaluate(
        token="SOL",
        qty=5.0,
        entry_price=10.0,
        breakeven_bps=80.0,
        price=10.28,
        spread_bps=40.0,
        depth1pct_usd=6000.0,
        signals=signals,
        now=now + 310,
    )
    assert suggestion is not None
    assert suggestion.reason == "time_stop"


def test_voting_conflict_bias_prefers_sell():
    emitted = []

    async def emit(decision):
        emitted.append(decision)

    async def runner() -> None:
        stage = VotingStage(
            emit,
            quorum=1,
            min_score=0.0,
            min_window_ms=10,
            max_window_ms=10,
            conflict_delta=0.05,
        )

        suggestion_buy = TradeSuggestion(
            agent="trend",
            mint="SOL",
            side="buy",
            notional_usd=1000.0,
            max_slippage_bps=80.0,
            risk={},
            confidence=0.6,
            inputs_hash="hash",
            ttl_sec=1.0,
            generated_at=time.time(),
        )
        suggestion_sell = TradeSuggestion(
            agent="risk",
            mint="SOL",
            side="sell",
            notional_usd=1000.0,
            max_slippage_bps=80.0,
            risk={},
            confidence=0.6,
            inputs_hash="hash",
            ttl_sec=1.0,
            generated_at=time.time(),
        )
        await stage.submit(suggestion_buy)
        await stage.submit(suggestion_sell)
        await asyncio.sleep(0.1)

    asyncio.run(runner())
    assert len(emitted) == 1
    assert emitted[0].side == "sell"
