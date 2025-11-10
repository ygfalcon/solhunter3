import asyncio
import logging
import time
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from solhunter_zero.exit_management import ExitManager, PostEntryMonitor
from solhunter_zero.golden_pipeline.types import GoldenSnapshot, TradeSuggestion
from solhunter_zero.golden_pipeline.voting import CONFLICT_DELTA, VotingStage
from solhunter_zero.portfolio import Portfolio
from solhunter_zero.exec_service.service import TradeExecutor


class DummyMemory:
    async def log_trade(self, **_: Any) -> None:  # pragma: no cover - simple stub
        return None


def _snapshot(
    price: float,
    *,
    spread_bps: float = 40.0,
    depth_usd: float = 8_000.0,
    ts: float = 0.0,
    hash_: str = "hash",
    mint: str = "ABC",
) -> GoldenSnapshot:
    return GoldenSnapshot(
        mint=mint,
        asof=ts,
        meta={},
        px={"mid_usd": price, "spread_bps": spread_bps},
        liq={"depth_pct": {"1": depth_usd}},
        ohlcv5m={},
        hash=hash_,
    )


def test_post_entry_monitor_schedule_and_signals() -> None:
    monitor = PostEntryMonitor(entry_ts=0.0)
    first_ticks = [monitor.next_due() for _ in range(5)]
    assert first_ticks == [1.0, 2.0, 3.0, 4.0, 5.0]
    monitor.advance_to(32.0)
    later_ticks = [monitor.next_due() for _ in range(3)]
    assert later_ticks == [35.0, 40.0, 45.0]

    snap = _snapshot(101.25, spread_bps=55.0, depth_usd=9_000.0, ts=5.0)
    signals = monitor.observe(snapshot=snap, entry_price=100.0)
    assert signals["delta_mid_bps"] == pytest.approx(125.0)
    assert signals["spread_bps"] == 55.0
    assert signals["depth_1pct_usd"] == 9_000.0


def test_portfolio_tracks_breakeven_bps(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("solhunter_zero.portfolio.publish", lambda *a, **k: None)
    portfolio = Portfolio(path=None)
    meta = {"fees_bps": 8.0, "latency_bps": 5.0, "impact_bps": 12.0}
    portfolio.update("ABC", 10.0, 100.0, ts=0.0, meta={"exit_costs": meta})
    pos = portfolio.get_position("ABC")
    assert pos is not None
    assert pos.breakeven_bps == pytest.approx(25.0)
    portfolio.apply_exit_slice("ABC", -4.0, 110.0, ts=10.0, reason="test")
    pos = portfolio.get_position("ABC")
    assert pos is not None
    assert pos.breakeven_bps > 0


def test_fast_adverse_move_triggers_must_exit() -> None:
    manager = ExitManager()
    manager.register_entry(
        "ABC",
        entry_price=100.0,
        size=5.0,
        breakeven_bps=40.0,
        ts=0.0,
        hot_watch=True,
    )
    snap = _snapshot(94.0, spread_bps=80.0, depth_usd=10_000.0, ts=4.0)
    suggestion = manager.evaluate_snapshot(
        snap,
        portfolio_drawdown_pct=0.04,
        venue_anomaly=False,
        global_brake=True,
    )
    assert suggestion is not None
    assert suggestion.must_exit is True
    assert suggestion.side == "sell"
    assert suggestion.exit_diagnostics["rail"] == "fast_adverse"
    assert suggestion.gating["requested_notional_usd"] == pytest.approx(470.0)
    summary = manager.summary()
    assert summary["hot_watch"][0]["token"] == "ABC"
    assert summary["queue"][0]["must"] is True

    manager.record_slice("ABC", -2.0, 93.5, ts=5.0, reason="partial", pnl=-13.0)
    progress_summary = manager.summary()
    queue_entry = progress_summary["queue"][0]
    assert queue_entry["filled_qty"] == pytest.approx(2.0)
    assert queue_entry["remaining_qty"] == pytest.approx(3.0)
    assert queue_entry["slices_executed"] == 1
    assert "partial" in queue_entry["slice_reasons"]
    assert queue_entry["progress"] == pytest.approx(0.4)
    hot_watch_entry = progress_summary["hot_watch"][0]
    assert hot_watch_entry["progress"] == pytest.approx(0.4)


def test_trailing_and_time_stop_behaviour() -> None:
    manager = ExitManager()
    manager.register_entry(
        "ABC",
        entry_price=100.0,
        size=3.0,
        breakeven_bps=30.0,
        ts=0.0,
    )
    snap_high = _snapshot(108.0, ts=15.0)
    manager.evaluate_snapshot(snap_high)
    snap_pullback = _snapshot(105.0, ts=40.0)
    suggestion = manager.evaluate_snapshot(snap_pullback)
    assert suggestion is not None
    assert suggestion.exit_diagnostics["rail"] == "smart_trailing"

    manager.register_entry(
        "XYZ",
        entry_price=200.0,
        size=2.0,
        breakeven_bps=25.0,
        ts=0.0,
    )
    manager.evaluate_snapshot(_snapshot(207.0, hash_="xyz", ts=10.0, mint="XYZ"))
    time_stop = manager.evaluate_snapshot(
        _snapshot(207.0, hash_="xyz", ts=140.0, mint="XYZ")
    )
    assert time_stop is not None
    assert time_stop.exit_diagnostics["rail"] == "time_stop"
    assert time_stop.must_exit is True


def test_strategy_exit_routes_to_normal_suggestion() -> None:
    manager = ExitManager()
    manager.register_entry(
        "ABC",
        entry_price=100.0,
        size=2.0,
        breakeven_bps=20.0,
        ts=0.0,
    )
    suggestion = manager.evaluate_snapshot(
        _snapshot(103.0, ts=20.0),
        strategy_reasons=["momentum_flip", "opportunity_cost"],
    )
    assert suggestion is not None
    assert suggestion.must_exit is False
    assert suggestion.exit_diagnostics["rail"] == "strategy"
    assert set(suggestion.exit_diagnostics["strategy_reasons"]) == {
        "momentum_flip",
        "opportunity_cost",
    }


def test_voting_conflict_bias_prefers_sell() -> None:
    emitted: List[TradeSuggestion] = []

    async def emit(decision: TradeSuggestion) -> None:
        emitted.append(decision)

    async def _run() -> None:
        stage = VotingStage(emit, window_ms=350, quorum=1)
        assert stage.conflict_delta == pytest.approx(CONFLICT_DELTA)
        now = time.time()
        base_kwargs: Dict[str, Any] = dict(
            notional_usd=1_000.0,
            max_slippage_bps=40.0,
            risk={},
            confidence=0.2,
            inputs_hash="hash",
            ttl_sec=1.0,
            generated_at=now,
        )
        sell = TradeSuggestion(
            agent="exit",
            mint="ABC",
            side="sell",
            must_exit=True,
            **base_kwargs,
        )
        buy = TradeSuggestion(
            agent="entry",
            mint="ABC",
            side="buy",
            must_exit=False,
            **base_kwargs,
        )
        await stage.submit(sell)
        await stage.submit(buy)
        await asyncio.sleep(stage.window_sec * 1.5)

    asyncio.run(_run())
    assert emitted, "sell decision should be emitted"
    assert emitted[0].side == "sell"


def test_trade_executor_skips_when_valuation_fails(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    portfolio = Portfolio(path=None)
    portfolio._apply_update("ABC", 5.0, 10.0, ts=0.0)

    memory = SimpleNamespace(log_trade=AsyncMock())
    executor = TradeExecutor(memory=memory, portfolio=portfolio)

    monkeypatch.setattr(
        portfolio,
        "total_value",
        MagicMock(side_effect=RuntimeError("valuation failed")),
    )

    portfolio.update_async = AsyncMock()
    place_order_mock = AsyncMock()
    monkeypatch.setattr(
        "solhunter_zero.exec_service.service.place_order_async",
        place_order_mock,
    )
    published: list[tuple[tuple[Any, ...], Dict[str, Any]]] = []
    monkeypatch.setattr(
        "solhunter_zero.exec_service.service.event_bus.publish",
        lambda *a, **k: published.append((a, k)),
    )

    payload = {
        "token": "ABC",
        "side": "sell",
        "expected_price": 12.0,
        "size": 1.0,
    }

    async def _run() -> None:
        caplog.set_level(logging.ERROR, logger="solhunter_zero.exec_service.service")
        executor._on_decision(payload)
        await asyncio.sleep(0)

    asyncio.run(_run())

    assert executor._n == 0
    assert not published
    assert memory.log_trade.await_count == 0
    assert portfolio.update_async.await_count == 0
    assert place_order_mock.await_count == 0
    assert any(
        "portfolio valuation failed" in record.getMessage()
        and record.exc_info is not None
        for record in caplog.records
    )
    assert not executor._tasks


def test_executor_builds_clamped_exit_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("solhunter_zero.exec_service.service.place_order_async", lambda *a, **k: None)
    monkeypatch.setattr("solhunter_zero.exec_service.service.event_bus.publish", lambda *a, **k: None)
    portfolio = Portfolio(path=None)
    portfolio.update("ABC", 10.0, 100.0, ts=0.0)
    executor = TradeExecutor(memory=DummyMemory(), portfolio=portfolio)
    payload = {
        "token": "ABC",
        "side": "sell",
        "notional_usd": 2_000.0,
        "expected_price": 105.0,
        "max_slippage_bps": 60.0,
        "must_exit": True,
        "depth_1pct_usd": 6_000.0,
    }
    plan = executor._build_execution_plan(payload)
    assert plan["total_qty"] == pytest.approx(min(10.0, 2_000.0 / 105.0))
    assert plan["slices"]
    first_slice = plan["slices"][0]
    assert first_slice["fraction"] == pytest.approx(0.6)
    assert 1.0 <= first_slice["jitter_sec"] <= 3.0
    for slice_ in plan["slices"]:
        assert slice_["qty"] <= payload["depth_1pct_usd"] * 0.35 / payload["expected_price"] + 1e-9


def test_exit_summary_tracks_queue_and_missed() -> None:
    manager = ExitManager()
    manager.register_entry(
        "ABC",
        entry_price=100.0,
        size=1.0,
        breakeven_bps=20.0,
        ts=0.0,
    )
    snap = _snapshot(101.0, spread_bps=150.0, depth_usd=8_000.0, ts=60.0)
    suggestion = manager.evaluate_snapshot(
        snap,
        strategy_reasons=["rebalance"],
    )
    assert suggestion is None
    manager.record_missed_exit("ABC", reason="spread_gate", diagnostics={"spread": 150.0})
    summary = manager.summary()
    assert summary["missed_exits"]
    assert summary["missed_exits"][0]["reason"] == "spread_gate"
    assert summary["queue"] == []
