import asyncio

from solhunter_zero.golden_pipeline.market import MarketDataStage
from solhunter_zero.golden_pipeline.types import TapeEvent

from tests.golden_pipeline.conftest import (
    SCENARIO_PAYLOADS,
    STREAMS,
    approx,
    assert_tolerant_dict,
)


def test_market_enrichment_combines_depth(fake_broker, golden_harness):
    depth_events = fake_broker.events[STREAMS.market_depth]
    assert depth_events, "no depth events recorded"

    expected_mid = (
        SCENARIO_PAYLOADS["depth_snapshots"][0].mid * SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["1"]
        + SCENARIO_PAYLOADS["depth_snapshots"][1].mid * SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["1"]
    ) / (
        SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["1"]
        + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["1"]
    )
    combined_depth = next(event for event in depth_events if event["mid_usd"] == approx(expected_mid))

    assert combined_depth["spread_bps"] == approx(min(f.spread_bps for f in SCENARIO_PAYLOADS["depth_snapshots"]))
    assert_tolerant_dict(
        combined_depth["depth_pct"],
        {
            "1": SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["1"]
            + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["1"],
            "2": SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["2"]
            + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["2"],
            "5": SCENARIO_PAYLOADS["depth_snapshots"][0].depth_pct["5"]
            + SCENARIO_PAYLOADS["depth_snapshots"][1].depth_pct["5"],
        },
    )

    bars = fake_broker.events[STREAMS.market_ohlcv]
    assert len(bars) == 1
    bar = bars[0]
    assert bar["trades"] == 2
    assert bar["buyers"] == 1
    assert bar["vol_usd"] == approx(abs(SCENARIO_PAYLOADS["tape_events"][0].amount_quote) + abs(SCENARIO_PAYLOADS["tape_events"][1].amount_quote))

    snapshot = next(
        snap for snap in golden_harness.golden_snapshots if snap.px["mid_usd"] == approx(expected_mid)
    )
    assert snapshot.metrics["latency_ms"] > 0.0
    assert snapshot.metrics["depth_staleness_ms"] > 0.0
    assert snapshot.metrics["latency_ms"] == approx(snapshot.metrics["depth_staleness_ms"])


def test_market_stage_release_lock_before_emit_on_submit():
    async def runner() -> None:
        emitted: list = []
        emit_started = asyncio.Event()
        emit_continue = asyncio.Event()
        first_bar_pending = True

        async def emit(bar):
            nonlocal first_bar_pending
            if first_bar_pending:
                first_bar_pending = False
                emit_started.set()
                await emit_continue.wait()
            emitted.append(bar)

        stage = MarketDataStage(emit)
        base_ts = 1_000.0
        event_template = dict(
            mint_base="LOCK",
            mint_quote="USD",
            route="jup",
            program_id="amm",
            pool="pool",
            signer="trader",
            signature="sig",
            slot=0,
            fees_base=0.0,
            fees_usd=0.05,
            is_self=False,
        )
        await stage.submit(
            TapeEvent(
                amount_base=1.0,
                amount_quote=10.0,
                ts=base_ts,
                price_usd=10.0,
                buyer="buyer",
                **event_template,
            )
        )
        emit_task = asyncio.create_task(
            stage.submit(
                TapeEvent(
                    amount_base=2.0,
                    amount_quote=20.0,
                    ts=base_ts + 300.0,
                    price_usd=10.0,
                    buyer=None,
                    **event_template,
                )
            )
        )
        await asyncio.wait_for(emit_started.wait(), timeout=1.0)

        await asyncio.wait_for(
            stage.submit(
                TapeEvent(
                    amount_base=3.0,
                    amount_quote=30.0,
                    ts=base_ts + 310.0,
                    price_usd=10.5,
                    buyer="buyer",
                    **event_template,
                )
            ),
            timeout=0.5,
        )

        emit_continue.set()
        await emit_task
        await stage.flush(now=base_ts + 700.0)

        assert len(emitted) == 2
        first, second = emitted
        assert first.trades == 1
        assert second.trades == 2

    asyncio.run(runner())


def test_market_stage_release_lock_before_emit_on_flush():
    async def runner() -> None:
        emitted: list = []
        emit_started = asyncio.Event()
        emit_continue = asyncio.Event()
        first_bar_pending = True

        async def emit(bar):
            nonlocal first_bar_pending
            if first_bar_pending:
                first_bar_pending = False
                emit_started.set()
                await emit_continue.wait()
            emitted.append(bar)

        stage = MarketDataStage(emit)
        base_ts = 2_000.0
        event_template = dict(
            mint_base="FLUSH",
            mint_quote="USD",
            route="jup",
            program_id="amm",
            pool="pool",
            signer="trader",
            signature="sig",
            slot=0,
            fees_base=0.0,
            fees_usd=0.05,
            is_self=False,
        )
        await stage.submit(
            TapeEvent(
                amount_base=1.0,
                amount_quote=10.0,
                ts=base_ts,
                price_usd=10.0,
                buyer="buyer",
                **event_template,
            )
        )

        flush_task = asyncio.create_task(stage.flush(now=base_ts + 400.0))
        await asyncio.wait_for(emit_started.wait(), timeout=1.0)

        await asyncio.wait_for(
            stage.submit(
                TapeEvent(
                    amount_base=2.0,
                    amount_quote=20.0,
                    ts=base_ts + 420.0,
                    price_usd=10.2,
                    buyer="buyer2",
                    **event_template,
                )
            ),
            timeout=0.5,
        )

        emit_continue.set()
        await flush_task
        await stage.flush(now=base_ts + 800.0)

        assert len(emitted) == 2
        first, second = emitted
        assert first.trades == 1
        assert second.trades == 1

    asyncio.run(runner())
