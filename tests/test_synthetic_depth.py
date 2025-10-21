import asyncio
import math
import time

import pytest

from solhunter_zero.synthetic_depth import (
    DexStats,
    Level,
    OrderbookData,
    PythPrice,
    SynthInputs,
    SyntheticState,
    _blend_depth,
    compute_depth_change,
    _synth_state,
    _synth_details,
)


def test_compute_depth_change_tracks_state(monkeypatch):
    """compute_depth_change should persist state between invocations."""

    # Ensure clean slate for the targeted mint
    _synth_state.clear()
    _synth_details.clear()

    now = time.time()
    orderbook = OrderbookData(
        bids=[Level(price=1.01, size_base=10.0, notional_usd=10.1)],
        asks=[Level(price=1.03, size_base=8.0, notional_usd=8.24)],
        total_bid_usd=10.1,
        total_ask_usd=8.24,
        best_bid=1.01,
        best_ask=1.03,
    )
    dex = DexStats(liquidity_usd=250_000.0, volume_24h=120_000.0, best_price=1.02)
    pyth = PythPrice(price=1.02, confidence=0.0005, publish_time=now)

    async def fake_inputs(mint: str, rpc_url: str | None) -> SynthInputs:
        return SynthInputs(
            pyth=pyth,
            orderbook=orderbook,
            dex=dex,
            mid_price_hint=1.02,
        )

    monkeypatch.setattr("solhunter_zero.synthetic_depth._gather_inputs", fake_inputs)

    mint = "So11111111111111111111111111111111111111112"
    async def runner() -> tuple[float, float]:
        first_delta = await compute_depth_change(mint)
        second_delta = await compute_depth_change(mint)
        return first_delta, second_delta

    first, second = asyncio.run(runner())
    assert first > 0
    assert isinstance(second, float)
    assert not math.isnan(second)
    assert mint in _synth_state


def test_blend_depth_prefers_fresh_data():
    """_blend_depth should blend orderbook liquidity with fallback depth."""

    now = time.time()
    orderbook = OrderbookData(
        bids=[Level(price=2.0, size_base=1.0, notional_usd=2.0)],
        asks=[Level(price=2.02, size_base=1.5, notional_usd=3.03)],
        total_bid_usd=2.0,
        total_ask_usd=3.03,
        best_bid=2.0,
        best_ask=2.02,
    )
    dex = DexStats(liquidity_usd=1_000_000.0, volume_24h=400_000.0, best_price=2.01)
    inputs = SynthInputs(
        pyth=None,
        orderbook=orderbook,
        dex=dex,
        mid_price_hint=2.01,
    )
    new_state, delta = _blend_depth("mint", None, inputs, now)
    assert new_state is not None
    assert delta == pytest.approx(new_state.depth_usd)
    # Ensure the blended depth exceeds the raw orderbook depth
    assert new_state.depth_usd > orderbook.total_bid_usd + orderbook.total_ask_usd
    assert new_state.spread_bps >= 0


def test_blend_depth_applies_staleness_decay():
    """Stale oracle updates should decay the carried depth."""

    now = time.time()
    state = SyntheticState(
        depth_usd=10_000.0,
        last_update=now - 60.0,
        mid_usd=1.5,
        spread_bps=20.0,
        liquidity_class="large",
        confidence_bps=5.0,
        publish_time=now - 60.0,
        depth_pct={"1": 1000.0},
        alpha=0.5,
        tau=120.0,
        extras={},
    )
    stale_pyth = PythPrice(price=1.5, confidence=0.0002, publish_time=now - 400.0)
    inputs = SynthInputs(
        pyth=stale_pyth,
        orderbook=None,
        dex=None,
        mid_price_hint=1.5,
    )
    new_state, delta = _blend_depth("mint", state, inputs, now)
    assert new_state is not None
    # Depth should decay below the previous value due to staleness
    assert new_state.depth_usd < state.depth_usd
    assert delta < 0
