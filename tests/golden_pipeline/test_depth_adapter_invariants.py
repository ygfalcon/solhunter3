import math
import time

from solhunter_zero.golden_pipeline.depth_adapter import (
    AnchorResult,
    GoldenDepthAdapter,
    QuoteLeg,
)


def _sample_anchor(*, degraded: bool = False) -> AnchorResult:
    return AnchorResult(
        price=1.05,
        confidence=0.0008,
        publish_time=time.time() - 0.25,
        source="jupiter" if not degraded else "pyth",
        degraded=degraded,
    )


def test_depth_snapshot_monotonicity() -> None:
    anchor = _sample_anchor()
    legs = [
        QuoteLeg(
            notional_usd=1_000.0,
            realized_usd=995.0,
            effective_price=1.045,
            impact_bps=47.6,
            route=("Jupiter", "Raydium"),
            hops=2,
            direction="sell",
        ),
        QuoteLeg(
            notional_usd=5_000.0,
            realized_usd=4_930.0,
            effective_price=1.042,
            impact_bps=76.2,
            route=("Jupiter", "Raydium"),
            hops=2,
            direction="sell",
        ),
        QuoteLeg(
            notional_usd=10_000.0,
            realized_usd=9_820.0,
            effective_price=1.038,
            impact_bps=114.3,
            route=("Jupiter", "Raydium"),
            hops=2,
            direction="sell",
        ),
        QuoteLeg(
            notional_usd=1_000.0,
            realized_usd=1_000.0,
            effective_price=1.058,
            impact_bps=76.2,
            route=("Jupiter", "Orca"),
            hops=1,
            direction="buy",
        ),
    ]

    snapshot = GoldenDepthAdapter.build_snapshot("MintA", anchor, legs, asof=time.time())

    depth_map = snapshot.depth_bands_usd or {}
    assert depth_map.get("0.1", 0.0) <= depth_map.get("0.5", 0.0) <= depth_map.get("1.0", 0.0)
    assert snapshot.staleness_ms is not None and snapshot.staleness_ms >= 0.0
    assert snapshot.source == "jup_route"
    assert snapshot.degraded is False
    assert snapshot.route_meta is not None
    assert snapshot.route_meta.get("sweeps")


def test_depth_snapshot_degraded_on_fallback() -> None:
    anchor = _sample_anchor(degraded=True)
    snapshot = GoldenDepthAdapter.build_snapshot("MintB", anchor, (), asof=time.time())

    assert snapshot.degraded is True
    assert snapshot.source == "pyth_synthetic"
    assert snapshot.px_bid_usd is not None and snapshot.px_ask_usd is not None
    assert snapshot.px_bid_usd < anchor.price < snapshot.px_ask_usd
    spread_bps = ((snapshot.px_ask_usd - snapshot.px_bid_usd) / snapshot.mid_usd) * 10_000
    expected_spread = 2.0 * (anchor.confidence / anchor.price) * 10_000
    assert math.isclose(spread_bps, expected_spread, rel_tol=1e-6)
