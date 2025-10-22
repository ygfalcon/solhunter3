import asyncio
import sys
import types

import pytest

if "jsonschema" not in sys.modules:
    jsonschema_stub = types.ModuleType("jsonschema")

    class _DummyValidator:
        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
            pass

        def validate(self, *args, **kwargs) -> None:  # pragma: no cover - simple stub
            return None

    jsonschema_stub.Draft202012Validator = _DummyValidator
    exceptions_module = types.ModuleType("exceptions")

    class _DummyValidationError(Exception):  # pragma: no cover - simple stub
        pass

    exceptions_module.ValidationError = _DummyValidationError
    jsonschema_stub.exceptions = exceptions_module
    sys.modules["jsonschema"] = jsonschema_stub
    sys.modules["jsonschema.exceptions"] = exceptions_module

from solhunter_zero.golden_pipeline.momentum import MomentumAgent
from solhunter_zero.golden_pipeline.types import GoldenSnapshot


async def _noop_publish(_mint: str, _computation) -> None:
    return None


def test_momentum_normalization() -> None:
    agent = MomentumAgent(
        pipeline=object(),
        publish=_noop_publish,
        config={"golden": {"momentum": {"enabled": True}}},
    )
    snapshot = GoldenSnapshot(
        mint="MintA",
        asof=123.0,
        meta={},
        px={},
        liq={},
        ohlcv5m={},
        hash="hash",
    )
    sources = {
        "birdeye": {
            "MintA": {"volume_1h_usd": 1200.0, "volume_24h_usd": 5400.0},
            "MintB": {"volume_1h_usd": 600.0, "volume_24h_usd": 1200.0},
        },
        "birdeye_trending": {"MintA": {"score": 0.82, "rank": 1}},
        "dexscreener": {
            "MintA": {
                "priceChange": {"m5": 12.5, "h1": 40.0},
                "rank": 1,
            }
        },
        "dexscreener_tokens": {},
        "pumpfun": {
            "MintA": {
                "rank": 2,
                "buyersLastHour": 48,
                "score": 0.9,
                "tweetsLastHour": 180,
                "sentiment": 0.7,
            }
        },
        "social": {},
    }

    async def _run() -> None:
        result = await agent._compute_momentum("MintA", snapshot, sources, budget=1.0)
        breakdown = result.momentum_breakdown
        for key in (
            "volume_rank_1h",
            "volume_rank_24h",
            "price_momentum_5m",
            "price_momentum_1h",
            "pump_intensity",
            "tweets_per_min",
            "social_sentiment",
            "social_score",
        ):
            value = breakdown.get(key)
            assert value is not None
            assert 0.0 <= value <= 1.0

        assert breakdown.get("volume_1h_usd_raw") == 1200.0
        assert breakdown.get("volume_24h_usd_raw") == 5400.0
        assert breakdown.get("price_change_5m_raw") == pytest.approx(12.5)
        assert breakdown.get("price_change_1h_raw") == pytest.approx(40.0)
        assert breakdown.get("pump_rank_raw") == 2
        assert breakdown.get("buyers_last_hour_raw") == 48
        assert breakdown.get("pump_score_raw") == pytest.approx(0.9)
        assert breakdown.get("pump_score_norm") == pytest.approx(0.9)
        assert breakdown.get("tweets_per_min_raw") == pytest.approx(3.0)

        weights = agent._config.weights
        weight_sum = (
            weights.volume_rank_1h
            + weights.volume_rank_24h
            + weights.price_momentum_5m
            + weights.price_momentum_1h
            + weights.pump_intensity
            + weights.social_sentiment
        )
        expected = (
            breakdown["volume_rank_1h"] * weights.volume_rank_1h
            + breakdown["volume_rank_24h"] * weights.volume_rank_24h
            + breakdown["price_momentum_5m"] * weights.price_momentum_5m
            + breakdown["price_momentum_1h"] * weights.price_momentum_1h
            + breakdown["pump_intensity"] * weights.pump_intensity
            + breakdown["social_sentiment"] * weights.social_sentiment
        ) / weight_sum

        assert result.momentum_score is not None
        assert round(result.momentum_score, 4) == round(expected, 4)
        assert result.momentum_partial is False
        assert result.momentum_stale is False
        assert set(result.momentum_sources) >= {"birdeye", "dexscreener", "pumpfun"}
        assert result.pump_score == pytest.approx(0.9)
        assert result.tweets_per_min == pytest.approx(1.0)

        sources_missing = {
            "birdeye": {},
            "birdeye_trending": {},
            "dexscreener": {},
            "dexscreener_tokens": {},
            "pumpfun": {},
            "social": {},
        }
        missing = await agent._compute_momentum("MintB", snapshot, sources_missing, budget=1.0)
        assert missing.momentum_partial is True
        assert missing.momentum_stale is True

    asyncio.run(_run())
