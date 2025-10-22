import asyncio
import math
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

from solhunter_zero.golden_pipeline.momentum import MomentumAgent, MomentumRateLimitError
from solhunter_zero.golden_pipeline.types import GoldenSnapshot
from solhunter_zero.golden_pipeline.utils import now_ts


def test_momentum_backpressure() -> None:
    published: list[tuple[str, object]] = []

    async def publish_stub(mint: str, computation) -> None:
        published.append((mint, computation))

    agent = MomentumAgent(
        pipeline=object(),
        publish=publish_stub,
        config={"golden": {"momentum": {"enabled": True}}},
    )
    snapshot = GoldenSnapshot(
        mint="MintA",
        asof=now_ts(),
        meta={},
        px={},
        liq={},
        ohlcv5m={},
        hash="hash",
    )
    base_sources = {
        "birdeye": {
            "MintA": {"volume_1h_usd": 1500.0, "volume_24h_usd": 6200.0},
            "MintB": {"volume_1h_usd": 300.0, "volume_24h_usd": 900.0},
        },
        "dexscreener": {
            "MintA": {
                "priceChange": {"m5": 8.0, "h1": 24.0},
                "rank": 3,
            }
        },
        "dexscreener_tokens": {},
        "pumpfun": {
            "MintA": {
                "rank": 4,
                "buyersLastHour": 32,
                "score": 0.6,
                "tweetsLastHour": 90,
            }
        },
    }

    limiter = agent._limiters["pump.fun"]
    limiter.set_tokens(0.0)
    breaker = agent._breakers["pump.fun"]

    error = MomentumRateLimitError("forced 429")
    sources_with_error = dict(base_sources)
    sources_with_error["pumpfun"] = {}
    sources_with_error["_errors"] = {"pumpfun": error}

    for _ in range(3):
        breaker.record_failure()

    async def _run() -> None:
        await agent._process_mint("MintA", snapshot, sources_with_error)

        assert published, "momentum agent should emit even under backpressure"
        stale_result = published[-1][1]
        assert stale_result.momentum_stale is True
        assert stale_result.momentum_partial is True
        error_hosts = stale_result.momentum_breakdown.get("error_hosts")
        assert error_hosts == ["pumpfun"]
        if stale_result.momentum_score is not None:
            assert 0.0 <= stale_result.momentum_score <= 1.0

        assert breaker.is_open is True
        cooldown = breaker.snapshot().get("cooldown_remaining", 0.0)
        assert cooldown == pytest.approx(30.0, rel=0.2)
        assert limiter.tokens <= limiter.capacity

        published.clear()
        breaker._opened_until = now_ts() - 1.0
        breaker.record_success()
        limiter.set_tokens(limiter.capacity)

        recovered_sources = dict(base_sources)
        recovered_sources["_errors"] = {}

        await agent._process_mint("MintA", snapshot, recovered_sources)

        assert published, "agent should emit after recovery"
        recovered_result = published[-1][1]
        assert recovered_result.momentum_stale is False
        assert recovered_result.momentum_partial is False
        assert recovered_result.momentum_score is not None
        assert math.isfinite(recovered_result.momentum_score)

    asyncio.run(_run())
