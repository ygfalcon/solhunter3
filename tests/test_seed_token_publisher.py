import asyncio
import json
import logging
import sys
import types
from typing import Dict

if "redis" not in sys.modules:
    redis_stub = types.ModuleType("redis")
    redis_asyncio_stub = types.ModuleType("redis.asyncio")
    redis_stub.asyncio = redis_asyncio_stub
    sys.modules["redis"] = redis_stub
    sys.modules["redis.asyncio"] = redis_asyncio_stub

import pytest

from solhunter_zero import prices, seed_token_publisher


def test_seed_publisher_emits_pyth_metadata(monkeypatch, caplog):
    sol = "So11111111111111111111111111111111111111112"
    usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9"
    bonk = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
    wormhole = "J3NKxxXZcnNiMjKw9hYb2K4LUxgwB6t1FtPtQVsv3KFr"
    pump_a = "Pump1111111111111111111111111111111111111111"
    pump_b = "Pump2222222222222222222222222222222222222222"
    tokens = (sol, usdc, bonk, wormhole, pump_a, pump_b)

    monkeypatch.setenv("SEED_TOKENS", ",".join(tokens))

    identifiers: Dict[str, prices.PythIdentifier] = {
        sol: prices.PythIdentifier(
            feed_id="0x" + "11" * 32,
            account="1" * 44,
            raw="1" * 44,
        ),
        usdc: prices.PythIdentifier(
            feed_id="0x" + "22" * 32,
            account="2" * 44,
            raw="2" * 44,
        ),
        bonk: prices.PythIdentifier(
            feed_id="0x" + "33" * 32,
            account="3" * 44,
            raw="3" * 44,
        ),
        wormhole: prices.PythIdentifier(
            feed_id="0x" + "44" * 32,
            account="4" * 44,
            raw="4" * 44,
        ),
    }

    async def fake_fetch_price_quotes_async(requested):
        return {
            token: prices.PriceQuote(
                price_usd=float(index + 1),
                source="pyth",
                asof=0,
                quality="authoritative",
            )
            for index, token in enumerate(requested)
        }

    class DummyRedis:
        def __init__(self) -> None:
            self.payloads = []

        async def publish(self, channel, payload):
            self.payloads.append((channel, payload))
            return 1

    dummy = DummyRedis()

    monkeypatch.setattr(
        seed_token_publisher.aioredis,
        "from_url",
        lambda *args, **kwargs: dummy,
        raising=False,
    )
    monkeypatch.setattr(
        seed_token_publisher.prices,
        "fetch_price_quotes_async",
        fake_fetch_price_quotes_async,
    )
    monkeypatch.setattr(
        seed_token_publisher.prices, "_parse_pyth_mapping", lambda: (identifiers, [])
    )

    async def stop_sleep(_interval):
        raise StopAsyncIteration

    monkeypatch.setattr(seed_token_publisher.asyncio, "sleep", stop_sleep)

    caplog.set_level(logging.INFO, logger="solhunter_zero.seed_token_publisher")

    async def runner() -> None:
        with pytest.raises(StopAsyncIteration):
            await seed_token_publisher.run_seed_token_publisher()

    asyncio.run(runner())

    token_events = {}
    for _channel, payload in dummy.payloads:
        event = json.loads(payload)
        if event.get("topic") == "token_discovered":
            token_events[event["mint"]] = event

    assert set(token_events) == set(tokens)

    for mint in (sol, usdc, bonk, wormhole):
        discovery = token_events[mint]["discovery"]
        pyth_info = discovery.get("pyth")
        assert pyth_info["status"] == "available"
        assert pyth_info["feed_id"] == identifiers[mint].feed_id
        assert pyth_info["account"] == identifiers[mint].account
        assert pyth_info["kind"] == identifiers[mint].kind
        assert pyth_info["canonical_mint"] == mint

    for mint in (pump_a, pump_b):
        discovery = token_events[mint]["discovery"]
        pyth_info = discovery.get("pyth")
        assert pyth_info["status"] == "missing"
        assert pyth_info["canonical_mint"] == mint

    account_logs = [
        record.message
        for record in caplog.records
        if "uses Pyth account" in record.message
    ]
    missing_logs = [
        record.message
        for record in caplog.records
        if "missing Pyth identifier" in record.message
    ]
    assert len(account_logs) == 4
    assert len(missing_logs) == 2


def test_build_seeded_token_metadata(monkeypatch):
    sol = "So11111111111111111111111111111111111111112"
    alias = f"  {sol}  "
    pump = "Pump3333333333333333333333333333333333333333"
    tokens = (sol, alias, pump)

    mapping = {
        sol: prices.PythIdentifier(
            feed_id="0x" + "aa" * 32,
            account="A" * 44,
            raw="A" * 44,
        )
    }

    metadata = seed_token_publisher.build_seeded_token_metadata(tokens, mapping)

    sol_meta = metadata[sol]
    assert sol_meta["status"] == "available"
    assert sol_meta["feed_id"] == mapping[sol].feed_id
    assert sol_meta["account"] == mapping[sol].account
    assert sol_meta["canonical_mint"] == sol

    alias_meta = metadata[alias]
    assert alias_meta["status"] == "available"
    assert alias_meta["feed_id"] == mapping[sol].feed_id
    assert alias_meta["account"] == mapping[sol].account
    assert alias_meta["canonical_mint"] == sol

    pump_meta = metadata[pump]
    assert pump_meta["status"] == "missing"
    assert pump_meta["canonical_mint"] == pump
