import asyncio
import os
import types

from solhunter_zero.pipeline.discovery_service import DiscoveryService


def test_emit_tokens_skips_reordered_batches():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        service = DiscoveryService(queue, emit_batch_size=10)
        service._agent = types.SimpleNamespace(
            last_method="unit-test", last_details={}
        )

        tokens = [
            "So11111111111111111111111111111111111111112",
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        ]

        await service._emit_tokens(tokens, fresh=True)
        assert queue.qsize() == 1

        first_batch = queue.get_nowait()
        assert {candidate.token for candidate in first_batch} == set(tokens)

        await service._emit_tokens(list(reversed(tokens)), fresh=True)

        assert queue.qsize() == 0
        assert service._last_emitted == list(reversed(tokens))
        assert service._last_emitted_set == frozenset(tokens)
        assert service._last_emitted_size == len(tokens)

    asyncio.run(runner())


def test_emit_tokens_detects_metadata_changes():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        token = "So11111111111111111111111111111111111111112"
        agent = types.SimpleNamespace(
            last_method="unit-test",
            last_details={token: {"price": 1.0}},
        )
        service = DiscoveryService(queue, emit_batch_size=10)
        service._agent = agent

        await service._emit_tokens([token], fresh=True)
        first_batch = queue.get_nowait()
        assert first_batch[0].metadata["price"] == 1.0

        assert queue.qsize() == 0

        agent.last_details[token]["price"] = 2.0

        await service._emit_tokens([token], fresh=True)

        assert queue.qsize() == 1
        second_batch = queue.get_nowait()
        assert second_batch[0].metadata["price"] == 2.0

    asyncio.run(runner())


def test_emit_tokens_purges_stale_details():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        token_a = "So11111111111111111111111111111111111111112"
        token_b = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
        agent = types.SimpleNamespace(
            last_method="unit-test",
            last_details={
                token_a: {"price": 1.0},
                token_b: {"price": 2.0},
            },
        )
        service = DiscoveryService(queue, emit_batch_size=10)
        service._agent = agent

        await service._emit_tokens([token_a, token_b], fresh=True)
        first_batch = queue.get_nowait()
        assert {candidate.token for candidate in first_batch} == {token_a, token_b}

        await service._emit_tokens([token_b], fresh=True)

        assert queue.qsize() == 1
        second_batch = queue.get_nowait()
        assert {candidate.token for candidate in second_batch} == {token_b}
        assert token_a not in agent.last_details

    asyncio.run(runner())


def test_emit_tokens_ignores_metadata_changes_when_not_fresh():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        token = "So11111111111111111111111111111111111111112"
        agent = types.SimpleNamespace(
            last_method="unit-test",
            last_details={token: {"price": 1.0}},
        )
        service = DiscoveryService(queue, emit_batch_size=10)
        service._agent = agent

        await service._emit_tokens([token], fresh=True)
        queue.get_nowait()

        agent.last_details[token]["price"] = 3.0

        await service._emit_tokens([token], fresh=False)

        assert queue.qsize() == 0

    asyncio.run(runner())


def test_empty_fetch_backoff_is_capped():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        service = DiscoveryService(
            queue,
            empty_cache_ttl=2.5,
            backoff_factor=2.0,
            max_backoff=5.0,
        )

        base_ts = 1000.0
        cooldowns: list[float] = []
        for idx in range(4):
            ts = base_ts + idx
            service._apply_fetch_stats([], ts)
            cooldown = service._cooldown_until - ts
            cooldowns.append(cooldown)

        assert cooldowns[0] == 2.5
        assert cooldowns[1] == 5.0
        assert cooldowns[2] == 5.0
        assert cooldowns[3] == 5.0
        assert service._current_backoff == 5.0

    asyncio.run(runner())


def test_env_override_for_max_backoff():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        previous = os.environ.get("DISCOVERY_MAX_BACKOFF")
        os.environ["DISCOVERY_MAX_BACKOFF"] = "4.5"
        try:
            service = DiscoveryService(
                queue,
                empty_cache_ttl=2.0,
                backoff_factor=2.0,
                max_backoff=15.0,
            )
        finally:
            if previous is None:
                os.environ.pop("DISCOVERY_MAX_BACKOFF", None)
            else:
                os.environ["DISCOVERY_MAX_BACKOFF"] = previous

        assert service.max_backoff == 4.5

        service._apply_fetch_stats([], 0.0)
        assert service._current_backoff == 2.0
        service._apply_fetch_stats([], 1.0)
        assert service._current_backoff == 4.0
        service._apply_fetch_stats([], 2.0)
        assert service._current_backoff == 4.5

    asyncio.run(runner())


def test_failure_backoff_reuses_cooldown():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        service = DiscoveryService(
            queue,
            empty_cache_ttl=2.0,
            backoff_factor=2.0,
            max_backoff=5.0,
        )

        base_ts = 50.0
        service._apply_failure_backoff(base_ts)
        assert service._consecutive_failures == 1
        assert service._current_backoff == 2.0
        assert service._cooldown_until == base_ts + 2.0

        service._apply_failure_backoff(base_ts + 1.0)
        assert service._consecutive_failures == 2
        assert service._current_backoff == 4.0
        assert service._cooldown_until == base_ts + 1.0 + 4.0

        service._apply_failure_backoff(base_ts + 2.0)
        assert service._consecutive_failures == 3
        assert service._current_backoff == 5.0
        assert service._cooldown_until == base_ts + 2.0 + 5.0

        assert not service._last_fetch_fresh

    asyncio.run(runner())


def test_failure_counter_resets_on_success():
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        service = DiscoveryService(queue)

        service._apply_failure_backoff(0.0)
        assert service._consecutive_failures == 1

        service._apply_fetch_stats(["So11111111111111111111111111111111111111112"], 1.0)

        assert service._consecutive_failures == 0
        assert service._current_backoff == 0.0

    asyncio.run(runner())
