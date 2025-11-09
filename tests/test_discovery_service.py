import asyncio
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
