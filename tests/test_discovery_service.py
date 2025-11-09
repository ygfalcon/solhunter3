from __future__ import annotations

import asyncio
from types import MethodType

import pytest

from solhunter_zero.pipeline.discovery_service import DiscoveryService
from solhunter_zero.pipeline.types import TokenCandidate


TOKEN_A = "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN"
TOKEN_B = "So11111111111111111111111111111111111111112"


def test_startup_clones_emit_metadata() -> None:
    async def run() -> None:
        queue: asyncio.Queue[list[TokenCandidate]] = asyncio.Queue()
        service = DiscoveryService(queue, startup_clones=2, emit_batch_size=2)

        clone_payloads: dict[int, tuple[list[str], dict[str, dict[str, object]]]] = {
            0: (
                [TOKEN_A, TOKEN_B],
                {
                    TOKEN_A: {"liquidity": "123.45", "sources": ["mempool", "birdeye"]},
                    TOKEN_B: {"liquidity": 678.9, "sources": ["birdeye"]},
                },
            ),
            1: (
                [TOKEN_B],
                {
                    TOKEN_B: {"liquidity": "250", "sources": ["birdeye", "mempool"]},
                },
            ),
        }

        async def fake_clone(self: DiscoveryService, idx: int) -> tuple[list[str], dict[str, dict[str, object]]]:
            tokens, details = clone_payloads[idx]
            token_list = list(tokens)
            detail_map = {token: dict(detail) for token, detail in details.items()}
            return token_list, detail_map

        service._clone_fetch = MethodType(fake_clone, service)

        await service._prime_startup_clones()

        batch = await asyncio.wait_for(queue.get(), timeout=0.1)
        assert batch, "expected discovery service to emit a batch from startup clones"
        assert isinstance(batch[0], TokenCandidate)
        assert len(batch) == 2

        metadata_a = batch[0].metadata
        assert metadata_a.get("liquidity") == pytest.approx(123.45)
        assert metadata_a.get("sources") == ["birdeye", "mempool"]

        metadata_b = batch[1].metadata
        assert metadata_b.get("liquidity") == pytest.approx(250.0)
        assert metadata_b.get("sources") == ["birdeye", "mempool"]

        assert service._agent.last_details[TOKEN_B]["liquidity"] == "250"

    asyncio.run(run())
