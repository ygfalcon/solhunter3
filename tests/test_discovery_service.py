from __future__ import annotations

import asyncio
from typing import List

import pytest

from solhunter_zero.pipeline.discovery_service import DiscoveryService


class _StubDiscoveryAgent:
    def __init__(self, responses: list[list[str]]) -> None:
        self._responses: list[list[str]] = [list(batch) for batch in responses]
        self.limit = 3

    async def discover_tokens(self, **_: object) -> List[str]:
        if not self._responses:
            return []
        return self._responses.pop(0)


def test_discovery_service_partial_cooldown_and_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _exercise() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        service = DiscoveryService(
            queue,
            cache_ttl=30.0,
            empty_cache_ttl=12.0,
            limit=3,
            emit_batch_size=10,
        )

        agent = _StubDiscoveryAgent([
            [
                "So11111111111111111111111111111111111111112",
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            ],
            [
                "So11111111111111111111111111111111111111112",
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "Vote111111111111111111111111111111111111111",
            ],
        ])
        service._agent = agent  # type: ignore[attr-defined]

        current_time = 1000.0

        def _fake_time() -> float:
            nonlocal current_time
            current_time += 1.0
            return current_time

        monkeypatch.setattr(
            "solhunter_zero.pipeline.discovery_service.time.time",
            _fake_time,
        )

        partial_tokens = await service._fetch()
        partial_cooldown = service._cooldown_until - service._last_fetch_ts

        assert partial_cooldown == pytest.approx(min(service.cache_ttl, service.empty_cache_ttl))
        assert service._last_fetch_partial is True

        await service._emit_tokens(partial_tokens, fresh=True, partial=True)
        partial_batch = queue.get_nowait()
        assert len(partial_batch) == len(partial_tokens)
        for candidate in partial_batch:
            assert candidate.metadata.get("partial") is True
            assert candidate.metadata.get("stale") is False

        current_time = service._cooldown_until + 10.0

        full_tokens = await service._fetch()
        full_cooldown = service._cooldown_until - service._last_fetch_ts

        assert full_cooldown == pytest.approx(service.cache_ttl)
        assert full_cooldown > partial_cooldown
        assert service._last_fetch_partial is False

        await service._emit_tokens(full_tokens, fresh=False, partial=False)
        full_batch = queue.get_nowait()
        assert len(full_batch) == len(full_tokens)
        for candidate in full_batch:
            assert candidate.metadata.get("partial") is False
            assert candidate.metadata.get("stale") is True

    asyncio.run(_exercise())
