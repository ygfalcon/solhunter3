import asyncio
import types

import pytest

from solhunter_zero.pipeline.discovery_service import DiscoveryService
from solhunter_zero.agents.discovery import DiscoveryConfigurationError


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


def test_discovery_service_surfaces_config_error(monkeypatch):
    async def runner() -> None:
        queue: asyncio.Queue[list] = asyncio.Queue()
        service = DiscoveryService(queue, emit_batch_size=1)

        async def fail_discover_tokens(*_, **__):
            raise DiscoveryConfigurationError("config invalid")

        service._agent = types.SimpleNamespace(discover_tokens=fail_discover_tokens)

        events: list[tuple[str, object]] = []

        def fake_publish(topic, payload):
            events.append((topic, payload))

        monkeypatch.setattr(
            "solhunter_zero.pipeline.discovery_service.publish", fake_publish
        )

        with pytest.raises(DiscoveryConfigurationError):
            await service._run()

        assert service._stopped.is_set()
        assert any(
            topic == "runtime.log"
            and getattr(payload, "level", None) == "CRITICAL"
            and "config_error" in getattr(payload, "detail", "")
            for topic, payload in events
        )

    asyncio.run(runner())
