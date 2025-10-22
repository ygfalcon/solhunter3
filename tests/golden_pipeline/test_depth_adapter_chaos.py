import asyncio
import time

import pytest

from solhunter_zero.golden_pipeline.depth_adapter import AnchorResult, GoldenDepthAdapter


def test_adapter_fallback_emits_within_budget() -> None:
    asyncio.run(_run_adapter_test())


async def _run_adapter_test() -> None:
    emitted: list[tuple[float, float]] = []

    async def _submit(snapshot) -> None:  # type: ignore[override]
        emitted.append((time.time(), snapshot.depth_bands_usd.get("1.0", 0.0) if snapshot.depth_bands_usd else 0.0))

    adapter = GoldenDepthAdapter(
        enabled=True,
        submit_depth=_submit,
        decimals_resolver=lambda _: 6,
        cache_ttl=0.2,
    )

    async def _fake_anchor(*args, **kwargs):  # type: ignore[override]
        return AnchorResult(
            price=1.02,
            confidence=0.0005,
            publish_time=time.time() - 0.1,
            source="pyth",
            degraded=True,
        )

    async def _fail_quotes(*args, **kwargs):  # type: ignore[override]
        await asyncio.sleep(0.05)
        raise asyncio.TimeoutError()

    async def _dummy_session() -> None:  # type: ignore[override]
        return None

    adapter._resolve_anchor = _fake_anchor  # type: ignore[assignment]
    adapter._collect_quotes = _fail_quotes  # type: ignore[assignment]
    adapter._get_session = _dummy_session  # type: ignore[assignment]

    adapter.record_activity("MintChaos")
    start = time.time()
    await adapter.start()
    await asyncio.sleep(0.4)
    await adapter.stop()

    assert emitted, "adapter failed to emit fallback snapshot"
    first_latency = emitted[0][0] - start
    assert first_latency < 0.8, f"fallback exceeded budget: {first_latency:.3f}s"
