import asyncio
from collections import Counter

from solhunter_zero.golden_pipeline.discovery import DiscoveryStage
from solhunter_zero.golden_pipeline.types import DiscoveryCandidate

from tests.golden_pipeline.conftest import BASE58_MINTS


def test_discovery_sources_deduplicate(golden_harness):
    events = golden_harness.discovery_events
    sources = Counter(event["source"] for event in events)

    assert sources["das"] == 2  # two unique mints via DAS
    assert sources["das_timeout"] == 3

    accepted_mints = {
        event["mint"]
        for event in events
        if event.get("accepted")
    }
    assert accepted_mints == {
        BASE58_MINTS["alpha"],
        BASE58_MINTS["beta"],
    }

    for source in ("fallback", "mempool", "amm", "pumpfun", "replay"):
        rejected = [event for event in events if event["source"] == source]
        assert rejected and all(event.get("accepted") is False for event in rejected)

    stage = golden_harness.pipeline._discovery_stage  # type: ignore[attr-defined]
    assert stage.seen_recently(BASE58_MINTS["alpha"]) is True
    assert stage.seen_recently(BASE58_MINTS["beta"]) is True


def test_discovery_stage_rejects_default_program_prefix() -> None:
    emitted: list[str] = []

    async def run() -> bool:
        async def emit(candidate: DiscoveryCandidate) -> None:
            emitted.append(candidate.mint)

        stage = DiscoveryStage(emit)
        candidate = DiscoveryCandidate(
            mint="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            asof=0.0,
        )

        return await stage.submit(candidate)

    accepted = asyncio.run(run())

    assert accepted is False
    assert emitted == []


def test_discovery_stage_allowlist_overrides_default_prefixes() -> None:
    emitted: list[str] = []
    allowed_mint = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    rejected_mint = "JUP2jxvYMk9UUVdN2A5isXwpPyFmc1B99x23dpsdwG3"

    async def run() -> tuple[bool, bool]:
        async def emit(candidate: DiscoveryCandidate) -> None:
            emitted.append(candidate.mint)

        stage = DiscoveryStage(emit, allow_program_prefixes={"Tokenkeg"})
        allowed_candidate = DiscoveryCandidate(
            mint=allowed_mint,
            asof=0.0,
        )
        rejected_candidate = DiscoveryCandidate(
            mint=rejected_mint,
            asof=1.0,
        )

        allowed = await stage.submit(allowed_candidate)
        rejected = await stage.submit(rejected_candidate)
        return allowed, rejected

    allowed, rejected = asyncio.run(run())

    assert allowed is True
    assert rejected is False
    assert emitted == [allowed_mint]
