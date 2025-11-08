import pytest

from solhunter_zero import discovery


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_collect_mempool_candidates_deduplicates(monkeypatch):
    payloads = [
        {"address": "dup", "score": 1.0, "tag": "low"},
        {"address": "dup", "combined_score": 5.0, "tag": "high"},
        {"address": "unique", "score": 3.0},
    ]

    async def fake_stream(_target_url, threshold):
        assert threshold == 0.0
        for payload in payloads:
            yield payload

    monkeypatch.setattr(
        discovery, "stream_ranked_mempool_tokens_with_depth", fake_stream
    )

    results = await discovery._collect_mempool_candidates(
        "https://rpc", limit=2, threshold=0.0
    )

    assert len(results) == 2
    assert [item["address"] for item in results] == ["dup", "unique"]
    assert results[0]["tag"] == "high"


@pytest.mark.anyio("asyncio")
async def test_collect_mempool_candidates_refreshes_timeout(monkeypatch):
    async def fake_stream(_target_url, threshold):
        yield {"address": "tok", "score": 1.0}

    captured: list[float] = []

    async def fake_wait_for(awaitable, timeout, **kwargs):
        captured.append(timeout)
        return await awaitable

    monkeypatch.setattr(
        discovery, "stream_ranked_mempool_tokens_with_depth", fake_stream
    )
    monkeypatch.setattr(discovery.asyncio, "wait_for", fake_wait_for)
    monkeypatch.setenv("DISCOVERY_MEMPOOL_TIMEOUT_RETRIES", "1")

    monkeypatch.setenv("DISCOVERY_MEMPOOL_TIMEOUT", "0.2")
    await discovery._collect_mempool_candidates("rpc", limit=1)
    assert captured[-1] == pytest.approx(0.2)

    monkeypatch.setenv("DISCOVERY_MEMPOOL_TIMEOUT", "0.6")
    await discovery._collect_mempool_candidates("rpc", limit=1)
    assert captured[-1] == pytest.approx(0.6)
