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
