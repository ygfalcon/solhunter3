import pytest
transformers = pytest.importorskip("transformers")
if not hasattr(transformers, "pipeline"):
    transformers.pipeline = lambda *a, **k: lambda x: []
import asyncio
import sys
import types

if "base58" not in sys.modules:
    def _fake_b58decode(value):
        if not isinstance(value, str):
            raise TypeError("value must be str")
        if not (32 <= len(value.strip()) <= 44):
            raise ValueError("invalid length")
        return b"\x00" * 32

    sys.modules["base58"] = types.SimpleNamespace(b58decode=_fake_b58decode)

import solhunter_zero.news as news
import solhunter_zero.http as http

class FakeResp:
    def __init__(self, text):
        self._text = text
    def raise_for_status(self):
        pass
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False
    async def text(self):
        return self._text

SAMPLE_XML = """
<rss><channel>
    <item><title>Good gains ahead</title></item>
    <item><title>Market crash expected</title></item>
</channel></rss>
"""


class DummyModel:
    def __call__(self, text):
        return [{"label": "POSITIVE", "score": 0.8}]


def test_fetch_headlines(monkeypatch):
    http._session = None
    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=10):
            return FakeResp(SAMPLE_XML)

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())
    headlines = news.fetch_headlines(["http://ok"], allowed=["http://ok"])
    assert headlines == ["Good gains ahead", "Market crash expected"]


def test_blocked_feed(monkeypatch):
    http._session = None
    called = {}
    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=10):
            called["url"] = url
            return FakeResp(SAMPLE_XML)

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())
    headlines = news.fetch_headlines(["http://bad"], allowed=["http://ok"])
    assert headlines == []
    assert "url" not in called


def test_compute_sentiment(monkeypatch):
    monkeypatch.setattr(news, "get_pipeline", lambda: DummyModel())
    text = "good gain up"
    score = news.compute_sentiment(text)
    assert score > 0


def test_fetch_sentiment(monkeypatch):
    http._session = None
    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, timeout=10):
            return FakeResp(SAMPLE_XML)

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())
    monkeypatch.setattr(news, "get_pipeline", lambda: DummyModel())
    score = news.fetch_sentiment(["http://ok"], allowed=["http://ok"])
    assert isinstance(score, float)
    assert -1.0 <= score <= 1.0


def test_fetch_token_mentions(monkeypatch):
    samples = [
        "So much alpha on https://solscan.io/token/So11111111111111111111111111111111111111112",
        "Whales talk about So11111111111111111111111111111111111111112 and EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "Another EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v sighting!",
    ]

    async def fake_fetch_headlines_async(*args, **kwargs):
        return list(samples)

    monkeypatch.setattr(news, "fetch_headlines_async", fake_fetch_headlines_async)

    result = asyncio.run(
        news.fetch_token_mentions_async(
            ["http://ok"],
            allowed=["http://ok"],
            limit=5,
            min_mentions=1,
        )
    )

    assert result
    top = {entry["token"]: entry["mentions"] for entry in result}
    assert top["So11111111111111111111111111111111111111112"] == 2
    assert top["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"] == 2
    assert all("samples" in entry for entry in result if entry["token"] in top)
