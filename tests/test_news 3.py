import pytest
transformers = pytest.importorskip("transformers")
if not hasattr(transformers, "pipeline"):
    transformers.pipeline = lambda *a, **k: lambda x: []
import solhunter_zero.news as news
import solhunter_zero.http as http

class FakeResp:
    def __init__(self, text):
        self.text = text
    def raise_for_status(self):
        pass

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
