import asyncio

from solhunter_zero import token_scanner


class _DummyResponse:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, *, content_type=None):
        return self._payload

    def raise_for_status(self):
        return None


class _DummySession:
    def __init__(self, payload):
        self._payload = payload
        self.last_request = None

    def get(self, url, params=None, timeout=None):
        self.last_request = (url, params, timeout)
        return _DummyResponse(self._payload)


def test_pump_trending_uses_token_address(monkeypatch):
    monkeypatch.setenv("PUMP_LEADERBOARD_URL", "https://example.com/pump")

    token_address = "H6yn7A6PRQT83wXWx3YpGHTKp21HBFA2wNrMESeiD7rq"
    payload = [{"tokenAddress": token_address, "name": "Token Address Only"}]

    session = _DummySession(payload)

    result = asyncio.run(token_scanner._pump_trending(session, limit=1))

    assert [item["address"] for item in result] == [token_address]
