import asyncio
import json

import pytest

from solhunter_zero import token_scanner


class FakeResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        return self._payload

    async def text(self):
        if isinstance(self._payload, str):
            return self._payload
        return json.dumps(self._payload)

    def raise_for_status(self):
        if self.status >= 400:
            raise RuntimeError(f"status={self.status}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        if not self.responses:
            raise AssertionError("no more responses queued")
        return self.responses.pop(0)


def _run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def reset_birdeye_state(monkeypatch):
    token_scanner._reset_birdeye_failures()
    yield
    token_scanner._reset_birdeye_failures()


def test_birdeye_trending_success(monkeypatch):
    payload = {
        "data": {
            "items": [
                {"address": "mint-one"},
                {"address": "mint-two"},
            ]
        }
    }
    session = FakeSession([FakeResponse(200, payload)])

    result = _run(token_scanner._birdeye_trending(session, "api", limit=2))

    assert result == ["mint-one", "mint-two"]
    assert session.calls == 1
    assert token_scanner._BIRDEYE_FAILURE_STATE["consecutive_failures"] == 0
    assert token_scanner.BIRDEYE_TRENDING_STATUS["ok"] is True


def test_birdeye_trending_cooldown(monkeypatch):
    current_time = {"value": 1000.0}

    def fake_monotonic():
        return current_time["value"]

    monkeypatch.setattr(token_scanner, "_monotonic", fake_monotonic)

    responses = [
        FakeResponse(400, {"message": "bad params"}),
        FakeResponse(
            200,
            {
                "data": {
                    "items": [
                        {"address": "mint-three"},
                        {"address": "mint-four"},
                    ]
                }
            },
        ),
    ]
    session = FakeSession(responses)

    first = _run(token_scanner._birdeye_trending(session, "api", limit=2))
    assert first == []
    assert session.calls == 1
    assert token_scanner._BIRDEYE_FAILURE_STATE["consecutive_failures"] == 1
    assert token_scanner.BIRDEYE_TRENDING_STATUS["ok"] is False
    assert "bad params" in token_scanner.BIRDEYE_TRENDING_STATUS["last_error"]["summary"]

    second = _run(token_scanner._birdeye_trending(session, "api", limit=2))
    assert second == []
    # Cooldown should prevent another HTTP call
    assert session.calls == 1

    # Advance beyond cooldown and retry, consuming the queued success response
    current_time["value"] += 120.0
    third = _run(token_scanner._birdeye_trending(session, "api", limit=2))
    assert third == ["mint-three", "mint-four"]
    assert session.calls == 2
    assert token_scanner._BIRDEYE_FAILURE_STATE["consecutive_failures"] == 0
    assert token_scanner.BIRDEYE_TRENDING_STATUS["ok"] is True
