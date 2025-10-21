import pytest

from solhunter_zero import token_discovery as td


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        _ = content_type
        return self._payload

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, responses):
        self._responses = responses
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, url, *, params=None, headers=None, timeout=None):
        _ = (url, headers, timeout)
        addr = params["tokenAddress"] if params else None
        self.calls.append(addr)
        response = self._responses.get(addr)
        if isinstance(response, Exception):
            raise response
        return FakeResponse(response or {})


@pytest.mark.asyncio
async def test_enrich_with_solscan_populates_fields(monkeypatch):
    address = "So11111111111111111111111111111111111111112"
    candidates = {
        address: {"address": address, "name": address},
    }

    payload = {
        "data": {
            "symbol": "SOL",
            "name": "Solana",
            "decimals": 9,
        }
    }

    session = FakeSession({address: payload})

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return session

    monkeypatch.setattr(td, "get_session", fake_get_session)
    monkeypatch.setattr(td, "_SOLSCAN_ENRICH_LIMIT", 4)
    monkeypatch.setattr(td, "_ENABLE_SOLSCAN", True)

    await td._enrich_with_solscan(candidates)

    entry = candidates[address]
    assert entry["symbol"] == "SOL"
    assert entry["name"] == "Solana"
    assert entry["decimals"] == 9
    assert "solscan" in entry["sources"]
    assert session.calls == [address]


@pytest.mark.asyncio
async def test_enrich_with_solscan_task_failure_does_not_cancel(monkeypatch):
    good_addr = "So11111111111111111111111111111111111111112"
    bad_addr = "Bad11111111111111111111111111111111111111113"

    candidates = {
        good_addr: {"address": good_addr, "name": good_addr},
        bad_addr: {"address": bad_addr, "name": bad_addr},
    }

    payload = {"data": {"symbol": "GOOD", "decimals": 6}}

    session = FakeSession({good_addr: payload, bad_addr: RuntimeError("boom")})

    async def fake_get_session(*, timeout=None):
        _ = timeout
        return session

    monkeypatch.setattr(td, "get_session", fake_get_session)
    monkeypatch.setattr(td, "_SOLSCAN_ENRICH_LIMIT", 2)
    monkeypatch.setattr(td, "_ENABLE_SOLSCAN", True)

    await td._enrich_with_solscan(candidates)

    good_entry = candidates[good_addr]
    assert good_entry["symbol"] == "GOOD"
    assert good_entry["decimals"] == 6
    assert "solscan" in good_entry["sources"]

    bad_entry = candidates[bad_addr]
    assert "symbol" not in bad_entry
    assert "solscan" not in bad_entry.get("sources", set())
    assert set(session.calls) == {good_addr, bad_addr}
