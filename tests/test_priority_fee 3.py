import asyncio
import types
import pytest

from solhunter_zero import gas


class FakeProvider:
    def __init__(self, fee):
        self.fee = fee

    async def make_request(self, method, params):
        return {"result": [{"prioritizationFee": self.fee}]}


class FakeClient:
    def __init__(self, url, fee):
        self.url = url
        self._provider = FakeProvider(fee)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


def test_get_priority_fee_estimate(monkeypatch):
    called = []

    def fake_client(url):
        called.append(url)
        return FakeClient(url, 5)

    monkeypatch.setattr(gas, "AsyncClient", fake_client)
    fee = asyncio.run(gas.get_priority_fee_estimate(["u1", "u2"]))
    assert called == ["u1"]
    assert fee == pytest.approx(5 / gas.LAMPORTS_PER_SOL)
