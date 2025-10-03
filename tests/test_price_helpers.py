import asyncio

import pytest

from solhunter_zero import arbitrage
from solhunter_zero import onchain_metrics
from solhunter_zero import prices


class _DummySession:
    def __init__(self, response=None):
        self.response = response
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers))
        if self.response is None:
            raise AssertionError("unexpected HTTP request")
        return self.response


class _DummyResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self, content_type=None):
        return self._payload

    async def text(self):  # pragma: no cover - rarely used fallback
        return ""


def test_fetch_price_prefers_helius(monkeypatch):
    arbitrage.PRICE_CACHE.clear()
    prices.PRICE_CACHE.clear()

    async def fake_get_session():
        return _DummySession()

    async def fake_helius_price_overview(session, token):
        assert token == "tok"
        return (2.5, 0.0, 0.0, 0.0, 0, 0, None)

    async def fail_birdeye(session, token):  # pragma: no cover - guard
        raise AssertionError("Birdeye fallback should not execute")

    monkeypatch.setattr(arbitrage, "get_session", fake_get_session)
    monkeypatch.setattr(arbitrage, "_helius_price_overview", fake_helius_price_overview)
    monkeypatch.setattr(arbitrage, "_birdeye_price_overview", fail_birdeye)

    async def _run():
        price = await arbitrage.fetch_jupiter_price_async("tok")
        assert price == pytest.approx(2.5)
        assert arbitrage.PRICE_CACHE.get(("jupiter", "tok")) == pytest.approx(2.5)

    asyncio.run(_run())


def test_fetch_price_uses_fallback(monkeypatch):
    arbitrage.PRICE_CACHE.clear()
    prices.PRICE_CACHE.clear()

    async def fake_get_session():
        return _DummySession()

    async def zero_helper(*_args, **_kwargs):
        return (0.0, 0.0, 0.0, 0.0, 0, 0, None)

    async def fake_dexscreener(session, token):
        assert token == "tok"
        return 1.8

    monkeypatch.setattr(arbitrage, "get_session", fake_get_session)
    monkeypatch.setattr(arbitrage, "_helius_price_overview", zero_helper)
    monkeypatch.setattr(arbitrage, "_birdeye_price_overview", zero_helper)
    monkeypatch.setattr(arbitrage, "_fetch_price_from_dexscreener", fake_dexscreener)
    monkeypatch.setattr(arbitrage, "ENABLE_BIRDEYE_FALLBACK", True, raising=False)
    monkeypatch.setattr(arbitrage, "ENABLE_DEXSCREENER_FALLBACK", True, raising=False)

    async def _run():
        price = await arbitrage.fetch_jupiter_price_async("tok")
        assert price == pytest.approx(1.8)

    asyncio.run(_run())


def test_fetch_price_graceful_failure(monkeypatch):
    arbitrage.PRICE_CACHE.clear()
    prices.PRICE_CACHE.clear()

    async def fake_get_session():
        return _DummySession()

    async def zero_helper(*_args, **_kwargs):
        return (0.0, 0.0, 0.0, 0.0, 0, 0, None)

    async def zero_dex(*_args, **_kwargs):
        return 0.0

    monkeypatch.setattr(arbitrage, "get_session", fake_get_session)
    monkeypatch.setattr(arbitrage, "_helius_price_overview", zero_helper)
    monkeypatch.setattr(arbitrage, "_birdeye_price_overview", zero_helper)
    monkeypatch.setattr(arbitrage, "_fetch_price_from_dexscreener", zero_dex)
    monkeypatch.setattr(arbitrage, "ENABLE_BIRDEYE_FALLBACK", True, raising=False)
    monkeypatch.setattr(arbitrage, "ENABLE_DEXSCREENER_FALLBACK", True, raising=False)

    async def _run():
        price = await arbitrage.fetch_jupiter_price_async("tok")
        assert price == 0.0

    asyncio.run(_run())


def test_price_impact_helius_provider(monkeypatch):
    response = _DummyResponse({"data": {"priceImpactPct": 0.123}})
    session = _DummySession(response=response)

    monkeypatch.setattr(onchain_metrics, "_helius_api_key", lambda: "api-key" )
    monkeypatch.setattr(
        onchain_metrics,
        "HELIUS_PRICE_IMPACT_URL",
        "https://fake.helius.test/impact",
        raising=False,
    )

    async def _run():
        impact = await onchain_metrics._jupiter_quote_price_impact_pct(
            session,
            "out",
            in_mint="in",
            in_amount_sol=0.25,
        )
        assert impact == pytest.approx(0.123)
        assert session.calls, "expected request to be issued"

    asyncio.run(_run())


def test_price_impact_disabled_without_key(monkeypatch):
    called = False

    class _FailSession:
        async def get(self, *_args, **_kwargs):  # pragma: no cover - should not run
            nonlocal called
            called = True
            return _DummyResponse({}, status=500)

    monkeypatch.setattr(onchain_metrics, "_helius_api_key", lambda: "")

    async def _run():
        impact = await onchain_metrics._jupiter_quote_price_impact_pct(
            _FailSession(),
            "out",
        )
        assert impact == 0.0
        assert called is False

    asyncio.run(_run())
