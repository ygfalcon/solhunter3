import asyncio
from solhunter_zero.market_stream import stream_market_data


def test_stream_market_data(monkeypatch):
    from solhunter_zero import http
    http._session = None
    async def fake_metrics(token):
        return {"volume": 100.0, "slippage": 0.05}

    async def fake_prices(tokens):
        return {tokens[0]: 2.0}

    monkeypatch.setattr("solhunter_zero.market_stream.fetch_token_metrics_async", fake_metrics)
    monkeypatch.setattr("solhunter_zero.market_stream.fetch_token_prices_async", fake_prices)

    async def run():
        gen = stream_market_data("tok", poll_interval=0.01)
        data = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return data

    data = asyncio.run(run())
    assert data == {"price": 2.0, "volume": 100.0, "slippage": 0.05}
