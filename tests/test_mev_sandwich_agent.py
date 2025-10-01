import asyncio
from solhunter_zero.agents.mev_sandwich import MEVSandwichAgent


async def fake_stream(url, **_):
    yield {"address": "tok", "avg_swap_size": 2.0, "slippage": 0.3, "size": 2.0}


async def _run(agent):
    gen = agent.listen("ws://node")
    token = await asyncio.wait_for(anext(gen), timeout=0.1)
    await gen.aclose()
    return token


def test_mev_sandwich_bundle(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.fetch_slippage_onchain",
        lambda t, u: 0.3,
    )

    async def fake_fetch(token, side, amount, price, base_url):
        return f"MSG_{side}"

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich._fetch_swap_tx_message",
        fake_fetch,
    )

    async def fake_prepare(msg):
        return f"TX_{msg}"

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.prepare_signed_tx",
        fake_prepare,
    )

    sent = []

    async def fake_submit(self, txs):
        sent.append(txs)

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.MEVExecutor.submit_bundle",
        fake_submit,
    )

    agent = MEVSandwichAgent(size_threshold=1.0, slippage_threshold=0.2)
    token = asyncio.run(_run(agent))

    assert token == "tok"
    assert sent == [["TX_MSG_buy", "TX_MSG_sell"]]


def test_mev_sandwich_jito(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.jito_stream.stream_pending_swaps",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.fetch_slippage_onchain",
        lambda t, u: 0.3,
    )

    async def fake_fetch(token, side, amount, price, base_url):
        return f"MSG_{side}"

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich._fetch_swap_tx_message",
        fake_fetch,
    )

    async def fake_prepare(msg):
        return f"TX_{msg}"

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.prepare_signed_tx",
        fake_prepare,
    )

    created = {}

    class FakeMEV:
        def __init__(self, token, *, priority_rpc=None, jito_rpc_url=None, jito_auth=None, **_):
            created["jito_url"] = jito_rpc_url
            created["jito_auth"] = jito_auth

        async def submit_bundle(self, txs):
            pass

    monkeypatch.setattr(
        "solhunter_zero.agents.mev_sandwich.MEVExecutor",
        FakeMEV,
    )

    agent = MEVSandwichAgent(
        size_threshold=1.0,
        slippage_threshold=0.2,
        jito_rpc_url="http://jito",
        jito_auth="T",
        jito_ws_url="ws://ws",
        jito_ws_auth="A",
    )
    asyncio.run(_run(agent))

    assert created["jito_url"] == "http://jito"
    assert created["jito_auth"] == "T"
