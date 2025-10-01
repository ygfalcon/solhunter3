import asyncio
from solhunter_zero.agents.flashloan_sandwich import FlashloanSandwichAgent


async def fake_stream(url, **_):
    yield {"address": "tok", "avg_swap_size": 2.0, "slippage": 0.3, "size": 2.0}


async def _run(agent):
    gen = agent.listen("ws://node")
    token = await asyncio.wait_for(anext(gen), timeout=0.1)
    await gen.aclose()
    return token


def test_flashloan_sandwich_bundle(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.fetch_slippage_onchain",
        lambda t, u: 0.3,
    )

    async def fake_fetch(token, side, amount, price, base_url):
        return f"MSG_{side}"

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich._fetch_swap_tx_message",
        fake_fetch,
    )

    async def fake_prepare(msg):
        return f"TX_{msg}"

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.prepare_signed_tx",
        fake_prepare,
    )

    sent = []

    async def fake_submit(self, txs):
        sent.append(txs)

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.MEVExecutor.submit_bundle",
        fake_submit,
    )

    calls = {}

    async def fake_borrow(amount, token, inst, *, payer):
        calls["borrow"] = (amount, token)
        return "sig"

    async def fake_repay(sig):
        calls["repay"] = sig
        return True

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.borrow_flash",
        fake_borrow,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.repay_flash",
        fake_repay,
    )

    agent = FlashloanSandwichAgent(size_threshold=1.0, slippage_threshold=0.2)
    token = asyncio.run(_run(agent))

    assert token == "tok"
    assert sent == [["TX_MSG_buy", "TX_MSG_sell"]]
    assert calls == {"borrow": (2.0, "tok"), "repay": "sig"}


def test_flashloan_sandwich_ratio(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.fetch_slippage_onchain",
        lambda t, u: 0.3,
    )

    async def fake_fetch(token, side, amount, price, base_url):
        return f"MSG_{side}"

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich._fetch_swap_tx_message",
        fake_fetch,
    )
    async def fake_prepare(msg):
        return f"TX_{msg}"

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.prepare_signed_tx",
        fake_prepare,
    )

    borrowed = []

    async def fake_borrow(amount, token, inst, *, payer):
        borrowed.append(amount)
        return "sig"

    async def fake_repay(sig):
        return True

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.borrow_flash",
        fake_borrow,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.repay_flash",
        fake_repay,
    )

    monkeypatch.setenv("FLASH_LOAN_RATIO", "0.1")
    monkeypatch.setenv("PORTFOLIO_VALUE", "10")

    agent = FlashloanSandwichAgent(size_threshold=1.0, slippage_threshold=0.2)
    asyncio.run(_run(agent))
    amt1 = borrowed[-1]

    borrowed.clear()
    monkeypatch.setenv("PORTFOLIO_VALUE", "20")
    asyncio.run(_run(agent))
    amt2 = borrowed[-1]

    assert amt1 < amt2


def test_flashloan_sandwich_jito(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.jito_stream.stream_pending_swaps",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.stream_ranked_mempool_tokens_with_depth",
        fake_stream,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.fetch_slippage_onchain",
        lambda t, u: 0.3,
    )

    async def fake_fetch(token, side, amount, price, base_url):
        return f"MSG_{side}"

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich._fetch_swap_tx_message",
        fake_fetch,
    )

    async def fake_prepare(msg):
        return f"TX_{msg}"

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.prepare_signed_tx",
        fake_prepare,
    )

    async def fake_borrow(*a, **k):
        return "sig"

    async def fake_repay(s):
        return True

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.borrow_flash",
        fake_borrow,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.repay_flash",
        fake_repay,
    )

    created = {}

    class FakeMEV:
        def __init__(self, token, *, priority_rpc=None, jito_rpc_url=None, jito_auth=None, **_):
            created["jito_url"] = jito_rpc_url
            created["jito_auth"] = jito_auth

        async def submit_bundle(self, txs):
            pass

    monkeypatch.setattr(
        "solhunter_zero.agents.flashloan_sandwich.MEVExecutor",
        FakeMEV,
    )

    agent = FlashloanSandwichAgent(
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
