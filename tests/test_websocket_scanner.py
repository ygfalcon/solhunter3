import asyncio

import solhunter_zero.websocket_scanner as ws_scanner
from solhunter_zero import scanner_common


class FakeWS:
    def __init__(self, messages):
        self.messages = list(messages)
        self.subscribed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def logs_subscribe(self, *args, **kwargs):
        self.subscribed = True

    async def recv(self):
        if self.messages:
            return [self.messages.pop(0)]
        raise asyncio.CancelledError


class FakeConnect:
    def __init__(self, url, messages):
        self.url = url
        self.ws = FakeWS(messages)

    async def __aenter__(self):
        return await self.ws.__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        await self.ws.__aexit__(exc_type, exc, tb)


def test_stream_new_tokens(monkeypatch):
    msgs = [
        {
            "result": {
                "value": {
                    "logs": [
                        "InitializeMint",
                        "name: coolbonk",
                        "mint: tok1",
                    ]
                }
            }
        },
        {
            "result": {
                "value": {
                    "logs": [
                        "InitializeMint",
                        "name: other",
                        "mint: tok2",
                    ]
                }
            }
        },
    ]

    def fake_connect(url):
        return FakeConnect(url, msgs)

    monkeypatch.setattr(ws_scanner, "connect", fake_connect)

    async def run():
        gen = ws_scanner.stream_new_tokens("ws://node", suffix="bonk")
        token = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return token

    token = asyncio.run(run())
    assert token == "tok1"


def test_stream_new_tokens_keyword(monkeypatch):
    msgs = [
        {
            "result": {
                "value": {
                    "logs": [
                        "InitializeMint",
                        "name: verycool",
                        "mint: tok3",
                    ]
                }
            }
        },
        {
            "result": {
                "value": {
                    "logs": [
                        "InitializeMint",
                        "name: other",
                        "mint: tok4",
                    ]
                }
            }
        },
    ]

    def fake_connect(url):
        return FakeConnect(url, msgs)

    monkeypatch.setattr(ws_scanner, "connect", fake_connect)

    async def run():

        gen = ws_scanner.stream_new_tokens("ws://node", keywords=["cool"])

        token = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return token

    token = asyncio.run(run())

    assert token == "tok3"


def test_offline_or_onchain_async_websocket(monkeypatch):
    async def fake_stream_new_tokens(url, *, suffix=None, keywords=None):
        yield "tokws"

    monkeypatch.setattr(
        ws_scanner,
        "stream_new_tokens",
        fake_stream_new_tokens,
    )

    scanner_common.BIRDEYE_API_KEY = None
    scanner_common.SOLANA_RPC_URL = "ws://node"

    tokens = asyncio.run(
        scanner_common.offline_or_onchain_async(
            False,
            method="websocket",
        )
    )
    assert tokens == ["tokws"]
