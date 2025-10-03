import base64
import asyncio
import pytest

pytest.importorskip("solders")

from solders.keypair import Keypair
from solders.hash import Hash
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.instruction import Instruction
from solders.signature import Signature
from solders.transaction import VersionedTransaction

from solhunter_zero.exchange import place_order, place_order_async
import solhunter_zero.exchange as exchange


async def _no_fee_async(*_a, **_k):
    return 0.0


def _configure_swap(monkeypatch: pytest.MonkeyPatch, mapping: dict[str, str]) -> None:
    monkeypatch.setattr(exchange, "SWAP_PRIORITIES", list(mapping.keys()), raising=False)
    monkeypatch.setattr(exchange, "SWAP_URLS", dict(mapping), raising=False)
    monkeypatch.setattr(exchange, "SWAP_PATHS", {k: "/swap" for k in mapping}, raising=False)
    monkeypatch.setattr(exchange, "DEFAULT_SWAP_PATH", "/swap", raising=False)
    monkeypatch.setattr(exchange, "VENUE_URLS", dict(mapping), raising=False)


def _dummy_tx(kp: Keypair) -> str:
    msg = MessageV0.try_compile(
        kp.pubkey(), [Instruction(Pubkey.default(), b"", [])], [], Hash.new_unique()
    )
    tx = VersionedTransaction.populate(msg, [Signature.default()])
    return base64.b64encode(bytes(tx)).decode()


class DummyResponse:
    def __init__(self, url: str, data: dict[str, object]):
        self.url = url
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def json(self):
        return self._data

    def raise_for_status(self):
        return None


def test_place_order_sends(monkeypatch: pytest.MonkeyPatch):
    kp = Keypair()
    calls: list[str] = []

    _configure_swap(monkeypatch, {"helius": "https://helius.test"})
    monkeypatch.setattr(exchange, "DEX_BASE_URL", "https://helius.test", raising=False)
    monkeypatch.setattr(exchange, "DEX_TESTNET_URL", "https://helius.test", raising=False)
    monkeypatch.setattr(exchange, "_order_loop", None, raising=False)

    class FakeSession:
        def post(self, url, json, timeout=10, headers=None):
            calls.append(url)
            return DummyResponse(url, {"swapTransaction": _dummy_tx(kp)})

    async def fake_get_session():
        return FakeSession()

    rpc_info: dict[str, object] = {}

    class FakeClient:
        def __init__(self, url):
            rpc_info["rpc"] = url

        def send_raw_transaction(self, data, opts=None):
            rpc_info["data_len"] = len(data)

            class Resp:
                value = "sig"

            return Resp()

    monkeypatch.setattr(exchange, "get_session", fake_get_session)
    monkeypatch.setattr(exchange, "Client", FakeClient)

    result = place_order("tok", "buy", 1.0, 0.0, keypair=kp, testnet=True)

    assert result["signature"] == "sig"
    assert rpc_info["data_len"] > 0
    assert calls[0] == "https://helius.test/swap"


def test_place_order_dry_run(monkeypatch: pytest.MonkeyPatch):
    kp = Keypair()

    _configure_swap(monkeypatch, {"helius": "https://helius.test"})
    monkeypatch.setattr(exchange, "_order_loop", None, raising=False)

    called: dict[str, bool] = {}

    async def fake_get_session():
        called["session"] = True
        return None

    monkeypatch.setattr(exchange, "get_session", fake_get_session)

    result = place_order("tok", "buy", 1.0, 0.0, keypair=kp, dry_run=True)

    assert result["dry_run"] is True
    assert "session" not in called


def test_place_order_async(monkeypatch: pytest.MonkeyPatch):
    kp = Keypair()
    calls: list[str] = []
    rpc_info: dict[str, object] = {}

    _configure_swap(monkeypatch, {"helius": "https://helius.test"})
    monkeypatch.setattr(exchange, "DEX_BASE_URL", "https://helius.test", raising=False)
    monkeypatch.setattr(exchange, "DEX_TESTNET_URL", "https://helius.test", raising=False)

    class FakeSession:
        def post(self, url, json, timeout=10, headers=None):
            calls.append(url)
            return DummyResponse(url, {"swapTransaction": _dummy_tx(kp)})

    async def fake_get_session():
        return FakeSession()

    class FakeClient:
        def __init__(self, url):
            rpc_info["rpc"] = url

        async def send_raw_transaction(self, data, opts=None):
            rpc_info["len"] = len(data)

            class Resp:
                value = "sig"

            return Resp()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(exchange, "get_session", fake_get_session)
    monkeypatch.setattr(exchange, "AsyncClient", FakeClient)
    monkeypatch.setattr(exchange, "get_current_fee_async", _no_fee_async)
    monkeypatch.setattr(exchange, "USE_RUST_EXEC", False, raising=False)

    result = asyncio.run(place_order_async("tok", "buy", 1.0, 0.0, keypair=kp, testnet=True))

    assert result["signature"] == "sig"
    assert rpc_info["len"] > 0
    assert calls[0] == "https://helius.test/swap"


def test_place_order_async_deducts_gas(monkeypatch: pytest.MonkeyPatch):
    kp = Keypair()
    payloads: list[dict[str, object]] = []

    _configure_swap(monkeypatch, {"helius": "https://helius.test"})
    monkeypatch.setattr(exchange, "DEX_BASE_URL", "https://helius.test", raising=False)
    monkeypatch.setattr(exchange, "DEX_TESTNET_URL", "https://helius.test", raising=False)

    class FakeSession:
        def post(self, url, json, timeout=10, headers=None):
            payloads.append(json)
            return DummyResponse(url, {"swapTransaction": _dummy_tx(kp)})

    async def fake_get_session():
        return FakeSession()

    class FakeClient:
        def __init__(self, url):
            pass

        async def send_raw_transaction(self, data, opts=None):
            class Resp:
                value = "sig"

            return Resp()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    async def fake_fee(*_a, **_k):
        return 1.0

    monkeypatch.setattr(exchange, "get_session", fake_get_session)
    monkeypatch.setattr(exchange, "AsyncClient", FakeClient)
    monkeypatch.setattr(exchange, "get_current_fee_async", fake_fee)
    monkeypatch.setattr(exchange, "USE_RUST_EXEC", False, raising=False)

    asyncio.run(place_order_async("tok", "buy", 2.0, 0.0, keypair=kp))

    assert payloads[0]["amount"] == pytest.approx(1.0)


def test_place_order_async_cascade(monkeypatch: pytest.MonkeyPatch):
    kp = Keypair()
    order: list[str] = []
    urls = {
        "helius": "https://helius.test",
        "birdeye": "https://birdeye.test",
        "jupiter": "https://jupiter.test",
    }

    _configure_swap(monkeypatch, urls)
    monkeypatch.setattr(exchange, "DEX_BASE_URL", urls["helius"], raising=False)
    monkeypatch.setattr(exchange, "DEX_TESTNET_URL", urls["helius"], raising=False)

    class FakeClientError(Exception):
        pass

    monkeypatch.setattr(exchange.aiohttp, "ClientError", FakeClientError, raising=False)
    monkeypatch.setattr(exchange, "USE_RUST_EXEC", False, raising=False)
    monkeypatch.setattr(exchange, "get_current_fee_async", _no_fee_async)

    class FakeSession:
        def post(self, url, json, timeout=10, headers=None):
            order.append(url)
            if "birdeye" in url:
                raise FakeClientError("birdeye down")
            if "helius" in url:
                return DummyResponse(url, {})
            return DummyResponse(url, {"swapTransaction": _dummy_tx(kp)})

    async def fake_get_session():
        return FakeSession()

    class FakeClient:
        def __init__(self, url):
            pass

        async def send_raw_transaction(self, data, opts=None):
            class Resp:
                value = "sig"

            return Resp()

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(exchange, "get_session", fake_get_session)
    monkeypatch.setattr(exchange, "AsyncClient", FakeClient)

    result = asyncio.run(
        place_order_async("tok", "buy", 1.0, 0.0, keypair=kp, max_retries=1)
    )

    assert result["signature"] == "sig"
    assert order == [
        "https://helius.test/swap",
        "https://birdeye.test/swap",
        "https://jupiter.test/swap",
    ]


def test_place_order_reuses_loop(monkeypatch: pytest.MonkeyPatch):
    kp = Keypair()
    loops: list[asyncio.AbstractEventLoop] = []

    _configure_swap(monkeypatch, {"helius": "https://helius.test"})
    monkeypatch.setattr(exchange, "DEX_BASE_URL", "https://helius.test", raising=False)
    monkeypatch.setattr(exchange, "DEX_TESTNET_URL", "https://helius.test", raising=False)

    orig_new_loop = asyncio.new_event_loop

    def fake_new_loop():
        loop = orig_new_loop()
        loops.append(loop)
        return loop

    monkeypatch.setattr(asyncio, "new_event_loop", fake_new_loop)
    monkeypatch.setattr(exchange, "_order_loop", None, raising=False)

    class FakeSession:
        def post(self, url, json, timeout=10, headers=None):
            return DummyResponse(url, {"swapTransaction": _dummy_tx(kp)})

    async def fake_get_session():
        return FakeSession()

    class FakeClient:
        def __init__(self, url):
            pass

        def send_raw_transaction(self, data, opts=None):
            class Resp:
                value = "sig"

            return Resp()

    monkeypatch.setattr(exchange, "get_session", fake_get_session)
    monkeypatch.setattr(exchange, "Client", FakeClient)

    place_order("tok", "buy", 1.0, 0.0, keypair=kp)
    place_order("tok", "buy", 1.0, 0.0, keypair=kp)

    assert len(loops) == 1


def test_place_order_ipc_reuses_connection(monkeypatch: pytest.MonkeyPatch):
    responses = [
        b'{"signature": "sig1"}',
        b'{"signature": "sig2"}',
    ]

    class FakeReader:
        def __init__(self, data):
            self.data = list(data)

        async def read(self):
            return self.data.pop(0)

    class FakeWriter:
        def __init__(self):
            self.data = b""
            self.closed = False

        def write(self, data):
            self.data += data

        async def drain(self):
            return None

        def close(self):
            self.closed = True

        def is_closing(self):
            return self.closed

        async def wait_closed(self):
            return None

    reader = FakeReader(responses)
    writer = FakeWriter()
    calls: list[str] = []

    async def fake_conn(path):
        calls.append(path)
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)
    monkeypatch.setattr(exchange, "_IPC_CONNECTIONS", {}, raising=False)
    monkeypatch.setattr(exchange, "_IPC_LOCKS", {}, raising=False)

    async def run():
        r1 = await exchange._place_order_ipc("TX1", socket_path="sock")
        r2 = await exchange._place_order_ipc("TX2", socket_path="sock")
        return r1, r2

    res1, res2 = asyncio.run(run())

    assert res1["signature"] == "sig1"
    assert res2["signature"] == "sig2"
    assert len(calls) == 1
    assert not writer.closed
