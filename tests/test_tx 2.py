import asyncio
import base64
import json
import pytest

try:
    from solders.keypair import Keypair
    from solders.pubkey import Pubkey
    from solders.hash import Hash
    from solders.instruction import Instruction, AccountMeta
    from solders.message import MessageV0
    from solders.transaction import VersionedTransaction
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pytest.skip("solders is required", allow_module_level=True)

from solhunter_zero import depth_client


class FakeReader:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self):
        return self._data


class FakeWriter:
    def __init__(self):
        self.data = b""

    def write(self, data: bytes):
        self.data += data

    async def drain(self):
        pass

    def close(self):
        pass

    async def wait_closed(self):
        pass


def test_submit_real_tx(monkeypatch):
    kp = Keypair()
    ix = Instruction(
        Pubkey.from_string("11111111111111111111111111111111"),
        b"", [AccountMeta(kp.pubkey(), True, True)]
    )
    msg = MessageV0.try_compile(kp.pubkey(), [ix], [], Hash.new_unique())
    sig = kp.sign_message(bytes(msg))
    tx = VersionedTransaction.populate(msg, [sig])
    tx_b64 = base64.b64encode(bytes(tx)).decode()

    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        reader = FakeReader(json.dumps({"signature": "sig"}).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.submit_raw_tx(tx_b64, socket_path="sock")

    sig_str = asyncio.run(run())

    assert sig_str == "sig"
    payload = json.loads(captured["writer"].data.decode())
    assert payload["cmd"] == "raw_tx"
    assert payload["tx"] == tx_b64
