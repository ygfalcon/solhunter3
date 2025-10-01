import asyncio
import json
import pytest

from solhunter_zero.wallet import save_keypair
from solhunter_zero import wallet
from solhunter_zero.portfolio import Portfolio


@pytest.mark.asyncio
async def test_load_keypair_async(tmp_path):
    path = tmp_path / "kp.json"
    data = [1] * 64
    path.write_text(json.dumps(data))
    kp = await wallet.load_keypair_async(str(path))
    assert kp.to_bytes()  # ensure object usable


@pytest.mark.asyncio
async def test_load_keypair_async_invalid_length(tmp_path):
    path = tmp_path / "kp.json"
    path.write_text(json.dumps([1] * 10))
    with pytest.raises(ValueError, match="64"):
        await wallet.load_keypair_async(str(path))


@pytest.mark.asyncio
async def test_load_keypair_async_invalid_value(tmp_path):
    path = tmp_path / "kp.json"
    data = [1] * 64
    data[0] = 256
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="0 and 255"):
        await wallet.load_keypair_async(str(path))


@pytest.mark.asyncio
async def test_portfolio_async_operations_responsive(tmp_path):
    pf = Portfolio(path=str(tmp_path / "pf.json"))

    async def spam():
        for _ in range(50):
            await pf.update_async("TOK", 1.0, 1.0)

    ticks = 0

    async def ticker():
        nonlocal ticks
        for _ in range(5):
            await asyncio.sleep(0.01)
            ticks += 1

    await asyncio.gather(spam(), ticker())
    assert ticks == 5
